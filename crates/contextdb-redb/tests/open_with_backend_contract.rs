//! FORK-LOCAL contract for the additive `Builder::open_with_backend`
//! counterpart to `Builder::create_with_backend`: an existing-file door with
//! the same `allow_initialize = false` and no-region-override semantics as
//! `Builder::open`, over a caller-supplied backend rather than a path.
//!
//! This is deliberately NOT one of the five upstream integration targets
//! (`backward_compatibility`, `basic_tests`, `integration_tests`,
//! `multimap_tests`, `multithreading_tests` -- see `MAINTENANCE.md`), which
//! stay copied unchanged. It is a new, additive fork-local target that
//! exercises only the new API surface:
//!
//! ```ignore
//! impl Builder {
//!     pub fn open_with_backend(&self, backend: impl StorageBackend) -> Result<Database, DatabaseError>;
//! }
//! ```
//!
//! matching `Builder::open`'s existing-file semantics exactly (`allow_initialize
//! = false`, region-size override ignored, same page size/cache size/repair
//! callback threaded through) -- see `Builder::open` and
//! `Builder::create_with_backend` in `src/db.rs`.
//!
//! Coverage note: `Builder::set_region_size` is `#[cfg(any(test, fuzzing))]`
//! in the fork, active only for the crate's OWN unit tests, not for this
//! external integration binary. Region-override-ignored parity for
//! `open_with_backend` is covered separately by a fork-local internal
//! `#[cfg(test)]` module that CAN see `set_region_size`, registered from
//! `src/db.rs`.

use redb::backends::FileBackend;
use redb::{ReadableDatabase, TableDefinition};
use std::io::{Read as _, Write as _};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

const TABLE: TableDefinition<u64, &str> = TableDefinition::new("open_with_backend_contract");
const CHILD_ROLE_ENV: &str = "CONTEXTDB_OPEN_WITH_BACKEND_CHILD_ROLE";
const CHILD_PATH_ENV: &str = "CONTEXTDB_OPEN_WITH_BACKEND_STORE_PATH";
const CHILD_COMMITTED_MARKER: &str = "COMMIT_THEN_EXIT_CHILD_COMMITTED";
const CHILD_BOUNDED_DEADLINE: Duration = Duration::from_secs(15);

/// Harness filter for a child process of THIS test binary: `--exact` needs
/// the module-qualified test name (here, just the bare function name, since
/// every test in this file lives in the binary's root module).
fn child_test_filter(fn_name: &str) -> String {
    match module_path!().split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_string(),
    }
}

/// Run a child process to completion within a bounded deadline, capturing
/// its combined stdout+stderr text. The wait is bounded by a channel receive
/// with a deadline (never an unbounded `status()`/`output()`/`read_line`),
/// not a sleep-poll loop: a dedicated thread drains both pipes concurrently
/// with the child running (avoiding the classic deadlock where a child
/// blocks writing to a full pipe nobody is reading yet) and signals this
/// thread the instant the child closes them, which happens exactly at exit.
/// A child still alive when the deadline is reached is killed -- the calling
/// thread still owns `Child` throughout, only its piped stdout/stderr are
/// handed to the reader thread -- and then reaped.
fn run_child_bounded(
    mut command: Command,
    deadline: Duration,
) -> (std::process::ExitStatus, String) {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("spawn bounded child process: {error}"));
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let (output_tx, output_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut text = String::new();
        let _ = stdout.read_to_string(&mut text);
        let mut err = String::new();
        let _ = stderr.read_to_string(&mut err);
        text.push_str(&err);
        let _ = output_tx.send(text);
    });
    let text = match output_rx.recv_timeout(deadline) {
        Ok(text) => text,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let _ = child.kill();
            // The kill closes the child's ends of the pipes, so the reader
            // thread's `read_to_string` calls return (EOF) and it sends
            // shortly; this is now an unbounded receive only in the sense
            // that a kill is assumed to actually terminate the process.
            output_rx
                .recv()
                .unwrap_or_else(|_| "bounded child reader thread died".to_string())
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("bounded child reader thread died before sending output")
        }
    };
    let status = child
        .wait()
        .unwrap_or_else(|error| panic!("reap bounded child: {error}"));
    (status, text)
}

/// Commit one durable row, then exit the process via `std::process::exit`
/// -- never returning from `main`, never unwinding, never running `Drop for
/// Database`. This is the only reliable way to leave a store in the "not
/// shut down cleanly" state redb's repair path exists for: forgetting the
/// `Database` value in-process would skip its Drop-time bookkeeping too,
/// but would ALSO leave this same process holding the exclusive file lock
/// forever. A genuinely separate process that exits releases the lock the
/// moment it dies, exactly like a real crashed writer.
#[test]
fn commit_then_exit_without_closing_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("commit-then-exit") {
        return;
    }
    let path = std::path::PathBuf::from(
        std::env::var_os(CHILD_PATH_ENV).expect("child receives a store path"),
    );
    let db = redb::Builder::new()
        .create(&path)
        .expect("child creates the fixture store");
    let write = db.begin_write().expect("child begins fixture write");
    write
        .open_table(TABLE)
        .expect("child opens fixture table")
        .insert(1, "committed-then-unclean-shutdown")
        .expect("child inserts fixture row");
    write.commit().expect("child commits fixture write");
    println!("{CHILD_COMMITTED_MARKER}");
    std::process::exit(0);
}

fn open_file_backend(path: &std::path::Path) -> FileBackend {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open existing file for a backend view");
    FileBackend::new(file).expect("construct a raw file backend")
}

#[test]
fn open_with_backend_refuses_a_zero_length_file_without_writing_to_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.redb");
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .expect("create a zero-length file");
    let before = std::fs::read(&path).expect("read empty file before the refused open");
    assert!(before.is_empty());

    let result = redb::Builder::new().open_with_backend(open_file_backend(&path));
    assert!(
        result.is_err(),
        "an empty file must be refused by the existing-file door, never silently initialized \
         the way `create_with_backend` would"
    );

    let after = std::fs::read(&path).expect("read empty file after the refused open");
    assert_eq!(
        before, after,
        "a refused existing-file open must leave a zero-length file byte-for-byte unchanged"
    );
}

#[test]
fn open_with_backend_refuses_a_truncated_non_redb_file_without_writing_to_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("truncated.redb");
    {
        let mut file = std::fs::File::create(&path).expect("create truncated fixture file");
        file.write_all(b"not a redb header, far too short")
            .expect("write truncated fixture bytes");
        file.sync_all().expect("sync truncated fixture bytes");
    }
    let before = std::fs::read(&path).expect("read truncated file before the refused open");
    assert!(!before.is_empty());

    let result = redb::Builder::new().open_with_backend(open_file_backend(&path));
    assert!(
        result.is_err(),
        "a short, non-redb file must be refused by the existing-file door, never treated as a \
         fresh store to initialize"
    );

    let after = std::fs::read(&path).expect("read truncated file after the refused open");
    assert_eq!(
        before, after,
        "a refused existing-file open must leave a truncated/corrupt file byte-for-byte unchanged"
    );
}

#[test]
fn open_with_backend_opens_an_existing_valid_store_and_serves_its_committed_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("valid.redb");
    {
        let db = redb::Builder::new()
            .create(&path)
            .expect("create the fixture store");
        let write = db.begin_write().expect("begin fixture write");
        write
            .open_table(TABLE)
            .expect("open fixture table")
            .insert(1, "committed-before-open-with-backend")
            .expect("insert fixture row");
        write.commit().expect("commit fixture write");
    }

    let reopened = redb::Builder::new()
        .open_with_backend(open_file_backend(&path))
        .expect("open_with_backend opens an existing valid store");
    let read = reopened.begin_read().expect("begin verification read");
    assert_eq!(
        read.open_table(TABLE)
            .expect("open table via open_with_backend")
            .get(1)
            .unwrap()
            .unwrap()
            .value(),
        "committed-before-open-with-backend"
    );
    drop(read);

    // Genuinely usable, not a read-only view: a further write through the
    // same handle commits and is durable after a raw reopen.
    let write = reopened
        .begin_write()
        .expect("begin write via open_with_backend");
    write
        .open_table(TABLE)
        .expect("open table for the follow-up write")
        .insert(2, "committed-after-open-with-backend")
        .expect("insert follow-up row");
    write.commit().expect("commit follow-up write");
    drop(reopened);

    let verify = redb::Builder::new()
        .open(&path)
        .expect("raw reopen for durability check");
    let read = verify.begin_read().expect("begin durability read");
    let table = read
        .open_table(TABLE)
        .expect("open table for durability check");
    assert_eq!(
        table.get(1).unwrap().unwrap().value(),
        "committed-before-open-with-backend"
    );
    assert_eq!(
        table.get(2).unwrap().unwrap().value(),
        "committed-after-open-with-backend"
    );
}

#[test]
fn open_with_backend_runs_the_configured_repair_callback_after_an_unclean_shutdown() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("needs_repair.redb");

    // Build the "not shut down cleanly" fixture in a genuinely separate,
    // bounded process (see `commit_then_exit_without_closing_child`): it
    // commits one durable row, then exits without running `Drop for
    // Database`, so the next open must take redb's real repair path.
    let mut command =
        Command::new(std::env::current_exe().expect("current integration-test binary"));
    command
        .arg("--exact")
        .arg(child_test_filter("commit_then_exit_without_closing_child"))
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "commit-then-exit")
        .env(CHILD_PATH_ENV, &path);
    let (status, text) = run_child_bounded(command, CHILD_BOUNDED_DEADLINE);
    assert!(
        status.success(),
        "the fixture child must commit and exit cleanly at the process level (only skipping \
         redb's own Drop-time bookkeeping, not the commit itself); got {status:?}\noutput:\n{text}"
    );
    assert!(
        text.contains(CHILD_COMMITTED_MARKER),
        "the fixture child must actually commit before this open is exercised\noutput:\n{text}"
    );

    let repaired = Arc::new(AtomicBool::new(false));
    let repaired_flag = repaired.clone();
    let mut builder = redb::Builder::new();
    builder.set_repair_callback(move |_session| {
        repaired_flag.store(true, Ordering::SeqCst);
    });

    let reopened = builder
        .open_with_backend(open_file_backend(&path))
        .expect("open_with_backend repairs and opens the unclean store");
    assert!(
        repaired.load(Ordering::SeqCst),
        "the caller's configured repair callback must actually run through open_with_backend, \
         exactly as it does through Builder::open"
    );
    let read = reopened.begin_read().expect("begin post-repair read");
    assert_eq!(
        read.open_table(TABLE)
            .expect("open table after repair")
            .get(1)
            .unwrap()
            .unwrap()
            .value(),
        "committed-then-unclean-shutdown",
        "repair recovers the last durable commit rather than discarding it"
    );
}
