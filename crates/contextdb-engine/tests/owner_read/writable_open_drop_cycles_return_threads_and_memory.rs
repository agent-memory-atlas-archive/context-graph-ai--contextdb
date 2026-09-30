#![cfg(all(target_os = "linux", feature = "nightly-tests"))]

//! A writable open must release the owner-read service on both close paths.
//!
//! The socket disappearing is not enough: the accept worker can retain the
//! service after its pathname has gone.  This Linux-only proof uses the
//! process's actual thread names and resident set, then waits for this
//! fixture's actual accept-thread and final-service events before measuring.

use contextdb_engine::local_transport::MonotonicDeadlineClock;
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, OwnerReadTestHooks, ReadRoute, ReadSession,
};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::Duration;

const CYCLES: usize = 40;
const MAX_RSS_GROWTH_KIB: u64 = 64 * 1024;
const EXIT_WAIT: Duration = Duration::from_secs(10);

fn secure_runtime_root(directory: &Path, arm: &str) -> PathBuf {
    let root = directory.join(format!("{arm}-owner-runtime"));
    std::fs::create_dir(&root).expect("create the task-scoped owner runtime directory");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped owner runtime directory");
    root
}

fn owner_options(runtime_dir: PathBuf, test_hooks: OwnerReadTestHooks) -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        owner_reads: OwnerReadConfig {
            runtime_dir: Some(runtime_dir),
            test_hooks: Some(test_hooks),
            ..OwnerReadConfig::default()
        },
        ..DatabaseOpenOptions::default()
    }
}

fn owner_thread_count() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("read this process's Linux thread directory")
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            std::fs::read_to_string(entry.path().join("comm"))
                .ok()
                .is_some_and(|comm| comm.trim().starts_with("contextdb-owner"))
        })
        .count()
}

fn resident_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read this process status");
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .expect("Linux reports VmRSS in /proc/self/status")
}

fn lifecycle_hooks() -> (OwnerReadTestHooks, Receiver<()>, Receiver<()>, Receiver<()>) {
    let (accept_thread_exited, accept_thread_exit) = mpsc::channel();
    let (accept_thread_joined, accept_thread_join) = mpsc::channel();
    let (service_dropped, service_drop) = mpsc::channel();
    (
        OwnerReadTestHooks {
            clock: Arc::new(MonotonicDeadlineClock::new()),
            drain_started: mpsc::channel().0,
            active_request_registered: None,
            accept_thread_exited: Some(accept_thread_exited),
            accept_thread_joined: Some(accept_thread_joined),
            service_dropped: Some(service_dropped),
            quiescent_stop: None,
        },
        accept_thread_exit,
        accept_thread_join,
        service_drop,
    )
}

fn run_cycles(arm: &str, close_before_drop: bool, retain_authenticated_idle_reader: bool) {
    let directory = tempfile::tempdir().expect("task-scoped owner-cycle directory");
    let runtime_root = secure_runtime_root(directory.path(), arm);
    let path = directory.path().join(format!("{arm}.db"));
    let before_rss = resident_kib();
    assert_eq!(
        Database::live_owner_read_service_count_for_test(),
        0,
        "{arm} begins with no service retained from another cycle"
    );

    for cycle in 1..=CYCLES {
        let thread_baseline = owner_thread_count();
        let (hooks, accept_thread_exit, accept_thread_join, service_drop) = lifecycle_hooks();
        let database =
            Database::open_with_options(&path, owner_options(runtime_root.clone(), hooks))
                .unwrap_or_else(|error| panic!("open {arm} cycle {cycle}: {error:?}"));
        let idle_reader = retain_authenticated_idle_reader.then(|| {
            let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
                ReadSession::open(&path)
            })
            .unwrap_or_else(|error| {
                panic!("open authenticated idle reader for {arm} cycle {cycle}: {error:?}")
            });
            assert_eq!(
                session.route(),
                ReadRoute::Owner,
                "{arm} cycle {cycle} retains an authenticated owner-route reader"
            );
            session
        });
        assert!(
            matches!(accept_thread_exit.try_recv(), Err(TryRecvError::Empty)),
            "{arm} cycle {cycle} does not report an accept-thread exit while its writable handle is still live"
        );
        assert!(
            matches!(accept_thread_join.try_recv(), Err(TryRecvError::Empty)),
            "{arm} cycle {cycle} does not report a service-owned accept-thread join while its writable handle is still live"
        );
        assert!(
            matches!(service_drop.try_recv(), Err(TryRecvError::Empty)),
            "{arm} cycle {cycle} does not report a service drop while its writable handle is still live"
        );
        if close_before_drop {
            database
                .close()
                .unwrap_or_else(|error| panic!("close {arm} cycle {cycle}: {error:?}"));
        }
        drop(database);

        accept_thread_exit
            .recv_timeout(EXIT_WAIT)
            .unwrap_or_else(|error| {
                panic!(
                    "{arm} cycle {cycle} did not observe its accept thread completion: {error:?}"
                )
            });
        if close_before_drop {
            accept_thread_join
                .recv_timeout(EXIT_WAIT)
                .unwrap_or_else(|error| {
                    panic!(
                        "{arm} cycle {cycle} did not observe the service-owned accept-thread join: {error:?}"
                    )
            });
            assert!(
                !matches!(accept_thread_join.try_recv(), Ok(())),
                "{arm} cycle {cycle} reports its service-owned accept-thread join exactly once"
            );
        } else {
            assert!(
                matches!(
                    accept_thread_join.try_recv(),
                    Err(TryRecvError::Disconnected)
                ),
                "{arm} cycle {cycle} drop releases the service without waiting to join its accept thread"
            );
        }
        service_drop
            .recv_timeout(EXIT_WAIT)
            .unwrap_or_else(|error| {
                panic!("{arm} cycle {cycle} did not observe its owner service drop: {error:?}")
            });
        assert_eq!(
            owner_thread_count(),
            thread_baseline,
            "{arm} cycle {cycle} releases its actual contextdb-owner threads after service-owned completion and join"
        );
        assert_eq!(
            Database::live_owner_read_service_count_for_test(),
            0,
            "{arm} cycle {cycle} returns the live-service census to zero after its final service drop"
        );
        drop(idle_reader);
    }

    let growth = resident_kib().saturating_sub(before_rss);
    assert!(
        growth < MAX_RSS_GROWTH_KIB,
        "{arm} grows VmRSS by {growth} KiB after {CYCLES} cycles, below the 64 MiB limit"
    );
}

#[test]
fn writable_open_drop_cycles_return_threads_and_memory() {
    run_cycles("drop", false, false);
    run_cycles("close", true, false);
}

#[test]
fn writable_drop_reaps_an_authenticated_idle_owner_reader() {
    run_cycles("connected-idle-drop", false, true);
}

#[test]
fn writable_close_reaps_an_authenticated_idle_owner_reader() {
    run_cycles("connected-idle-close", true, true);
}
