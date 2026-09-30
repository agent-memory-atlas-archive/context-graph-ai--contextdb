//! A commit's cost does not quietly double because something upstream left a
//! savepoint open.
//!
//! Redb starts every write transaction tracking the pages it allocates, and
//! turns that tracking off the moment the transaction becomes dirty — but only
//! while no savepoint exists (`contextdb-redb/src/transactions.rs:622-630`).
//! So a store that holds a savepoint open across writes keeps tracking on, and
//! every commit then writes its full allocated-page list into
//! `DATA_ALLOCATED_TABLE` on top of the freed list it already writes
//! (`:1618`). At the measured ~436 freed pages per one-event append that is a
//! second per-commit page list of the same order, and nothing in the append
//! path would report it.
//!
//! This pins both halves: an ordinary write lists nothing, and a write taken
//! while a savepoint is alive lists its allocations. The engine creates no
//! savepoint on any path today — `ephemeral_savepoint` and
//! `persistent_savepoint` have no caller in `contextdb-engine/src` — so the
//! first half is the contract an append relies on and the second half is the
//! cliff it must not walk off.

use redb::TableDefinition;

const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("savepoint_allocation_probe");

/// Write `count` rows in one transaction and report the two page lists that
/// commit had to store.
fn commit_rows(
    database: &redb::Database,
    from: u64,
    count: u64,
) -> redb::commit_work::CommitPagesForTest {
    redb::commit_work::reset_commit_pages_for_test();
    let write = database.begin_write().expect("begin a write transaction");
    {
        let mut rows = write.open_table(ROWS).expect("open the probe table");
        for row in from..from + count {
            rows.insert(row, vec![0x5au8; 512].as_slice())
                .expect("insert a probe row");
        }
    }
    write.commit().expect("commit the probe rows");
    redb::commit_work::take_commit_pages_for_test()
}

#[test]
fn a_held_savepoint_makes_every_commit_list_its_allocated_pages() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let database = redb::Database::create(root.path().join("savepoint-allocation.redb"))
        .expect("create store");

    // Seed, so neither measured commit is the one that creates the table.
    commit_rows(&database, 0, 64);

    let ordinary = commit_rows(&database, 1_000, 64);
    assert_eq!(ordinary.commits, 1, "one commit was observed");
    assert!(
        ordinary.freed_pages > 0,
        "an ordinary commit still lists the pages its own copy-on-write freed"
    );
    assert_eq!(
        ordinary.allocated_pages, 0,
        "with no savepoint alive, redb stops tracking allocations as soon as the transaction is \
         dirty, so the commit writes no allocated-page list"
    );

    // A savepoint cannot be taken in a dirty transaction, so it is taken in a
    // transaction of its own and then held across the next write.
    let held = {
        let write = database
            .begin_write()
            .expect("begin the savepoint transaction");
        let savepoint = write
            .ephemeral_savepoint()
            .expect("take a savepoint in a clean transaction");
        write.commit().expect("commit the savepoint transaction");
        savepoint
    };

    let under_savepoint = commit_rows(&database, 2_000, 64);
    assert_eq!(under_savepoint.commits, 1, "one commit was observed");
    assert!(
        under_savepoint.allocated_pages > 0,
        "a commit taken while a savepoint is alive writes its full allocated-page list; \
         ordinary commit listed {} allocated pages, this one listed {}",
        ordinary.allocated_pages,
        under_savepoint.allocated_pages,
    );

    // The savepoint is what does it: drop it and the next commit is ordinary
    // again. Nothing about the workload changed.
    drop(held);
    let after = commit_rows(&database, 3_000, 64);
    assert_eq!(
        after.allocated_pages, 0,
        "releasing the savepoint returns commits to listing no allocated pages"
    );
}

/// No engine path opens a savepoint, so the cliff above is not reachable from a
/// write this engine performs. A new caller has to say so.
#[test]
fn the_engine_opens_no_savepoint() {
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut callers = Vec::new();
    let mut pending = vec![source];
    while let Some(entry) = pending.pop() {
        for item in std::fs::read_dir(&entry).expect("read the engine source tree") {
            let path = item.expect("read a source entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let text = std::fs::read_to_string(&path).expect("read a source file");
                if text.contains("ephemeral_savepoint(") || text.contains("persistent_savepoint(") {
                    callers.push(path);
                }
            }
        }
    }
    assert!(
        callers.is_empty(),
        "an engine path now opens a savepoint, so every commit while it is held also writes its \
         allocated-page list: {callers:?}"
    );
}
