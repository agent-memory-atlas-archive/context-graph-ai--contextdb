//! A read session is charged for the image it holds, and for nothing else.
//!
//! The writer's own retention -- its in-memory change log with the two side
//! indexes, and the transaction manager's commit index -- is charged through
//! `MemoryAccountant::charge_retained`, an unconditional door that exists so an
//! import that outgrows a declared `MEMORY_LIMIT` meets the engine's typed
//! refusal instead of the machine's out-of-memory kill. That door belongs to
//! the writer alone: a committed read image never appends a change-log entry,
//! so nothing it holds there grows with an import, and every byte of the image
//! it does hold was already admitted against this very accountant when it was
//! decoded (`read_image_memory::decode`). Charging it a second time at the open
//! silently shrinks the ceiling the reader declared.
//!
//! So: whatever the writer's accounting charges, a read session's accountant
//! reports zero bytes taken at that door.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;

const ROWS: usize = 600;
const SESSION_BYTES: usize = 24 * 1024 * 1024;

fn seed(path: &std::path::Path) {
    let db = Database::open(path).expect("open the store for writing");
    db.execute(
        "CREATE TABLE retention_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
        &HashMap::new(),
    )
    .expect("create the fixture table");
    let tx = db.begin().expect("begin the seeding transaction");
    for row in 0..ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO retention_probe VALUES ($id, $payload)",
            &HashMap::from([
                ("id".into(), Value::Int64(row as i64)),
                (
                    "payload".into(),
                    Value::Text(format!("{row:08x}").repeat(8)),
                ),
            ]),
        )
        .expect("insert a fixture row");
    }
    db.commit(tx).expect("commit the fixture rows");
    db.close().expect("close the store");
}

#[test]
fn a_read_only_session_pays_nothing_at_the_writers_retention_door() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = directory.path().join("writer-retention.db");
    seed(&path);

    let session = ReadSession::open_with_options(
        &path,
        ReadSessionOptions {
            memory_limit: Some(SESSION_BYTES),
            ..Default::default()
        },
    )
    .expect("open the idle store under a declared session ceiling");
    assert_eq!(
        session.route(),
        ReadRoute::File,
        "this contract is about the file route's own accountant"
    );

    let charged_at_the_writers_door = session
        .file_retained_charge_bytes_for_test()
        .expect("the file route reports what it took at the retention door");
    let usage = session
        .file_memory_for_test()
        .expect("the file route reports its session accounting")
        .0;
    assert_eq!(
        charged_at_the_writers_door, 0,
        "a read session took {charged_at_the_writers_door} B of its \
         {:?} B ceiling at the writer's retention door; {ROWS} rows of \
         change log and one commit index entry are the owner of those bytes, \
         and they were already admitted when the image was decoded \
         (usage: {usage:?})",
        usage.limit,
    );
}
