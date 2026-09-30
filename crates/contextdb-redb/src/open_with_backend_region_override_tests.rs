//! Fork-local, crate-internal coverage for `Builder::open_with_backend`'s
//! region-size-override parity with `Builder::open`: a caller-requested
//! region size never applies to an existing, already-initialized store.
//!
//! `TransactionalMemory::new` only ever consults `requested_region_size`
//! while initializing a fresh (magic-number-absent) file; for a file whose
//! magic number already matches, that branch never runs and the requested
//! value has no path by which it could take effect. This module proves the
//! contract holds observably for `open_with_backend`: a store created under
//! one region size stays fully readable and writable when reopened through
//! `open_with_backend` with a deliberately different requested region size.
//!
//! This lives inside the crate (registered as a private `#[cfg(test)]`
//! module from `src/db.rs`) specifically because `Builder::set_region_size`
//! is `#[cfg(any(test, fuzzing))]` and unreachable from the crate's external
//! integration test binaries.

use crate::backends::FileBackend;
use crate::{Database, ReadableDatabase, TableDefinition};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("region_override_parity");

fn open_file_backend(path: &std::path::Path) -> FileBackend {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open existing file for a backend view");
    FileBackend::new(file).expect("construct a raw file backend")
}

#[test]
fn open_with_backend_ignores_a_different_requested_region_size_on_an_existing_store() {
    let tmpfile = crate::create_tempfile();
    let (file, path) = tmpfile.into_parts();
    drop(file);

    // Create the store under a small region size, then write enough rows to
    // make the region layout structurally meaningful (more than one page
    // worth of small regions), so a real region-boundary mismatch would
    // surface as corruption or a refused open rather than staying silent.
    let small_region_size: u64 = 32 * 4096;
    {
        let mut builder = Database::builder();
        builder.set_region_size(small_region_size);
        let db = builder
            .create(&path)
            .expect("create the fixture store under a small region size");
        let write = db.begin_write().expect("begin fixture write");
        {
            let mut table = write.open_table(TABLE).expect("open fixture table");
            let value = vec![7_u8; 512];
            for id in 0..512 {
                table
                    .insert(id, value.as_slice())
                    .expect("insert fixture row");
            }
        }
        write.commit().expect("commit fixture write");
    }

    // Reopen through open_with_backend with a deliberately different
    // (much larger) requested region size.
    let large_region_size: u64 = 16 * 1024 * 1024;
    let mut reopen_builder = Database::builder();
    reopen_builder.set_region_size(large_region_size);
    let reopened = reopen_builder
        .open_with_backend(open_file_backend(&path))
        .expect("open_with_backend opens the existing store despite the mismatched requested region size");

    let read = reopened.begin_read().expect("begin verification read");
    let table = read.open_table(TABLE).expect("open table after reopen");
    for id in 0..512 {
        assert_eq!(
            table.get(id).unwrap().unwrap().value(),
            vec![7_u8; 512],
            "row {id} survives a reopen whose requested region size does not match the store's own"
        );
    }
    drop(read);

    // The store stays genuinely writable afterward, under the SAME
    // requested (ignored) region size.
    let write = reopened.begin_write().expect("begin follow-up write");
    write
        .open_table(TABLE)
        .expect("open table for follow-up write")
        .insert(512, vec![9_u8; 512].as_slice())
        .expect("insert follow-up row");
    write.commit().expect("commit follow-up write");
    drop(reopened);

    // A further raw reopen (default region size, no override at all) still
    // reads every row back: the on-disk layout was never touched by either
    // requested override.
    let verify = Database::open(&path).expect("plain reopen for durability check");
    let read = verify.begin_read().expect("begin durability read");
    let table = read
        .open_table(TABLE)
        .expect("open table for durability check");
    assert_eq!(table.get(0).unwrap().unwrap().value(), vec![7_u8; 512]);
    assert_eq!(table.get(512).unwrap().unwrap().value(), vec![9_u8; 512]);
}
