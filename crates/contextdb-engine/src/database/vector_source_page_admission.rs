//! Source extents are separate owners from generation decode/build workspaces.
//! In particular, a borrowed graph still holds its entire redb source page.

use crate::memory_accounting::MemoryAccountant;
use contextdb_core::{Error, Result};
use std::cell::RefCell;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

struct SourceRefusal {
    marker: String,
    error: parking_lot::Mutex<Option<Error>>,
}

thread_local! {
    static SOURCE_REFUSAL: RefCell<Option<Arc<SourceRefusal>>> = const { RefCell::new(None) };
}

struct SourceRefusalScope(Option<Arc<SourceRefusal>>);

impl Drop for SourceRefusalScope {
    fn drop(&mut self) {
        SOURCE_REFUSAL.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

/// Recover only the refusal belonging to this operation and this thread.
/// The private marker survives redb's I/O error adaptation, so a caught or
/// unrelated later storage error cannot consume stale source-page detail.
pub(crate) fn take_source_page_refusal(message: &str) -> Option<Error> {
    SOURCE_REFUSAL.with(|slot| {
        let slot = slot.borrow();
        let refusal = slot.as_ref()?;
        if message.strip_prefix("I/O error: ") != Some(refusal.marker.as_str()) {
            return None;
        }
        refusal.error.lock().take()
    })
}

struct SourcePageCharge {
    accountant: Arc<MemoryAccountant>,
    bytes: usize,
}

impl Drop for SourcePageCharge {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
    }
}

/// Admit the actual extent before redb allocates it. Uncached pages retain
/// the charge until their last handle drops; cached pages hand ownership to
/// the writer's already-reserved fixed page cache. Decode/build reservations
/// cover other allocations and must not include this source buffer again.
///
/// Redb restores enclosing callbacks on return or unwind. Capture the exact
/// admission error separately because redb transports it as an I/O error.
pub(super) fn with_source_page_admission<T>(
    accountant: Arc<MemoryAccountant>,
    read: impl FnOnce() -> Result<T>,
) -> Result<T> {
    with_source_page_admission_for(
        accountant,
        "read_vector_generation_source_page",
        "Raise MEMORY_LIMIT before reading this maintained generation's source page.",
        read,
    )
}

/// Catalog reclamation reads cleanup evidence, not a generation being built
/// or served. Keep its physical source owner under the cleanup operation.
pub(super) fn with_catalog_reclamation_source_page_admission<T>(
    accountant: Arc<MemoryAccountant>,
    read: impl FnOnce() -> Result<T>,
) -> Result<T> {
    with_source_page_admission_for(
        accountant,
        "prepare_vector_generation",
        "Raise MEMORY_LIMIT before reclaiming this maintained route's catalog.",
        read,
    )
}

/// Covered journal deletion owns storage pages independently of any later
/// generation installation. Its reads must not imply generation-body work.
pub(super) fn with_journal_cleanup_source_page_admission<T>(
    accountant: Arc<MemoryAccountant>,
    read: impl FnOnce() -> Result<T>,
) -> Result<T> {
    with_source_page_admission_for(
        accountant,
        "truncate_vector_partition_journal",
        "Raise MEMORY_LIMIT before cleaning this maintained route's journal.",
        read,
    )
}

/// Frontier metadata can occupy an arbitrarily large physical extent even
/// when its decoded descriptor is bounded. Charge that extent before redb
/// allocates it, while retaining the preparation operation's early refusal.
pub(super) fn with_preparation_source_page_admission<T>(
    accountant: Arc<MemoryAccountant>,
    read: impl FnOnce() -> Result<T>,
) -> Result<T> {
    with_source_page_admission_for(
        accountant,
        "prepare_vector_generation",
        "Raise MEMORY_LIMIT before inspecting this maintained route.",
        read,
    )
}

fn with_source_page_admission_for<T>(
    accountant: Arc<MemoryAccountant>,
    operation: &'static str,
    hint: &'static str,
    read: impl FnOnce() -> Result<T>,
) -> Result<T> {
    static NEXT_SCOPE: AtomicU64 = AtomicU64::new(1);
    let refusal = Arc::new(SourceRefusal {
        marker: format!(
            "contextdb vector source-page refusal [{}]",
            NEXT_SCOPE.fetch_add(1, Ordering::Relaxed)
        ),
        error: parking_lot::Mutex::new(None),
    });
    let previous = SOURCE_REFUSAL.with(|slot| slot.replace(Some(Arc::clone(&refusal))));
    let _scope = SourceRefusalScope(previous);
    let admission = {
        let refusal = Arc::clone(&refusal);
        Arc::new(move |bytes: usize| {
            match accountant.try_allocate_for(bytes, "vector_maintenance", operation, hint) {
                Ok(()) => Ok(Box::new(SourcePageCharge {
                    accountant: Arc::clone(&accountant),
                    bytes,
                }) as redb::ReadPageCharge),
                Err(error) => {
                    let mut first = refusal.error.lock();
                    if first.is_none() {
                        *first = Some(error);
                    }
                    Err(std::io::Error::new(
                        std::io::ErrorKind::OutOfMemory,
                        refusal.marker.clone(),
                    ))
                }
            }
        }) as Arc<dyn Fn(usize) -> std::io::Result<redb::ReadPageCharge> + Send + Sync>
    };
    let result = redb::with_read_page_charge(admission, read);
    // A caller may have classified a post-publication refusal as a successful
    // publication with deferred cleanup. Preserve that successful outcome.
    // Fallback only serves callers that transported our own I/O marker without
    // using persistence's immediate typed adapter.
    match result {
        Err(error) => Err(take_source_page_refusal(&error.to_string()).unwrap_or(error)),
        Ok(value) => Ok(value),
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "test-seams"))]
mod cleanup_attribution_tests {
    use super::*;
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};
    use std::sync::atomic::AtomicUsize;

    #[derive(Clone, Copy)]
    enum Cleanup {
        Catalog,
        Journal,
    }

    impl Cleanup {
        fn operation(self) -> &'static str {
            match self {
                Self::Catalog => "prepare_vector_generation",
                Self::Journal => "truncate_vector_partition_journal",
            }
        }

        fn with_admission<T>(
            self,
            accountant: Arc<MemoryAccountant>,
            read: impl FnOnce() -> Result<T>,
        ) -> Result<T> {
            match self {
                Self::Catalog => with_catalog_reclamation_source_page_admission(accountant, read),
                Self::Journal => with_journal_cleanup_source_page_admission(accountant, read),
            }
        }
    }

    #[test]
    fn cleanup_page_admissions_keep_typed_attribution_and_release_their_source_owner() {
        use crate::memory_accounting::{take_budget_ledger_for_test, watch_budget_ledger_for_test};
        const CACHE_BYTES: usize = 8 * 1024 * 1024;
        const EVIDENCE_BYTES: usize = 9 * 1024 * 1024;
        const STARTUP_BUDGET_BYTES: usize = 64 * 1024 * 1024;

        for cleanup in [Cleanup::Catalog, Cleanup::Journal] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("cleanup-source.db");
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(cleanup.operation());
            let mut builder = redb::Database::builder();
            builder.set_cache_size(CACHE_BYTES);
            let db = builder.create(&path).unwrap();
            let payload = vec![7u8; EVIDENCE_BYTES];
            let write = db.begin_write().unwrap();
            write
                .open_table(definition)
                .unwrap()
                .insert(b"cleanup-evidence".as_slice(), payload.as_slice())
                .unwrap();
            write.commit().unwrap();
            drop(db);
            let db = builder.open(&path).unwrap();
            let read = db.begin_read().unwrap();
            // Prime only bounded table navigation metadata. The evidence
            // exceeds the fixed cache and still needs its own source owner.
            drop(read.open_table(definition).unwrap());
            let accountant = Arc::new(MemoryAccountant::with_budget(STARTUP_BUDGET_BYTES));
            accountant.set_budget(Some(CACHE_BYTES + 1024)).unwrap();
            accountant.try_allocate(CACHE_BYTES).unwrap();
            let largest_read = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&largest_read);
            let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
                observed.fetch_max(bytes, Ordering::SeqCst);
            }));
            let error = cleanup
                .with_admission(Arc::clone(&accountant), || {
                    read.open_table(definition)
                        .map_err(crate::persistence::RedbPersistence::storage_error)?
                        .get(b"cleanup-evidence".as_slice())
                        .map_err(crate::persistence::RedbPersistence::storage_error)
                })
                .err()
                .expect("the cold cleanup evidence extent cannot fit in one KiB");
            assert!(
                matches!(&error, Error::MemoryBudgetExceeded {
                    subsystem, operation, requested_bytes, available_bytes: 1024,
                    budget_limit_bytes, hint,
                } if subsystem == "vector_maintenance"
                    && operation == cleanup.operation()
                    && *requested_bytes > payload.len()
                    && *budget_limit_bytes == CACHE_BYTES + 1024
                    && !hint.contains("generation")),
                "{error:?}"
            );
            assert_eq!(
                contextdb_vector::VectorMaintenanceFailureDetails::from_error(&error).operation,
                cleanup.operation(),
                "the public failure details retain the declared cleanup operation"
            );
            assert!(largest_read.load(Ordering::SeqCst) < payload.len());
            assert_eq!(accountant.usage().used, CACHE_BYTES);

            accountant.set_budget(Some(STARTUP_BUDGET_BYTES)).unwrap();
            largest_read.store(0, Ordering::SeqCst);
            watch_budget_ledger_for_test();
            let table = cleanup
                .with_admission(Arc::clone(&accountant), || {
                    read.open_table(definition)
                        .map_err(crate::persistence::RedbPersistence::storage_error)
                })
                .unwrap();
            let (key, value) = cleanup
                .with_admission(Arc::clone(&accountant), || {
                    table
                        .iter()
                        .map_err(crate::persistence::RedbPersistence::storage_error)?
                        .next()
                        .unwrap()
                        .map_err(crate::persistence::RedbPersistence::storage_error)
                })
                .unwrap();
            drop(observation);
            assert!(largest_read.load(Ordering::SeqCst) >= payload.len());
            let ledger = take_budget_ledger_for_test();
            assert!(ledger.iter().any(|entry| {
                entry.what == format!("vector_maintenance/{}", cleanup.operation())
            }));
            assert!(
                !ledger
                    .iter()
                    .any(|entry| { entry.what.contains("read_vector_generation_source_page") })
            );
            assert_eq!(value.value(), payload.as_slice());
            let held = accountant.usage().used;
            assert!(held > CACHE_BYTES + payload.len());
            drop(value);
            assert_eq!(accountant.usage().used, held, "the key still owns the page");
            assert_eq!(key.value(), b"cleanup-evidence");
            drop(key);
            assert_eq!(
                accountant.usage().used,
                held,
                "the table's cached root still owns the page"
            );
            drop(table);
            assert_eq!(accountant.usage().used, CACHE_BYTES);
            accountant.release(CACHE_BYTES);
            assert_eq!(accountant.usage().used, 0);
            assert_eq!(accountant.underflow_count_for_test(), 0);
        }
    }
}
