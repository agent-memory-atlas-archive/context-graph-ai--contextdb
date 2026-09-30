//! Instance-scoped observation of actual user-table mutations, compiled only for tests.
//! Bytes are serialized logical key/value bytes, not physical page or fsync traffic.
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TableWriteOperationForTest {
    Insert,
    Remove,
    DeleteTable,
    DirectoryInsert,
    DirectoryRemove,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableWriteForTest {
    pub table: String,
    pub operation: TableWriteOperationForTest,
    pub key: Vec<u8>,
    pub value_bytes: usize,
}

pub type TableWriteObserverForTest = Arc<dyn Fn(&TableWriteForTest) + Send + Sync>;
pub(crate) type ObserverSlot = Arc<Mutex<Option<TableWriteObserverForTest>>>;

pub struct TableWriteObservationGuardForTest {
    slot: ObserverSlot,
    installed: TableWriteObserverForTest,
    previous: Option<TableWriteObserverForTest>,
}

pub(crate) fn install(
    slot: ObserverSlot,
    observer: TableWriteObserverForTest,
) -> TableWriteObservationGuardForTest {
    let previous = slot.lock().unwrap().replace(observer.clone());
    TableWriteObservationGuardForTest {
        slot,
        installed: observer,
        previous,
    }
}

impl Drop for TableWriteObservationGuardForTest {
    fn drop(&mut self) {
        let mut slot = self.slot.lock().unwrap();
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &self.installed))
        {
            *slot = self.previous.take();
        }
    }
}

pub(crate) fn record(
    slot: &ObserverSlot,
    table: &str,
    operation: TableWriteOperationForTest,
    key: &[u8],
    value_bytes: usize,
) {
    let observer = slot.lock().unwrap().clone();
    if let Some(observer) = observer {
        observer(&TableWriteForTest {
            table: table.to_string(),
            operation,
            key: key.to_vec(),
            value_bytes,
        });
    }
}
