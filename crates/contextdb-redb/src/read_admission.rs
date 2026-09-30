//! Operation-scoped admission before allocating a source page. The caller
//! keeps admitted bytes owned until its read handles have been released.
use std::{cell::RefCell, io, sync::Arc};
use std::{marker::PhantomData, rc::Rc};
type Admission = Arc<dyn Fn(usize) -> io::Result<()> + Send + Sync>;

/// A charge a caller's admission hands back for one page. The page the read
/// cache does not keep carries it and drops it when its last holder drops the
/// page, so the caller's charge lasts exactly as long as the page does.
pub type ReadPageCharge = Box<dyn Send + Sync>;
type ChargingAdmission = Arc<dyn Fn(usize) -> io::Result<ReadPageCharge> + Send + Sync>;

enum ActiveAdmission {
    Scoped(Admission),
    Charging(ChargingAdmission),
}

thread_local! { static ACTIVE: RefCell<Vec<ActiveAdmission>> = const { RefCell::new(Vec::new()) }; }

struct ResetAdmission;
impl Drop for ResetAdmission {
    fn drop(&mut self) {
        ACTIVE.with(|active| {
            active.borrow_mut().pop();
        });
    }
}

/// Execute synchronous source reads with admission before each cache-miss
/// allocation. Nested operations restore the enclosing admission on unwind.
#[doc(hidden)]
pub fn with_read_memory_admission<R>(admit: Admission, operation: impl FnOnce() -> R) -> R {
    ACTIVE.with(|active| active.borrow_mut().push(ActiveAdmission::Scoped(admit)));
    let _reset = ResetAdmission;
    operation()
}

/// Like [`with_read_memory_admission`], but the admission returns a charge
/// for each page. A page the read cache keeps is released from its charge at
/// once, because the cache's own budget holds it; a page the cache does not
/// keep carries the charge until its last holder drops it.
#[doc(hidden)]
pub fn with_read_page_charge<R>(admit: ChargingAdmission, operation: impl FnOnce() -> R) -> R {
    ACTIVE.with(|active| active.borrow_mut().push(ActiveAdmission::Charging(admit)));
    let _reset = ResetAdmission;
    operation()
}

pub(crate) fn admit_read_memory(bytes: usize) -> io::Result<Option<ReadPageCharge>> {
    ACTIVE.with(|active| match active.borrow().last() {
        Some(ActiveAdmission::Scoped(admit)) => admit(bytes).map(|()| None),
        Some(ActiveAdmission::Charging(admit)) => admit(bytes).map(Some),
        None => Ok(None),
    })
}

type ReadObserver = Arc<dyn Fn(usize) + Send + Sync>;
thread_local! { static READ_OBSERVER: RefCell<Option<ReadObserver>> = const { RefCell::new(None) }; }

/// Observe bytes returned by successful backend reads on this thread. Cache
/// hits contribute zero. This measures storage API I/O, not physical device
/// traffic: the operating system may satisfy a file read from its page cache.
#[doc(hidden)]
pub fn observe_backend_reads(observer: ReadObserver) -> BackendReadObservation {
    BackendReadObservation {
        previous: READ_OBSERVER.with(|slot| slot.replace(Some(observer))),
        _thread: PhantomData,
    }
}

#[doc(hidden)]
pub struct BackendReadObservation {
    previous: Option<ReadObserver>,
    _thread: PhantomData<Rc<()>>,
}

impl Drop for BackendReadObservation {
    fn drop(&mut self) {
        READ_OBSERVER.with(|slot| slot.replace(self.previous.take()));
    }
}

pub(crate) fn record_backend_read(bytes: usize) {
    let observer = READ_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer(bytes);
    }
}
