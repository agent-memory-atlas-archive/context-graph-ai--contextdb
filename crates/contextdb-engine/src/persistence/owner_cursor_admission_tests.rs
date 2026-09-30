use super::*;
use std::cell::RefCell;
use std::os::unix::fs::PermissionsExt as _;
use std::rc::Rc;

type ReaderDescriptorValidationHook = ([u8; 16], Box<dyn FnOnce()>);

thread_local! {
    static BEFORE_PROVIDER_ADMISSION: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static BEFORE_READER_DESCRIPTOR_VALIDATION: RefCell<Option<ReaderDescriptorValidationHook>> = const { RefCell::new(None) };
}

pub(super) fn before_reader_descriptor_validation_for_test(snapshot: &RegisteredReaderSnapshot) {
    let hook = BEFORE_READER_DESCRIPTOR_VALIDATION.with(|hook| {
        let mut hook = hook.borrow_mut();
        if hook
            .as_ref()
            .is_some_and(|(identity, _)| *identity == snapshot.handle_identity)
        {
            hook.take().map(|(_, operation)| operation)
        } else {
            None
        }
    });
    if let Some(hook) = hook {
        hook();
    }
}

pub(super) fn before_provider_admission_for_test() {
    BEFORE_PROVIDER_ADMISSION.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
}

fn writer() -> (tempfile::TempDir, RedbPersistence, PathBuf) {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = directory.path().join("owner-admission.db");
    let database = crate::Database::open(&path).expect("create a durable store");
    database.close().expect("close the seed writer");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    let persistence =
        RedbPersistence::open_with_cache_in_runtime_dir(&path, REDB_CACHE_BYTES, Some(&runtime))
            .expect("open the writer with a task-scoped registry");
    (directory, persistence, runtime)
}

fn assert_coordination_refusal(error: Error, path: &Path, cause: &str) {
    assert!(
        matches!(&error, Error::ReaderCoordinationUnavailable {
            operation,
            path: actual_path,
            reason,
        } if operation == "owner cursor pin"
            && actual_path == &path.display().to_string()
            && reason.contains(cause)),
        "expected owner cursor coordination refusal at {} caused by {cause}: {error:?}",
        path.display(),
    );
}

fn snapshot_paths(runtime: &Path, identity: (u64, u64)) -> Vec<PathBuf> {
    let directory = reader_snapshot_directory(runtime, identity);
    match std::fs::read_dir(directory) {
        Ok(entries) => entries
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "snapshot")
            })
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("cannot enumerate test registrations: {error}"),
    }
}

#[test]
// Exclusive guards intentionally force contention with the admission read locks.
#[allow(clippy::readonly_write_lock)]
fn owner_cursor_persistence_guard_contention_refuses_before_lock_release() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    {
        let held = persistence.lock_file.write().unwrap();
        assert_coordination_refusal(
            persistence
                .owner_cursor_pin()
                .err()
                .expect("busy companion refuses"),
            persistence.path(),
            "companion ownership state is unavailable",
        );
        assert!(held.is_some());
    }
    {
        let held = persistence.db.write().unwrap();
        assert_coordination_refusal(
            persistence
                .owner_cursor_pin()
                .err()
                .expect("busy database refuses"),
            persistence.path(),
            "Redb handle state is unavailable",
        );
        assert!(held.is_some());
    }
    {
        let held = persistence.writer_file.write().unwrap();
        assert_coordination_refusal(
            persistence
                .owner_cursor_pin()
                .err()
                .expect("busy writer file refuses"),
            persistence.path(),
            "writer file state is unavailable",
        );
        assert!(held.is_some());
    }
    assert!(snapshot_paths(&runtime, identity).is_empty());
    assert!(!OWNER_CURSOR_COORDINATION_NONBLOCKING.with(|mode| mode.get()));
    drop(
        persistence
            .owner_cursor_pin()
            .expect("retry after guard release"),
    );
}

#[test]
fn owner_cursor_refresh_contention_refuses_before_refresh_release() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    persistence
        .with_db(|db| {
            db.with_external_snapshot_refresh_lock_for_test(|| {
                // Ordinary root capture does not acquire the provider refresh lock.
                drop(
                    db.begin_read()
                        .expect("local root capture remains available"),
                );
                assert_coordination_refusal(
                    persistence
                        .owner_cursor_pin()
                        .err()
                        .expect("busy refresh refuses"),
                    persistence.path(),
                    "external snapshot refresh is held by another operation",
                );
                // The refresh stays held through these assertions; no concurrent
                // release or timer can make a blocking admission pass this proof.
                assert!(snapshot_paths(&runtime, identity).is_empty());
                let owner = persistence
                    .writer_file
                    .read()
                    .unwrap()
                    .as_ref()
                    .cloned()
                    .unwrap();
                assert!(
                    !owner
                        .unknown_reader_presence(&ReaderSnapshotRegistry::Complete(Vec::new()))
                        .unwrap()
                );
                assert!(!OWNER_CURSOR_COORDINATION_NONBLOCKING.with(|mode| mode.get()));
                Ok(())
            })
        })
        .unwrap();
    drop(
        persistence
            .owner_cursor_pin()
            .expect("retry after refresh release"),
    );
}

#[test]
fn owner_cursor_registry_contention_is_retryable_before_lock_release() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let mut held = ReaderSnapshotRegistryLock::create(&runtime, identity).unwrap();
    held.lock_exclusive().unwrap();

    // The held description stays locked across the refusal assertion. There is
    // no timer or concurrent unlock that could make a blocking implementation pass.
    let error = persistence
        .owner_cursor_pin_for_admission()
        .err()
        .expect("occupied registry yields admission");
    assert!(matches!(&error, OwnerCursorPinAdmissionError::Busy(_)));
    assert_coordination_refusal(
        error.into_error(),
        &runtime,
        "reader snapshot registry lock is held by another operation",
    );
    assert!(held.locked);
    assert!(snapshot_paths(&runtime, identity).is_empty());
    held.unlock().unwrap();
    drop(
        persistence
            .owner_cursor_pin()
            .expect("retry succeeds after registry release"),
    );
}

#[test]
fn owner_cursor_fence_contention_refuses_before_either_lock_release() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let (database_identity, _, _, _) = published_writer_identity(persistence.path()).unwrap();

    for admission in [true, false] {
        let held = purge_publication::hold_reader_lock_for_test(
            &runtime,
            identity,
            database_identity,
            admission,
        );
        assert_coordination_refusal(
            persistence
                .owner_cursor_pin()
                .err()
                .expect("occupied fence refuses admission"),
            &runtime,
            "authoritative erasure publication fence is held by another operation",
        );
        assert!(snapshot_paths(&runtime, identity).is_empty());
        // Keep the exclusive fence description open until after the refusal.
        assert!(held.metadata().is_ok());
        drop(held);
        drop(
            persistence
                .owner_cursor_pin()
                .expect("retry succeeds after fence release"),
        );
    }
}

#[test]
fn owner_cursor_provider_contention_refuses_and_inventory_clears_teardown_fallback() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let held = Rc::new(RefCell::new(None));
    let hook_held = Rc::clone(&held);
    let hook_runtime = runtime.clone();
    BEFORE_PROVIDER_ADMISSION.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            assert_eq!(snapshot_paths(&hook_runtime, identity).len(), 1);
            let mut registry = ReaderSnapshotRegistryLock::open(&hook_runtime, identity).unwrap();
            registry.lock_exclusive().unwrap();
            *hook_held.borrow_mut() = Some(registry);
        }));
    });

    let error = persistence
        .owner_cursor_pin_for_admission()
        .err()
        .expect("provider contention yields admission");
    assert!(
        matches!(&error, OwnerCursorPinAdmissionError::Busy(_)),
        "healthy registry scan contention reaches the bounded admission retry"
    );
    assert_coordination_refusal(
        error.into_error(),
        &runtime,
        "external snapshot admission failed",
    );
    assert!(held.borrow().as_ref().unwrap().locked);
    let stale = snapshot_paths(&runtime, identity);
    assert_eq!(
        stale.len(),
        1,
        "busy-registry cleanup leaves only the existing stale-file fallback"
    );
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&stale[0])
        .unwrap();
    fs2::FileExt::try_lock_exclusive(&file).expect("fallback closed the snapshot's lifetime lock");
    fs2::FileExt::unlock(&file).unwrap();
    let owner = persistence
        .writer_file
        .read()
        .unwrap()
        .as_ref()
        .cloned()
        .unwrap();
    assert!(
        !owner
            .unknown_reader_presence(&ReaderSnapshotRegistry::Complete(Vec::new()))
            .unwrap(),
        "refused admission releases physical reader presence before registry release",
    );

    drop(held.borrow_mut().take());
    assert!(matches!(
        capture_reader_snapshot_registry(identity, Some(&runtime)),
        ReaderSnapshotRegistry::Complete(snapshots) if snapshots.is_empty()
    ));
    assert!(
        snapshot_paths(&runtime, identity).is_empty(),
        "the next inventory removes the unlocked fallback"
    );
    assert!(!OWNER_CURSOR_COORDINATION_NONBLOCKING.with(|mode| mode.get()));
    drop(
        persistence
            .owner_cursor_pin()
            .expect("retry succeeds after provider registry release"),
    );
}

#[test]
fn untrusted_provider_scan_registry_is_a_terminal_typed_refusal() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let hook_runtime = runtime.clone();
    BEFORE_PROVIDER_ADMISSION.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let registry = ReaderSnapshotRegistryLock::open(&hook_runtime, identity).unwrap();
            std::fs::set_permissions(&registry.path, std::fs::Permissions::from_mode(0o644))
                .unwrap();
        }));
    });
    let error = persistence.owner_cursor_pin_for_admission().err().unwrap();
    assert!(matches!(&error, OwnerCursorPinAdmissionError::Refused(_)));
    assert_coordination_refusal(error.into_error(), &runtime, "registry is indeterminate");
    let path = reader_snapshot_registry_lock_path(&runtime, identity);
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(persistence.owner_cursor_pin().unwrap());
}

#[test]
fn provider_io_wouldblock_does_not_imply_registry_scan_contention() {
    let error = redb::TransactionError::Storage(redb::StorageError::Io(std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        "reader snapshot registry scan is held by another operation",
    )));
    assert!(
        !writer_file::is_reader_registry_scan_contention(&error),
        "an arbitrary provider I/O error with the same kind and text stays terminal"
    );
}

#[test]
fn reader_departing_during_registry_scan_releases_its_stale_registration() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let earlier = persistence.owner_cursor_pin().unwrap();
    let handle = earlier._registration.snapshot.handle_identity;
    let departed = Rc::new(RefCell::new(Some(earlier)));
    let hook_departed = Rc::clone(&departed);
    BEFORE_READER_DESCRIPTOR_VALIDATION.with(|hook| {
        *hook.borrow_mut() = Some((
            handle,
            Box::new(move || {
                // The scan holds the registry and has just observed this reader's
                // locked registration. Nonblocking teardown releases the snapshot
                // lifetime lock and data descriptor before validation resumes.
                drop(hook_departed.borrow_mut().take());
            }),
        ));
    });
    let admitted = persistence
        .owner_cursor_pin()
        .expect("a reader departing during the scan is authenticated stale state");
    assert!(
        departed.borrow().is_none(),
        "the real teardown ran inside the scan"
    );
    drop(admitted);
    assert!(matches!(
        capture_reader_snapshot_registry(identity, Some(&runtime)),
        ReaderSnapshotRegistry::Complete(snapshots) if snapshots.is_empty()
    ));
}

#[test]
fn locked_registry_snapshot_without_its_data_descriptor_refuses_terminally() {
    let (_directory, persistence, _runtime) = writer();
    let earlier = persistence.owner_cursor_pin().unwrap();
    let registration = earlier._registration.registration.as_ref().unwrap();
    let mut invalid = earlier._registration.snapshot.clone();
    invalid.descriptor_number = u32::MAX;
    std::fs::write(
        &registration.path,
        encode_reader_snapshot(&invalid).unwrap(),
    )
    .unwrap();
    let error = persistence.owner_cursor_pin_for_admission().err().unwrap();
    assert!(matches!(&error, OwnerCursorPinAdmissionError::Refused(_)));
    assert_coordination_refusal(
        error.into_error(),
        registration.path.parent().unwrap().parent().unwrap(),
        "registry is indeterminate",
    );
    std::fs::write(
        &registration.path,
        encode_reader_snapshot(&earlier._registration.snapshot).unwrap(),
    )
    .unwrap();
    drop(earlier);
    drop(persistence.owner_cursor_pin().unwrap());
}

#[cfg(feature = "test-seams")]
#[test]
fn replaced_provider_scan_registry_is_a_terminal_typed_refusal() {
    use read_persistence_test_scaffold::pause_reader_snapshot_registry_scan_for_test;

    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let persistence = Arc::new(persistence);
    let pause = pause_reader_snapshot_registry_scan_for_test(persistence.path()).unwrap();
    let arriving = Arc::clone(&persistence);
    let reader = std::thread::spawn(move || {
        let error = arriving.owner_cursor_pin_for_admission().err().unwrap();
        let terminal = matches!(&error, OwnerCursorPinAdmissionError::Refused(_));
        (terminal, error.into_error())
    });
    let observed = pause.wait_until_inspected_for_test();
    let parked = observed.registry_directory.with_extension("parked");
    std::fs::rename(&observed.registry_directory, &parked).unwrap();
    std::fs::create_dir(&observed.registry_directory).unwrap();
    std::fs::set_permissions(
        &observed.registry_directory,
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    pause.resume_for_test();
    let (terminal, error) = reader.join().unwrap();
    assert!(terminal, "replaced scan state never reaches Busy");
    assert_coordination_refusal(
        error,
        observed.registry_directory.parent().unwrap(),
        "registry is indeterminate",
    );
    // Refused admission may already remove the empty replacement while
    // releasing its registration. Restore the pinned original in either case.
    if let Err(error) = std::fs::remove_dir(&observed.registry_directory) {
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    }
    std::fs::rename(&parked, &observed.registry_directory).unwrap();
    pause.finish_for_test();
    assert!(matches!(
        capture_reader_snapshot_registry(identity, Some(&runtime)),
        ReaderSnapshotRegistry::Complete(snapshots) if snapshots.is_empty()
    ));
    drop(persistence.owner_cursor_pin().unwrap());
}

#[test]
fn poisoned_owner_cursor_refresh_is_a_terminal_typed_coordination_refusal() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let poisoned = catch_unwind(AssertUnwindSafe(|| {
        persistence
            .with_db(|db| {
                db.with_external_snapshot_refresh_lock_for_test(|| {
                    panic!("poison only the provider refresh mutex");
                });
                Ok(())
            })
            .unwrap();
    }));
    assert!(poisoned.is_err());
    let error = persistence
        .owner_cursor_pin_for_admission()
        .err()
        .expect("poisoned provider refresh refuses admission");
    assert!(matches!(&error, OwnerCursorPinAdmissionError::Refused(_)));
    assert_coordination_refusal(
        error.into_error(),
        persistence.path(),
        "Poisoned internal lock",
    );
    assert!(snapshot_paths(&runtime, identity).is_empty());
}

#[test]
fn untrusted_owner_cursor_registry_is_a_terminal_typed_refusal() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let registry = ReaderSnapshotRegistryLock::create(&runtime, identity).unwrap();
    std::fs::set_permissions(&registry.path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let error = persistence
        .owner_cursor_pin_for_admission()
        .err()
        .expect("an untrusted registry refuses admission");
    assert!(matches!(&error, OwnerCursorPinAdmissionError::Refused(_)));
    assert_coordination_refusal(
        error.into_error(),
        &runtime,
        "not a trusted private regular file",
    );
    assert!(snapshot_paths(&runtime, identity).is_empty());
    std::fs::set_permissions(&registry.path, std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(
        persistence
            .owner_cursor_pin()
            .expect("restored trust admits the cursor"),
    );
}

#[test]
fn a_still_linked_replaced_registry_lock_is_not_temporary_contention() {
    let (_directory, persistence, runtime) = writer();
    let identity = persistence.physical_identity().unwrap();
    let mut registry = ReaderSnapshotRegistryLock::create(&runtime, identity).unwrap();
    let parked = registry.path.with_extension("parked");
    std::fs::rename(&registry.path, &parked).unwrap();
    let replacement = ReaderSnapshotRegistryLock::create(&runtime, identity).unwrap();
    let error = registry
        .try_lock_exclusive()
        .expect_err("a linked replaced descriptor is unsafe");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(!registry.locked);
    drop(replacement);
    std::fs::remove_file(&parked).unwrap();
}

#[cfg(feature = "test-seams")]
#[test]
fn a_registry_replaced_before_validation_is_a_terminal_typed_admission_refusal() {
    use read_persistence_test_scaffold::{
        ReaderRegistrationLockPhase, pause_reader_registration_lock_for_test,
    };

    let (_directory, persistence, _runtime) = writer();
    let persistence = Arc::new(persistence);
    let paths = persistence.owner_cursor_registry_paths_for_test();
    let pause = pause_reader_registration_lock_for_test(
        persistence.path(),
        ReaderRegistrationLockPhase::DescriptorOpened,
    )
    .unwrap();
    let arriving = Arc::clone(&persistence);
    let reader = std::thread::spawn(move || {
        let error = arriving.owner_cursor_pin_for_admission().err().unwrap();
        let terminal = matches!(&error, OwnerCursorPinAdmissionError::Refused(_));
        (terminal, error.into_error())
    });
    assert!(pause.wait_for_attempt_for_test(1, std::time::Duration::from_secs(30)));
    let parked = paths[0].with_extension("parked");
    std::fs::rename(&paths[0], &parked).unwrap();
    let replacement = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&paths[0])
        .unwrap();
    replacement
        .set_permissions(std::fs::Permissions::from_mode(0o600))
        .unwrap();
    pause.stop_pausing_for_test();
    let (terminal, error) = reader.join().unwrap();
    std::fs::remove_file(&paths[0]).unwrap();
    std::fs::rename(&parked, &paths[0]).unwrap();
    assert!(terminal, "unsafe replacement must not become Busy");
    assert_coordination_refusal(
        error,
        paths[0].parent().unwrap(),
        "not a trusted private regular file",
    );
    assert_eq!(pause.attempts_for_test(), 1);
}
