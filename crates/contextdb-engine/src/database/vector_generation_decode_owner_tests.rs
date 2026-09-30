use super::*;
use std::sync::atomic::AtomicUsize;

fn resident_owner_bound(quantization: VectorQuantization) {
    let layout = VectorIndexLayout::unpartitioned(64, quantization);
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("decode_items", "embedding"),
        VectorPartitionKey::unpartitioned(),
    );
    let policy = layout.resolve_policy(16, 1);
    let direct =
        VectorGenerationWorkspacePlan::new(32, 16, &layout, &partition, policy, 8192).unwrap();
    let mut resident = direct;
    resident.assign_raw_decode_owner(true, false);
    assert_eq!(resident.raw_decode_scratch_bytes, 0);
    assert_eq!(
        resident.build_phase_bytes(),
        direct.build_phase_bytes() - direct.raw_decode_scratch_bytes
    );
    assert!(resident.raw_entry_bytes > resident.selected_entry_bytes);
    assert_eq!(resident.graph_build_bytes, direct.graph_build_bytes);
    assert_eq!(resident.raw_entry_bytes, direct.raw_entry_bytes);
    assert_eq!(resident.selected_entry_bytes, direct.selected_entry_bytes);
    assert_eq!(
        resident.durable_metadata_bytes,
        direct.durable_metadata_bytes
    );
    assert_eq!(
        resident.encoding_phase_bytes(4096),
        direct.encoding_phase_bytes(4096)
    );
    assert_eq!(
        resident.persistence_phase_bytes(4096),
        direct.persistence_phase_bytes(4096)
    );
    assert_eq!(
        resident.prepared_phase_bytes(4096),
        direct.prepared_phase_bytes(4096)
    );
    let accountant = MemoryAccountant::with_budget(resident.build_phase_bytes());
    assert!(
        accountant
            .try_allocate_for(
                direct.build_phase_bytes(),
                "vector_index",
                "prepare_vector_generation",
                "test"
            )
            .is_err()
    );
    assert_eq!(accountant.usage().used, 0);
    accountant
        .try_allocate_for(
            resident.build_phase_bytes(),
            "vector_index",
            "prepare_vector_generation",
            "test",
        )
        .unwrap();
    assert_eq!(accountant.usage().used, resident.build_phase_bytes());
    accountant.release(resident.build_phase_bytes());
    assert_eq!(accountant.usage().used, 0);
}

#[test]
fn resident_f32_generation_funds_snapshot_without_a_decoder_owner() {
    resident_owner_bound(VectorQuantization::F32);
}

#[test]
fn resident_sq8_generation_funds_snapshot_without_a_decoder_owner() {
    resident_owner_bound(VectorQuantization::SQ8);
}

#[test]
fn resident_sq4_generation_funds_snapshot_without_a_decoder_owner() {
    resident_owner_bound(VectorQuantization::SQ4);
}

#[test]
fn direct_cold_and_delete_carrier_generations_keep_the_decode_floor() {
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let layout = VectorIndexLayout::unpartitioned(1, quantization);
        let partition = VectorPartitionRef::new(
            VectorIndexRef::new("retained_items", "embedding"),
            VectorPartitionKey::unpartitioned(),
        );
        // Retained raw history has no live graph points, but still decodes.
        let direct = VectorGenerationWorkspacePlan::new(
            1,
            0,
            &layout,
            &partition,
            layout.resolve_policy(0, 1),
            0,
        )
        .unwrap();
        assert!(direct.raw_decode_scratch_bytes > 1024 * 1024);
        assert_eq!(direct.selected_entry_bytes, 0);
        for (state_present, delete_carriers) in [(false, false), (false, true), (true, true)] {
            let mut plan = direct;
            plan.assign_raw_decode_owner(state_present, delete_carriers);
            assert_eq!(
                plan.raw_decode_scratch_bytes,
                direct.raw_decode_scratch_bytes
            );
            assert_eq!(plan.build_phase_bytes(), direct.build_phase_bytes());
            let accountant = MemoryAccountant::with_budget(plan.build_phase_bytes() - 1);
            assert!(
                accountant
                    .try_allocate_for(
                        plan.build_phase_bytes(),
                        "vector_index",
                        "prepare_vector_generation",
                        "test",
                    )
                    .is_err()
            );
            assert_eq!(accountant.usage().used, 0);
        }
    }
}

#[test]
fn dormant_partition_loader_admits_its_decode_floor_before_reads_and_releases() {
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("decode-owner.db");
        Database::open(&path).unwrap().close().unwrap();
        let persistence = Arc::new(RedbPersistence::open_with_cache(&path, 0).unwrap());
        let partition = VectorPartitionRef::new(
            VectorIndexRef::new("decode_items", "embedding"),
            VectorPartitionKey::unpartitioned(),
        );
        let entry = VectorEntry {
            index: partition.index.clone(),
            row_id: RowId(1),
            vector: vec![0.25],
            created_tx: TxId(1),
            deleted_tx: None,
            lsn: Lsn(1),
        };
        persistence
            .append_vector_entries_batch(
                std::slice::from_ref(&entry),
                &HashMap::from([(partition.index.clone(), quantization)]),
            )
            .unwrap();
        persistence
            .append_vector_partition_memberships_batch(&[VectorPartitionMembershipRecord {
                index: partition.index.clone(),
                partition_key: partition.partition_key.clone(),
                row_id: entry.row_id,
                row_created_tx: entry.created_tx,
                row_lsn: entry.lsn,
                vector_created_tx: entry.created_tx,
                vector_lsn: entry.lsn,
                visible_from: entry.created_tx,
                deleted_tx: None,
            }])
            .unwrap();
        let raw_directory = [RawVectorDirectoryEntry {
            row_id: entry.row_id,
            created_tx: entry.created_tx,
            deleted_tx: None,
            lsn: entry.lsn,
        }];
        let mut loader = DurableRawVectorPartitionLoader {
            source: DurableRawVectorSource::Writable(persistence),
            accountant: Arc::new(MemoryAccountant::with_budget(1024 * 1024)),
            partition,
            dimension: 1,
            quantization,
        };
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&reads);
        let observation = redb::observe_backend_reads(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        }));
        let error = match loader.load(&raw_directory) {
            Err(error) => error,
            Ok(_) => panic!("loader omitted its independently owned decoder scratch"),
        };
        assert!(matches!(error, Error::MemoryBudgetExceeded {
            subsystem, operation, requested_bytes, available_bytes, budget_limit_bytes, ..
        } if subsystem == "vector_store" && operation == "load_raw_vector_partition"
            && requested_bytes > 1024 * 1024 && available_bytes == 1024 * 1024
            && budget_limit_bytes == 1024 * 1024));
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        drop(observation);
        assert_eq!(loader.accountant.usage().used, 0);
        loader.accountant = Arc::new(MemoryAccountant::with_budget(2 * 1024 * 1024));
        let loaded = loader.load(&raw_directory).unwrap();
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].vector, vec![0.25]);
        let retained = loaded.resident_bytes + loaded.workspace_bytes;
        assert_eq!(loader.accountant.usage().used, retained);
        assert!(
            retained < 1024 * 1024,
            "decoder scratch died before publication"
        );
        drop(loaded);
        loader.accountant.release(retained);
        assert_eq!(loader.accountant.usage().used, 0);
    }
}
