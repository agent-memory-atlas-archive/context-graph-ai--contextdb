use super::*;
use crate::{Database, MaintenancePolicy};

#[test]
fn authenticated_graph_page_records_fit_the_fixed_64_kib_extent() {
    let graph = VectorPartitionGraphBytes::verified(vec![7; 128 * 1024]);
    let prepared = RedbPersistence::prepare_vector_partition_graph_pages(
        &VectorIndexRef::new("items", "embedding"),
        &VectorPartitionKey::unpartitioned(),
        11,
        11,
        true,
        b"generation-key",
        &graph,
    )
    .unwrap();

    assert_eq!(prepared.graph_bytes.len().div_ceil(prepared.page_bytes), 3);
    for body in prepared.graph_bytes.chunks(prepared.page_bytes).take(2) {
        assert_eq!(
            VECTOR_PARTITION_GRAPH_PAGE_LEAF_FRAME_BYTES
                + prepared.root_key.len()
                + VECTOR_PARTITION_GRAPH_PAGE_KEY_SUFFIX_BYTES
                + VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES
                + body.len(),
            VECTOR_PARTITION_GRAPH_PAGE_RECORD_BYTES
        );
    }
    let manifest: VectorPartitionGraphPageManifestRecord = decode_vector_partition_record_payload(
        VectorPartitionRecordKind::GraphPageManifest,
        &prepared.manifest_bytes,
    )
    .unwrap();
    let expected_page_bytes = VECTOR_PARTITION_GRAPH_PAGE_RECORD_BYTES
        - VECTOR_PARTITION_GRAPH_PAGE_LEAF_FRAME_BYTES
        - prepared.root_key.len()
        - VECTOR_PARTITION_GRAPH_PAGE_KEY_SUFFIX_BYTES
        - VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES;
    assert_eq!(manifest.page_bytes as usize, expected_page_bytes);
    assert_eq!(manifest.page_count, 3);
}

/// What the damaged catalog claims is already resident. The graph admission
/// asks for the recomputed charge less this, so the refusal names it.
const CATALOG_DECLARED_RESIDENT_BYTES: usize = 1;

#[test]
fn an_older_catalog_charge_is_recomputed_before_graph_decode() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("older-charge.db");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 24, EF_CONSTRUCTION = 64))", &HashMap::new()).unwrap();
    for id in 0..32 {
        db.execute(
            "INSERT INTO items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.run_maintenance_cycle().unwrap();
    let before = db.vector_memory_ownership_receipt_for_test().base_graph;
    db.close().unwrap();
    drop(db);
    {
        let file = redb::Database::open(&path).unwrap();
        let write = file.begin_write().unwrap();
        {
            let mut table = write
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .unwrap();
            let (key_guard, value) = table.iter().unwrap().next().unwrap().unwrap();
            let key = key_guard.value().to_vec();
            drop(key_guard);
            let mut catalog: VectorPartitionGenerationCatalogRecord =
                decode_vector_partition_record_payload(
                    VectorPartitionRecordKind::GenerationCatalog,
                    value.value(),
                )
                .unwrap();
            drop(value);
            catalog.base.resident_bytes = CATALOG_DECLARED_RESIDENT_BYTES as u64;
            let bytes = RedbPersistence::encode_vector_partition_record(
                VectorPartitionRecordKind::GenerationCatalog,
                &catalog,
            )
            .unwrap();
            table.insert(key.as_slice(), bytes.as_slice()).unwrap();
        }
        write.commit().unwrap();
    }
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let used = db.accountant().usage().used;
    // Headroom is derived from the charge this test is about rather than
    // guessed: half the resolved topology charge. It has to be large enough
    // that the query actually reaches the graph admission, and that each
    // persisted configuration write below can admit its own committed-proof
    // maintenance - both `set_memory_limit` calls would refuse otherwise, and
    // their `unwrap`s carry that end of the relationship. It has to stay
    // strictly under the topology charge so that admission is the thing that
    // fails; the refusal's own `requested_bytes` carries that end, which is
    // what keeps this test from passing on some cheaper refusal.
    let headroom = before / 2;
    assert!(
        headroom < before,
        "the graph cannot be admitted inside the headroom it is refused in: headroom={headroom} charge={before}"
    );
    db.set_memory_limit(Some(used + headroom)).unwrap();
    let sql = "SELECT id FROM items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3";
    let refusal = db
        .execute(sql, &HashMap::new())
        .expect_err("an understated catalog cannot admit its real graph");
    let Error::MemoryBudgetExceeded {
        subsystem,
        requested_bytes,
        ..
    } = &refusal
    else {
        panic!("the typed budget refusal, not {refusal:?}");
    };
    // The refusal is the recomputed topology charge, minus the single byte the
    // damaged catalog claimed was already resident. The bounded read relabels
    // the subsystem as it propagates, so the requested amount - not the label -
    // is what says this was the graph admission and not the proof maintenance
    // of the configuration write or the query's own candidate buffer.
    assert_ne!(
        subsystem, "content_identity",
        "the refusal is the graph admission, not proof setup: {refusal:?}"
    );
    assert_eq!(
        *requested_bytes,
        before - CATALOG_DECLARED_RESIDENT_BYTES,
        "the refused amount is the recomputed topology charge: {refusal:?}"
    );
    assert_eq!(db.vector_memory_ownership_receipt_for_test().base_graph, 0);
    db.set_memory_limit(None).unwrap();
    assert_eq!(db.execute(sql, &HashMap::new()).unwrap().rows.len(), 3);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test().base_graph,
        before
    );
    eprintln!(
        "catalog_declared_bytes={CATALOG_DECLARED_RESIDENT_BYTES} resolved_topology_charge={before} headroom={headroom} refused_bytes={requested_bytes} refused_before_decode=true retry=ok"
    );
}

#[test]
fn near_extent_sized_page_roots_are_streamed_and_every_disk_key_is_counted() {
    let graph = VectorPartitionGraphBytes::verified(vec![7; 128 * 1024]);
    let generation_key = vec![9; 65_400];
    let prepared = RedbPersistence::prepare_vector_partition_graph_pages(
        &VectorIndexRef::new("items", "embedding"),
        &VectorPartitionKey::unpartitioned(),
        11,
        11,
        true,
        &generation_key,
        &graph,
    )
    .unwrap();
    assert_eq!(prepared.root_key.len(), generation_key.len() + 1);
    let pages = prepared.graph_bytes.len().div_ceil(prepared.page_bytes);
    assert!(
        pages > 1_000,
        "small bodies exercise repeated near-64KiB keys"
    );
    let owned = prepared.root_key.capacity()
        + prepared.manifest_bytes.capacity()
        + prepared.graph_bytes.capacity();
    assert!(
        owned < 256 * 1024,
        "preparation retains only one body and root: {owned}"
    );
    assert_eq!(
        prepared.write_bytes(),
        prepared.root_key.len()
            + prepared.manifest_bytes.len()
            + graph.bytes.len()
            + pages
                * (prepared.root_key.len()
                    + VECTOR_PARTITION_GRAPH_PAGE_KEY_SUFFIX_BYTES
                    + VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES)
    );
    assert!(
        prepared.write_bytes() > owned * 100,
        "disk admission still includes every repeated physical key"
    );
}
