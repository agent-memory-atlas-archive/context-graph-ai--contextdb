use super::*;

#[test]
fn concrete_streamed_frame_covers_exact_capacities_and_root_edges() {
    let index = VectorIndexRef::new("items", "v");
    for text_bytes in [0, 5748, 65390, 65420, 65536, 100000] {
        let partition = VectorPartitionKey::from_components(vec![
            VectorPartitionComponent::Text("x".repeat(text_bytes)),
            VectorPartitionComponent::Integer(7),
            VectorPartitionComponent::Boolean(true),
            VectorPartitionComponent::Uuid(uuid::Uuid::nil()),
            VectorPartitionComponent::Timestamp(11),
            VectorPartitionComponent::TxId(TxId(13)),
        ]);
        for is_base in [true, false] {
            let generation_key = if is_base {
                RedbPersistence::vector_partition_base_generation_key_from_parts(
                    &index, &partition, 1,
                )
            } else {
                RedbPersistence::vector_partition_change_generation_key_from_parts(
                    &index, &partition, 1, 2,
                )
            };
            let root =
                RedbPersistence::vector_partition_graph_page_root_key(&generation_key, is_base);
            let body_limit = VECTOR_PARTITION_GRAPH_PAGE_RECORD_BYTES
                - root.len()
                - VECTOR_PARTITION_GRAPH_PAGE_KEY_SUFFIX_BYTES
                - VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES
                - VECTOR_PARTITION_GRAPH_PAGE_LEAF_FRAME_BYTES;
            for graph_bytes in [0, 64, 5748, 65536, 131072] {
                let bound = RedbPersistence::vector_streamed_frame_bytes(
                    &index,
                    &partition,
                    if is_base { 8 } else { 16 },
                    graph_bytes,
                )
                .unwrap();
                if graph_bytes == 0 {
                    assert_eq!(bound, 0);
                    continue;
                }
                let key = RedbPersistence::vector_partition_graph_page_key(&root, 0);
                let body = Vec::<u8>::with_capacity(
                    graph_bytes.min(body_limit) + VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES,
                );
                assert_eq!(bound, key.capacity() + body.capacity());
                assert!(bound + VECTOR_PARTITION_GRAPH_PAGE_LEAF_FRAME_BYTES <= 65536);
            }
        }
    }
}

#[test]
fn candidate_streamed_frame_is_sequential_max_and_empty_zero() {
    let index = VectorIndexRef::new("items", "v");
    let partition = VectorPartitionKey::from_components(vec![VectorPartitionComponent::Integer(0)]);
    let descriptor = VectorPartitionGenerationDescriptor {
        generation_id: 1,
        covered_tx: TxId(1),
        covered_lsn: Lsn(1),
        resident_bytes: 0,
        durable_bytes: 0,
        policy_revision: 0,
        hnsw_m: 4,
        hnsw_ef_construction: 64,
    };
    let mut candidate = VectorPartitionGenerationCandidate {
        base: VectorPartitionBaseGenerationRecord {
            index: index.clone(),
            partition_key: partition.clone(),
            generation_id: 1,
            base_tx: TxId(1),
            base_lsn: Lsn(1),
            highest_included_tx: TxId(1),
            highest_included_lsn: Lsn(1),
            dimension: 3,
            quantization: VectorQuantization::F32,
            algorithm_version: 1,
            graph_format_version: 1,
            graph: VectorPartitionGraphBytes::verified(vec![0; 5748]),
            state_digest: [0; 32],
        },
        change: Some(VectorPartitionChangeGenerationRecord {
            index: index.clone(),
            partition_key: partition.clone(),
            base_generation_id: 1,
            change_generation_id: 2,
            covered_through_tx: TxId(2),
            covered_through_lsn: Lsn(2),
            fresh_tail_from_tx: TxId(3),
            fresh_tail_from_lsn: Lsn(3),
            dimension: 3,
            quantization: VectorQuantization::F32,
            algorithm_version: 1,
            graph_format_version: 1,
            graph: VectorPartitionGraphBytes::verified(vec![0; 131072]),
        }),
        catalog: VectorPartitionGenerationCatalogRecord {
            index: index.clone(),
            partition_key: partition.clone(),
            base: descriptor,
            change: Some(descriptor),
            published_tx: TxId(2),
            published_lsn: Lsn(2),
        },
    };
    let base = RedbPersistence::vector_streamed_frame_bytes(&index, &partition, 8, 5748).unwrap();
    let change =
        RedbPersistence::vector_streamed_frame_bytes(&index, &partition, 16, 131072).unwrap();
    assert_eq!(
        RedbPersistence::vector_candidate_streamed_frame_bytes(&candidate).unwrap(),
        base.max(change)
    );
    assert!(base.max(change) < base + change);
    candidate.base.graph.bytes.clear();
    candidate.change.as_mut().unwrap().graph.bytes.clear();
    assert_eq!(
        RedbPersistence::vector_candidate_streamed_frame_bytes(&candidate).unwrap(),
        0
    );
}
