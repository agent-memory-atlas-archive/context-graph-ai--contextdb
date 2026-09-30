//! The durable index comparator runs inside Redb's B-tree, where it cannot
//! refuse. It must therefore be total and non-panicking over arbitrary stored
//! bytes while ordering every key the engine itself writes exactly as the
//! declaration says.

use super::*;
use crate::persistence::DurableRelationalIndexKey;
use contextdb_core::{DirectedValue, TotalOrdAsc, TotalOrdDesc};

fn key(parts: Vec<DirectedValue>, row: u64) -> Vec<u8> {
    DurableRelationalIndexKey::new(parts, RowId(row), TxId(0))
        .expect("encode a durable posting key")
        .0
}

fn ascending_text(text: &str, row: u64) -> Vec<u8> {
    key(
        vec![DirectedValue::Asc(TotalOrdAsc(Value::Text(
            text.to_owned(),
        )))],
        row,
    )
}

fn descending_text(text: &str, row: u64) -> Vec<u8> {
    key(
        vec![DirectedValue::Desc(TotalOrdDesc(Value::Text(
            text.to_owned(),
        )))],
        row,
    )
}

fn ascending_int(value: i64, row: u64) -> Vec<u8> {
    key(
        vec![DirectedValue::Asc(TotalOrdAsc(Value::Int64(value)))],
        row,
    )
}

fn compare(left: &[u8], right: &[u8]) -> Ordering {
    RedbPersistence::compare_relational_index_order_keys(left, right)
}

#[test]
fn valid_keys_keep_exactly_the_declared_sql_order() {
    let ascending = ["a", "b", "m", "z"]
        .into_iter()
        .enumerate()
        .map(|(row, text)| ascending_text(text, row as u64))
        .collect::<Vec<_>>();
    for pair in ascending.windows(2) {
        assert_eq!(compare(&pair[0], &pair[1]), Ordering::Less);
        assert_eq!(compare(&pair[1], &pair[0]), Ordering::Greater);
    }
    let descending = ["a", "b", "m", "z"]
        .into_iter()
        .enumerate()
        .map(|(row, text)| descending_text(text, row as u64))
        .collect::<Vec<_>>();
    for pair in descending.windows(2) {
        assert_eq!(
            compare(&pair[0], &pair[1]),
            Ordering::Greater,
            "a descending index stores the larger value first"
        );
    }
    // NULL is ordered, not refused, and keeps its declared NULLS-LAST place.
    let null = key(vec![DirectedValue::Asc(TotalOrdAsc(Value::Null))], 9);
    assert_eq!(compare(&ascending[3], &null), Ordering::Less);
    // The row breaks a tie between equal values, exactly as before.
    assert_eq!(
        compare(&ascending_text("a", 1), &ascending_text("a", 2)),
        Ordering::Less
    );
}

#[test]
fn malformed_stored_keys_are_ordered_deterministically_and_never_panic() {
    // Each of these is a stored byte string the declaration could not have
    // produced. The derived `RelationalIndexPosition` ordering panics on the
    // first pair and answers `Equal` for the second.
    let unlike_variant = ascending_int(7, 0);
    let mismatched_direction = descending_text("m", 0);
    let truncated = ascending_text("m", 0)[..4].to_vec();
    let garbage = vec![0xff_u8; 32];
    let empty: Vec<u8> = Vec::new();
    let valid = ascending_text("m", 0);

    let keys = [
        valid.clone(),
        unlike_variant.clone(),
        mismatched_direction.clone(),
        truncated,
        garbage,
        empty,
        ascending_text("a", 3),
        descending_text("z", 4),
    ];

    for left in &keys {
        assert_eq!(compare(left, left), Ordering::Equal, "reflexive");
        for right in &keys {
            let forward = compare(left, right);
            assert_eq!(
                forward.reverse(),
                compare(right, left),
                "antisymmetric for every stored byte string"
            );
            if left != right {
                assert_ne!(
                    forward,
                    Ordering::Equal,
                    "two different stored keys never tie"
                );
            }
            // Deterministic: the same pair answers the same way every time.
            assert_eq!(forward, compare(left, right));
        }
    }

    assert_ne!(
        compare(&valid, &unlike_variant),
        Ordering::Equal,
        "an unorderable type pair is decided, not panicked on"
    );
    assert_ne!(
        compare(&valid, &mismatched_direction),
        Ordering::Equal,
        "a direction mismatch is decided, not silently tied"
    );
}

#[test]
fn the_proof_tree_and_the_durable_index_share_one_comparator() {
    // The proof tree's fallible comparator is the same function; where it can
    // order a pair, the durable index orders it identically, so the two never
    // disagree about a valid key.
    for (left, right) in [
        (ascending_text("a", 0), ascending_text("b", 1)),
        (ascending_text("b", 1), ascending_text("a", 0)),
        (descending_text("a", 0), descending_text("b", 1)),
        (ascending_text("a", 0), ascending_text("a", 1)),
    ] {
        let proof = validated_posting_order(&left, &right).expect("a valid pair orders");
        assert_eq!(proof, compare(&left, &right));
    }
    // And where it cannot, the durable index still answers without panicking.
    assert!(validated_posting_order(&ascending_text("a", 0), &ascending_int(1, 0)).is_err());
    assert_ne!(
        compare(&ascending_text("a", 0), &ascending_int(1, 0)),
        Ordering::Equal
    );
}
