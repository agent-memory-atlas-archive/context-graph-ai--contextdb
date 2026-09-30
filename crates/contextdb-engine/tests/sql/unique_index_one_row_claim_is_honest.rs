//! The rejection reason "chosen index is UNIQUE and reads at most one row"
//! requires more than `whole_key && unique`, which only means every
//! declared column of the index was matched -- not that the match implies a
//! single value per column. `WHERE u IN (1, 2)` and `WHERE u > 0 AND u < 10`
//! on a UNIQUE column are both whole-key matches that can return more than
//! one row, so EXPLAIN must not make that claim for them.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

const ONE_ROW_REASON: &str =
    "chosen index is UNIQUE and reads at most one row, so no sort is needed";

fn seed() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE unique_probe (id INTEGER PRIMARY KEY, u INTEGER NOT NULL UNIQUE, seq INTEGER NOT NULL)",
        &empty(),
    )
    .expect("create the unique_probe table");
    // Explicit, so it is eligible for every predicate shape below -- the
    // automatic index a column-level UNIQUE declares is excluded from a
    // range predicate by its own `automatic_index_supports_pick` rule, which
    // would otherwise remove it from candidacy and hide this finding.
    db.execute(
        "CREATE INDEX unique_probe_u_explicit_idx ON unique_probe (u)",
        &empty(),
    )
    .expect("create the explicit unique-matching index");
    db.execute(
        "CREATE INDEX unique_probe_u_seq_idx ON unique_probe (u, seq)",
        &empty(),
    )
    .expect("create the composite sort-eliding candidate");
    for (id, u, seq) in [(1_i64, 1_i64, 5_i64), (2, 2, 6), (3, 3, 7)] {
        db.execute(
            "INSERT INTO unique_probe (id, u, seq) VALUES ($id, $u, $seq)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("u".into(), Value::Int64(u)),
                ("seq".into(), Value::Int64(seq)),
            ]),
        )
        .expect("insert a unique_probe row");
    }
    db
}

fn composite_rejected_reason(explained: &contextdb_engine::ExplainOutput) -> String {
    explained
        .indexes_considered
        .iter()
        .find(|c| c.name == "unique_probe_u_seq_idx")
        .unwrap_or_else(|| {
            panic!(
                "explain omitted the composite candidate: {:?}",
                explained.indexes_considered
            )
        })
        .rejected_reason
        .to_string()
}

/// `u = 1` names exactly one row on a UNIQUE column: the one-row claim is
/// true, so it may be made.
#[test]
fn equality_on_a_unique_column_may_honestly_claim_one_row() {
    let db = seed();
    let explained = db
        .explain_output("SELECT id FROM unique_probe WHERE u = 1 ORDER BY seq ASC")
        .expect("explain must succeed");
    assert_eq!(
        composite_rejected_reason(&explained),
        ONE_ROW_REASON,
        "a genuine single-value equality probe on a UNIQUE column may honestly claim one row"
    );
}

/// `u IN (1, 2)` can match two rows on a UNIQUE column: the winner must not
/// claim it reads at most one.
#[test]
fn a_multi_value_in_list_on_a_unique_column_must_not_claim_one_row() {
    let db = seed();
    let explained = db
        .explain_output("SELECT id FROM unique_probe WHERE u IN (1, 2) ORDER BY seq ASC")
        .expect("explain must succeed");
    let reason = composite_rejected_reason(&explained);
    assert_ne!(
        reason, ONE_ROW_REASON,
        "u IN (1, 2) can match two rows even though u is UNIQUE; EXPLAIN must not claim one row: {reason}"
    );
}

/// A range matching more than one row on a UNIQUE column: same requirement.
#[test]
fn a_range_matching_more_than_one_row_on_a_unique_column_must_not_claim_one_row() {
    let db = seed();
    let explained = db
        .explain_output("SELECT id FROM unique_probe WHERE u > 0 AND u < 10 ORDER BY seq ASC")
        .expect("explain must succeed");
    let reason = composite_rejected_reason(&explained);
    assert_ne!(
        reason, ONE_ROW_REASON,
        "a range over a UNIQUE column can match more than one row; EXPLAIN must not claim one row: {reason}"
    );
}
