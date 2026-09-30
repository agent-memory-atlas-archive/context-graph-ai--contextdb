//! Who is actually holding the budget when a 1 KB store refuses its first
//! insert.
//!
//! `oom_does_not_partially_commit_visible_state` declares `MEMORY_LIMIT '1K'`
//! and requires one minimal `INSERT` to succeed. It does not, and the reason
//! has moved twice. It was read as "proof maintenance costs too much", because
//! that is what the refusal named. A budget ledger then said proof maintenance
//! never held more than 458 bytes and the retained table metadata was holding
//! 464 of the kilobyte.
//!
//! Since the charge model was corrected, the statement does not get that far.
//! A table with a single-column PRIMARY KEY allocates a fixed 64 KiB
//! exact-probe bitmap and holds it for the life of the index. It always did;
//! the accountant simply could not see it, which is why a 1,024-byte limit
//! used to admit a store that went on to hold 64 KiB. Now the `CREATE TABLE`
//! is refused on that figure alone, before a single proof byte is charged.
//!
//! So this pins the live finding: **one table with a primary key costs a
//! declared limit 64 KiB, and no limit below that can declare one.** Whether a
//! fixed 64 KiB negative-probe bitmap in front of an identity-hashed map is
//! the right size is a read-path question; this test only holds the engine to
//! saying so out loud.
use contextdb_engine::Database;
use contextdb_engine::memory_accounting::MemoryAccountant;
use contextdb_engine::memory_accounting::{
    BudgetLedgerEntry, take_budget_ledger_for_test, watch_budget_ledger_for_test,
};
use std::collections::HashMap;
use std::sync::Arc;

/// What is outstanding at the point the ledger reached its highest total, by
/// the name that charged it.
///
/// Releases carry no name, so each one is attributed to the most recent charge
/// still outstanding — last in, first out, which is how a transaction's nested
/// admissions actually unwind.
fn outstanding_at_the_peak(ledger: &[BudgetLedgerEntry]) -> (usize, Vec<(String, usize)>) {
    let peak = ledger
        .iter()
        .enumerate()
        .max_by_key(|(_, entry)| entry.used_after)
        .map(|(index, entry)| (index, entry.used_after))
        .unwrap_or((0, 0));
    let mut outstanding: Vec<(String, usize)> = Vec::new();
    if ledger.is_empty() {
        return (0, Vec::new());
    }
    for entry in &ledger[..=peak.0.min(ledger.len() - 1)] {
        if entry.what == "release" {
            let mut left = entry.bytes;
            while left > 0 {
                let Some((_, held)) = outstanding.last_mut() else {
                    break;
                };
                let taken = (*held).min(left);
                *held -= taken;
                left -= taken;
                if *held == 0 {
                    outstanding.pop();
                }
            }
        } else {
            outstanding.push((entry.what.clone(), entry.bytes));
        }
    }
    let mut totals: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for (what, bytes) in outstanding {
        *totals.entry(what).or_default() += bytes;
    }
    let mut totals = totals.into_iter().collect::<Vec<_>>();
    totals.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    (peak.1, totals)
}

fn report(label: &str, ledger: &[BudgetLedgerEntry]) -> (usize, Vec<(String, usize)>) {
    eprintln!("{label}: {} ledger entries", ledger.len());
    for entry in ledger {
        eprintln!(
            "  {:<44} {:>6} bytes -> used {}",
            entry.what, entry.bytes, entry.used_after
        );
    }
    let (peak, held) = outstanding_at_the_peak(ledger);
    eprintln!("{label}: at the peak of {peak} bytes, outstanding by charge name:");
    for (what, bytes) in &held {
        eprintln!("  {what:<44} {bytes:>6} bytes");
    }
    (peak, held)
}

/// The exact sequence of `oom_does_not_partially_commit_visible_state`, with
/// the ledger watching the statement that refuses.
#[test]
fn a_one_kilobyte_store_says_what_is_holding_its_budget() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = root.path().join("one-kilobyte.db");

    let db = Database::open(&path).expect("open the writable fixture");
    db.execute("SET MEMORY_LIMIT '1K'", &HashMap::new())
        .expect("declare the limit");

    // The statement that is refused is the DECLARATION, not the insert that
    // follows it: `oom_does_not_partially_commit_visible_state` never reaches
    // its `INSERT` (that test's `.unwrap()` at
    // `cross_cutting_acceptance_tests.rs:37`).
    watch_budget_ledger_for_test();
    let outcome = db.execute(
        "CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    );
    let ledger = take_budget_ledger_for_test();
    report("one minimal CREATE TABLE under MEMORY_LIMIT '1K'", &ledger);

    let error = outcome.expect_err(
        "this fixture is the one that is refused; if it now succeeds, the finding below has been \
         fixed and this test should be rewritten to pin that instead",
    );
    let contextdb_core::Error::MemoryBudgetExceeded {
        subsystem,
        operation,
        requested_bytes,
        budget_limit_bytes,
        ..
    } = &error
    else {
        panic!("the refusal must be the typed memory refusal, not {error:?}");
    };

    // The finding, in one line: the whole of the kilobyte goes nowhere near
    // proof maintenance. The table's own declaration asks for 64 KiB more than
    // the limit, because that is what its primary key's exact-probe bitmap
    // holds for as long as the index exists.
    assert_eq!(
        (subsystem.as_str(), operation.as_str()),
        ("ddl", "create_table"),
        "the refusal must be the table's own declaration, not anything inside it: {error:?}"
    );
    assert_eq!(*budget_limit_bytes, 1024);
    assert!(
        *requested_bytes > contextdb_core::EXACT_PROBE_FILTER_BYTES,
        "the declaration asks for {requested_bytes}, which must include the \
         {} B exact-probe bitmap the primary key allocates",
        contextdb_core::EXACT_PROBE_FILTER_BYTES,
    );
    assert!(
        requested_bytes - contextdb_core::EXACT_PROBE_FILTER_BYTES < 1024,
        "and everything else about this two-column table fits inside the declared kilobyte: {} B",
        requested_bytes - contextdb_core::EXACT_PROBE_FILTER_BYTES,
    );
    assert!(
        ledger
            .iter()
            .all(|entry| !entry.what.starts_with("content_identity/")),
        "proof maintenance never runs: the statement is refused before it: {ledger:?}"
    );
}

/// The other refused acceptance test, `m12_open_with_config_wires_accountant`,
/// and it refuses for the same reason one byte earlier.
///
/// That test sizes its budget as "enough for the schema and less than one row",
/// measuring the schema's cost as what the accountant still holds after a
/// `CREATE TABLE` on an unlimited budget. What that measures is the RETAINED
/// charge — the table metadata the store keeps. It does not include the
/// transient peak the same statement reaches while it is running, because that
/// peak is gone by the time the measurement is taken. So the budget is set to
/// the high-water mark of the statement's aftermath rather than of the
/// statement, and the statement cannot fit in it.
#[test]
fn the_tight_open_with_config_budget_omits_the_statement_that_sets_it() {
    const SCHEMA: &str = "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)";
    let root = tempfile::tempdir().expect("task-scoped fixture root");

    // What the test's own measurement sees: the charge still outstanding after
    // a CREATE TABLE completes, on a budget nothing comes near.
    let measuring = Arc::new(MemoryAccountant::with_budget(64 * 1024 * 1024));
    let measured = Database::open_with_config(
        root.path().join("m12-measurement.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        measuring.clone(),
    )
    .expect("open the measuring fixture");
    watch_budget_ledger_for_test();
    measured
        .execute(SCHEMA, &HashMap::new())
        .expect("a CREATE TABLE on a budget nothing comes near");
    let ledger = take_budget_ledger_for_test();
    let (peak, held) = report("CREATE TABLE on an unlimited budget", &ledger);
    let retained = measuring.usage().used;
    measured.close().expect("close the measuring fixture");

    let transient_only = held
        .iter()
        .filter(|(what, _)| what.starts_with("content_identity/"))
        .map(|(_, bytes)| bytes)
        .sum::<usize>();
    eprintln!(
        "CREATE TABLE: peak {peak}, retained afterwards {retained}, transient proof at the peak \
         {transient_only}"
    );

    // The gap between what the statement needs and what it leaves behind is the
    // whole of the finding: a budget set from `retained` is short by the
    // transient peak, whatever the table is.
    assert!(
        peak > retained,
        "a CREATE TABLE's peak ({peak}) is above what it retains ({retained}); a budget measured \
         from the retained figure cannot admit the statement that produced it"
    );
    assert!(
        transient_only > 0,
        "the difference is proof maintenance, which is transient by design: {held:?}"
    );
}
