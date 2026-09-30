//! Which level of the proof tree one appended event rewrites, and what each
//! level costs the storage engine.
//!
//! A chunk's leaves are the great majority of a family's chunks; the levels
//! above them are few. One edit rewrites one chunk per level on its way to the
//! root, so the levels above the leaves are where several edits' paths
//! converge — and they are also where a shorter storage path would help most,
//! because there are so few of them. This test says how the rewrites and the
//! pages they cost actually divide, so a repair aimed at the upper levels can
//! be sized before it is built rather than after.
//!
//! Measured at 5,010 staged events, which already carries the same tree height
//! as 20,010 on this fixture, so the division is the one that holds at depth.

use contextdb_engine::Database;
use contextdb_engine::append_work;

use crate::one_event_append_work_by_family::{append_event, declare};

const STAGED: usize = 5_010;
const SAMPLES: usize = 3;

#[test]
#[ignore = "builds a 5,010-event store; run with --ignored"]
fn where_an_appends_chunk_rewrites_sit_in_the_proof_tree() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = Database::open(root.path().join("rewrites-by-level.db")).expect("open the fixture");
    declare(&db);
    for event in 0..STAGED {
        append_event(&db, event);
    }

    let mut totals: std::collections::BTreeMap<u8, (u64, u64)> = std::collections::BTreeMap::new();
    let mut rewrites = 0u64;
    let mut pages = 0u64;
    for sample in 0..SAMPLES {
        append_work::reset();
        append_event(&db, STAGED + sample);
        let commit = append_work::take_commit_pages_for_test();
        let copies = append_work::take_page_copies();
        let by_level = append_work::take_writes_by_level();
        let _ = append_work::take_family_work();
        rewrites += by_level.iter().map(|(_, writes, _)| writes).sum::<u64>();
        pages += copies.inside_a_family();
        assert_eq!(
            copies.total(),
            commit.freed_pages,
            "every page the commit listed as freed is a page the storage engine copied under \
             some tag"
        );
        for (level, writes, copied) in by_level {
            let entry = totals.entry(level).or_default();
            entry.0 += writes;
            entry.1 += copied;
        }
    }

    eprintln!("one event appended at {STAGED} staged events, mean of {SAMPLES} appends:");
    eprintln!(
        "{:>7} {:>10} {:>8} {:>14} {:>8}",
        "level", "rewrites", "share", "pages copied", "share"
    );
    for (level, (writes, copied)) in &totals {
        eprintln!(
            "{level:>7} {:>10.1} {:>7.0}% {:>14.1} {:>7.0}%",
            *writes as f64 / SAMPLES as f64,
            100.0 * *writes as f64 / rewrites as f64,
            *copied as f64 / SAMPLES as f64,
            100.0 * *copied as f64 / pages as f64,
        );
    }
    eprintln!(
        "  totals: {:.1} chunk rewrites, {:.1} pages copied inside a proof walk, per append",
        rewrites as f64 / SAMPLES as f64,
        pages as f64 / SAMPLES as f64,
    );

    let leaves = totals.get(&0).copied().unwrap_or_default();
    assert!(
        rewrites > 0 && pages > 0,
        "an append rewrote chunks and copied pages: {totals:?}"
    );
    assert!(
        totals.len() > 1,
        "an append at this depth rewrites chunks above the leaves: {totals:?}"
    );
    assert!(
        leaves.0 < rewrites,
        "not every rewrite is a leaf: {totals:?}"
    );
}
