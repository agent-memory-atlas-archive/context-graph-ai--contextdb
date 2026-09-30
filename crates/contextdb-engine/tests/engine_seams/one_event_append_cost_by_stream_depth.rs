//! What one event appended to a long stream costs, as the stream gets longer.
//!
//! The product statement this grades: appending one event to a stream that
//! already holds twenty thousand events should cost what appending one event
//! costs, not what the store's size costs. The consumer's own benchmark
//! measured the opposite — p50 rising ≈9.9 → ≈15.2 → ≈22.6 → ≈31.5 ms at
//! 10 / 1,010 / 5,010 / 20,010 staged events, monotonically, with a flat 15.0
//! engine statements per append at every depth. So the growth is not in the statements
//! and this test goes looking for it in the durable write.
//!
//! Every quantity here is a deterministic count taken from the engine's own
//! seams, at the same four depths, on the pinned cg startup schema with the
//! consumer's three-row one-event append. The spans are reported beside the
//! counts and never asserted on: a count grades a repair, a span says where to
//! look.
//!
//! This test builds four stores of up to twenty thousand events through a
//! debug-profile engine, so it is slow by construction. It is a work-count
//! instrument, never a latency measurement. The consumer benchmark measures
//! latency on its recorded machine.

use contextdb_engine::Database;
use contextdb_engine::append_work::{
    self, AppendStageNanos, CommitSpansForTest, FamilyWork, IdentitySpans,
};

use crate::one_event_append_work_by_family::{append_event, declare};

/// The four depths the consumer benchmark reports, so a number here can be put
/// beside a number there. Building four stores of up to twenty thousand events
/// through a debug-profile engine takes tens of minutes, so the tests that use
/// them are `#[ignore]`d and run on demand.
pub(crate) const DEPTHS: [usize; 4] = [10, 1_010, 5_010, 20_010];

/// Three shallow depths that show the same growth in seconds rather than in
/// tens of minutes: the tree is already a level taller at the third than at
/// the first, which is the whole of the law the deep curve measures.
const SHALLOW_DEPTHS: [usize; 3] = [10, 110, 510];

/// Why the deep tests do not run in the ordinary suite, and how to run them.
const ON_DEMAND: &str = "builds stores of up to 20,010 events; run with --ignored";

/// How many appends are observed at each depth. The commit's own freed-page
/// list follows tree shape as well as write count and moved by ±2 between
/// identical measurements, so a depth is read from several
/// appends and reported at its median rather than from one.
const SAMPLES: usize = 5;

#[derive(Debug, Clone, Default)]
pub(crate) struct AppendCost {
    pub(crate) mutations: u64,
    pub(crate) node_writes: u64,
    pub(crate) node_reads: u64,
    pub(crate) cache_hits: u64,
    pub(crate) bytes_written: u64,
    pub(crate) bytes_read: u64,
    pub(crate) freed_pages: u64,
    pub(crate) allocated_pages: u64,
    /// The freed pages the identity families' walks did not copy: the rows,
    /// index postings, logs and system tables of the same commit.
    pub(crate) freed_pages_outside_the_proof: u64,
    /// The pages each family's own walk copied, beside its other counts.
    pub(crate) page_copies_by_family: Vec<(String, u64)>,
    /// Pages the commit's own system-table bookkeeping copied. They land in the
    /// next commit's list, so they are reported beside the total, not inside.
    pub(crate) page_copies_during_the_commit: u64,
    /// Chunks rewritten more than once in one commit, and writes that store a
    /// wide separator's key bytes rather than a chunk.
    pub(crate) repeated_writes: u64,
    pub(crate) key_blob_writes: u64,
    /// Where the content-identity stage's own time went.
    pub(crate) identity: IdentitySpans,
    /// Root-to-leaf chunks in the tallest family tree the walk entered.
    pub(crate) deepest_family_height: u64,
    pub(crate) families: Vec<(String, FamilyWork)>,
    pub(crate) stages: AppendStageNanos,
    pub(crate) commit: CommitSpansForTest,
}

fn observe(db: &Database, event: usize) -> AppendCost {
    append_work::reset();
    append_event(db, event);
    let pages = append_work::take_commit_pages_for_test();
    let stages = append_work::take_append_stages();
    let identity = append_work::take_identity_spans();
    let commit = append_work::take_commit_spans();
    let copies = append_work::take_page_copies();
    assert_eq!(
        copies.total(),
        pages.freed_pages,
        "every page the commit listed as freed is a page the storage engine copied under some \
         tag: {copies:?}"
    );
    let families = append_work::take_family_work();
    let total = |of: fn(&FamilyWork) -> u64| families.iter().map(|(_, _, work)| of(work)).sum();
    AppendCost {
        mutations: total(|work| work.mutations),
        node_writes: total(|work| work.node_writes),
        node_reads: total(|work| work.node_reads),
        cache_hits: total(|work| work.cache_hits),
        repeated_writes: total(|work| work.repeated_writes),
        key_blob_writes: total(|work| work.key_blob_writes),
        identity,
        bytes_written: total(|work| work.bytes_written),
        bytes_read: total(|work| work.bytes_read),
        freed_pages: pages.freed_pages,
        allocated_pages: pages.allocated_pages,
        freed_pages_outside_the_proof: copies.outside_every_family,
        page_copies_during_the_commit: copies.during_the_commit,
        page_copies_by_family: copies
            .by_family
            .iter()
            .map(|(_, name, pages)| ((*name).to_owned(), *pages))
            .collect(),
        deepest_family_height: families
            .iter()
            .map(|(_, _, work)| work.tree_height)
            .max()
            .unwrap_or_default(),
        families: families
            .into_iter()
            .map(|(_, name, work)| (name.to_owned(), work))
            .collect(),
        stages,
        commit,
    }
}

fn median(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

/// The median of each quantity over the sampled appends. Medians are taken per
/// quantity because the counts are independent observations of the same append
/// shape, not a joint one.
fn median_cost(samples: &[AppendCost]) -> AppendCost {
    let pick = |of: fn(&AppendCost) -> u64| median(samples.iter().map(of).collect());
    let last = samples.last().expect("at least one sample");
    let mut families = Vec::new();
    for (name, _) in &last.families {
        let of_family = |work: fn(&FamilyWork) -> u64| {
            median(
                samples
                    .iter()
                    .filter_map(|sample| {
                        sample
                            .families
                            .iter()
                            .find(|(family, _)| family == name)
                            .map(|(_, counts)| work(counts))
                    })
                    .collect(),
            )
        };
        families.push((
            name.clone(),
            FamilyWork {
                mutations: of_family(|work| work.mutations),
                node_reads: of_family(|work| work.node_reads),
                node_writes: of_family(|work| work.node_writes),
                bytes_read: of_family(|work| work.bytes_read),
                bytes_written: of_family(|work| work.bytes_written),
                cache_hits: of_family(|work| work.cache_hits),
                tree_height: of_family(|work| work.tree_height),
                repeated_writes: of_family(|work| work.repeated_writes),
                key_blob_writes: of_family(|work| work.key_blob_writes),
            },
        ));
    }
    AppendCost {
        mutations: pick(|cost| cost.mutations),
        node_writes: pick(|cost| cost.node_writes),
        node_reads: pick(|cost| cost.node_reads),
        cache_hits: pick(|cost| cost.cache_hits),
        bytes_written: pick(|cost| cost.bytes_written),
        bytes_read: pick(|cost| cost.bytes_read),
        freed_pages: pick(|cost| cost.freed_pages),
        allocated_pages: pick(|cost| cost.allocated_pages),
        freed_pages_outside_the_proof: pick(|cost| cost.freed_pages_outside_the_proof),
        page_copies_during_the_commit: pick(|cost| cost.page_copies_during_the_commit),
        repeated_writes: pick(|cost| cost.repeated_writes),
        key_blob_writes: pick(|cost| cost.key_blob_writes),
        identity: IdentitySpans {
            read_and_decode_nanos: pick(|cost| cost.identity.read_and_decode_nanos),
            build_and_hash_nanos: pick(|cost| cost.identity.build_and_hash_nanos),
            encode_and_store_nanos: pick(|cost| cost.identity.encode_and_store_nanos),
            coalesce_and_group_nanos: pick(|cost| cost.identity.coalesce_and_group_nanos),
        },
        page_copies_by_family: last
            .page_copies_by_family
            .iter()
            .map(|(name, _)| {
                (
                    name.clone(),
                    median(
                        samples
                            .iter()
                            .filter_map(|sample| {
                                sample
                                    .page_copies_by_family
                                    .iter()
                                    .find(|(family, _)| family == name)
                                    .map(|(_, pages)| *pages)
                            })
                            .collect(),
                    ),
                )
            })
            .collect(),
        deepest_family_height: pick(|cost| cost.deepest_family_height),
        families,
        stages: AppendStageNanos {
            write_set_preparation: pick(|cost| cost.stages.write_set_preparation),
            table_meta_clone: pick(|cost| cost.stages.table_meta_clone),
            rows_indexes_and_logs: pick(|cost| cost.stages.rows_indexes_and_logs),
            content_identity: pick(|cost| cost.stages.content_identity),
            commit: pick(|cost| cost.stages.commit),
        },
        commit: CommitSpansForTest {
            commits: pick(|cost| cost.commit.commits),
            flush_and_close_nanos: pick(|cost| cost.commit.flush_and_close_nanos),
            store_page_lists_nanos: pick(|cost| cost.commit.store_page_lists_nanos),
            process_freed_pages_nanos: pick(|cost| cost.commit.process_freed_pages_nanos),
            system_tree_nanos: pick(|cost| cost.commit.system_tree_nanos),
            storage_commit_nanos: pick(|cost| cost.commit.storage_commit_nanos),
        },
    }
}

/// Stage one store per depth rather than one store sampled as it grows: a
/// sample taken from a store that has just been sampled carries the previous
/// sample's copy-on-write predecessors, and the depths are far enough apart
/// that sharing a store would make the shallow rows describe a store that is
/// about to be deep.
pub(crate) fn curve(depths: &[usize]) -> Vec<(usize, AppendCost)> {
    curve_with(depths, |_| {})
}

/// The same curve with one extra declaration step after the startup schema, so
/// the same instrument can measure what an append costs under a
/// `MAINTAIN COUNT` declaration the pinned startup schema does not carry.
pub(crate) fn curve_with(
    depths: &[usize],
    declare_counts: impl Fn(&Database),
) -> Vec<(usize, AppendCost)> {
    let mut curve = Vec::new();
    for depth in depths {
        let root = tempfile::tempdir().expect("task-scoped fixture root");
        let db = Database::open(root.path().join(format!("append-curve-{depth}.db")))
            .expect("open the writable fixture");
        declare(&db);
        declare_counts(&db);
        for event in 0..*depth {
            append_event(&db, event);
        }
        let samples = (0..SAMPLES)
            .map(|sample| observe(&db, depth + sample))
            .collect::<Vec<_>>();
        curve.push((*depth, median_cost(&samples)));
        drop(db);
        root.close().expect("remove the fixture");
    }
    curve
}

fn micros(nanos: u64) -> String {
    format!("{:.1}", nanos as f64 / 1_000.0)
}

fn growth(shallow: u64, deep: u64) -> f64 {
    if shallow == 0 {
        return f64::INFINITY;
    }
    deep as f64 / shallow as f64
}

pub(crate) fn report(curve: &[(usize, AppendCost)]) -> String {
    let mut lines = vec![
        "one-event append, by how many events the stream already holds".to_owned(),
        format!(
            "{:>8} {:>6} {:>7} {:>7} {:>7} {:>9} {:>7} {:>8} {:>7} {:>7} {:>8} {:>7} {:>7}",
            "staged",
            "muts",
            "nwrites",
            "nreads",
            "cachhit",
            "byteswr",
            "height",
            "freedpg",
            "proofpg",
            "rowspg",
            "commitpg",
            "repeat",
            "keyblob",
        ),
    ];
    for (depth, cost) in curve {
        lines.push(format!(
            "{depth:>8} {:>6} {:>7} {:>7} {:>7} {:>9} {:>7} {:>8} {:>7} {:>7} {:>8} {:>7} {:>7}",
            cost.mutations,
            cost.node_writes,
            cost.node_reads,
            cost.cache_hits,
            cost.bytes_written,
            cost.deepest_family_height,
            cost.freed_pages,
            cost.freed_pages - cost.freed_pages_outside_the_proof,
            cost.freed_pages_outside_the_proof,
            cost.page_copies_during_the_commit,
            cost.repeated_writes,
            cost.key_blob_writes,
        ));
    }
    lines.push(String::new());
    lines.push(
        "stage spans, microseconds (diagnostic — a debug-profile engine test is not a latency \
         measurement)"
            .to_owned(),
    );
    lines.push(format!(
        "{:>8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "staged",
        "prep",
        "meta",
        "rows+log",
        "identity",
        "commit",
        "flush&cl",
        "pagelist",
        "reclaim",
        "fsync",
    ));
    for (depth, cost) in curve {
        lines.push(format!(
            "{depth:>8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            micros(cost.stages.write_set_preparation),
            micros(cost.stages.table_meta_clone),
            micros(cost.stages.rows_indexes_and_logs),
            micros(cost.stages.content_identity),
            micros(cost.stages.commit),
            micros(cost.commit.flush_and_close_nanos),
            micros(cost.commit.store_page_lists_nanos),
            micros(cost.commit.process_freed_pages_nanos),
            micros(cost.commit.storage_commit_nanos),
        ));
    }
    lines.push(String::new());
    lines.push(
        "inside the content-identity stage, microseconds, and per chunk read or rewritten"
            .to_owned(),
    );
    lines.push(format!(
        "{:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "staged",
        "read+dec",
        "build+hash",
        "enc+store",
        "coal+grp",
        "walk rest",
        "us/read",
        "us/rewrite",
    ));
    for (depth, cost) in curve {
        lines.push(format!(
            "{depth:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10.2} {:>10.2}",
            micros(cost.identity.read_and_decode_nanos),
            micros(cost.identity.build_and_hash_nanos),
            micros(cost.identity.encode_and_store_nanos),
            micros(cost.identity.coalesce_and_group_nanos),
            micros(cost.stages.content_identity.saturating_sub(
                cost.identity.read_and_decode_nanos
                    + cost.identity.build_and_hash_nanos
                    + cost.identity.encode_and_store_nanos
                    + cost.identity.coalesce_and_group_nanos,
            )),
            cost.identity.read_and_decode_nanos as f64 / 1_000.0 / cost.node_reads.max(1) as f64,
            cost.identity.encode_and_store_nanos as f64 / 1_000.0 / cost.node_writes.max(1) as f64,
        ));
    }
    lines.push(String::new());
    lines.push("per family, at each depth".to_owned());
    for (depth, cost) in curve {
        for (name, work) in &cost.families {
            lines.push(format!(
                "{depth:>8} {name:<18} muts={:<4} nwrites={:<5} nreads={:<5} hits={:<5} \
                 height={:<3} freedpg={:<6} byteswr={}",
                work.mutations,
                work.node_writes,
                work.node_reads,
                work.cache_hits,
                work.tree_height,
                cost.page_copies_by_family
                    .iter()
                    .find(|(family, _)| family == name)
                    .map(|(_, pages)| *pages)
                    .unwrap_or_default(),
                work.bytes_written,
            ));
        }
    }
    let (shallow_depth, shallow) = curve.first().expect("a shallow row");
    let (deep_depth, deep) = curve.last().expect("a deep row");
    lines.push(String::new());
    lines.push(format!(
        "growth from {shallow_depth} to {deep_depth} staged events, largest first"
    ));
    let mut factors = vec![
        (
            "freed pages per append",
            growth(shallow.freed_pages, deep.freed_pages),
        ),
        (
            "pages the proof's own walks copied",
            growth(
                shallow.freed_pages - shallow.freed_pages_outside_the_proof,
                deep.freed_pages - deep.freed_pages_outside_the_proof,
            ),
        ),
        (
            "pages the rows, postings and logs copied",
            growth(
                shallow.freed_pages_outside_the_proof,
                deep.freed_pages_outside_the_proof,
            ),
        ),
        (
            "node writes per append",
            growth(shallow.node_writes, deep.node_writes),
        ),
        (
            "node reads per append",
            growth(shallow.node_reads, deep.node_reads),
        ),
        (
            "proof-cache hits per append",
            growth(shallow.cache_hits, deep.cache_hits),
        ),
        (
            "identity bytes written",
            growth(shallow.bytes_written, deep.bytes_written),
        ),
        (
            "tallest family tree",
            growth(shallow.deepest_family_height, deep.deepest_family_height),
        ),
        (
            "coalesced mutations per append",
            growth(shallow.mutations, deep.mutations),
        ),
        (
            "pages freed per node write",
            growth(
                shallow.freed_pages * deep.node_writes,
                deep.freed_pages * shallow.node_writes,
            ),
        ),
    ];
    factors.sort_by(|left, right| right.1.total_cmp(&left.1));
    for (name, factor) in factors {
        lines.push(format!("  {factor:>7.2}x  {name}"));
    }
    lines.join("\n")
}

/// Twenty-six thousand appends through a debug-profile engine is minutes of
/// work, and both tests below read the same four depths, so the curve is built
/// once for the whole binary.
fn shared_curve() -> &'static [(usize, AppendCost)] {
    static CURVE: std::sync::OnceLock<Vec<(usize, AppendCost)>> = std::sync::OnceLock::new();
    CURVE.get_or_init(|| {
        let curve = curve(&DEPTHS);
        eprintln!("{}", report(&curve));
        curve
    })
}

/// The growth law, at a depth that runs in seconds: one append's coalesced
/// mutations do not depend on how long the stream is, but the authenticated
/// chunks they rewrite and the pages the commit copies both do, because the
/// tree gets taller.
///
/// This is the registered, always-run form. The deep curve below measures the
/// same quantities at the depths the consumer benchmark reports.
#[test]
fn one_event_append_cost_grows_with_the_stream() {
    let curve = curve(&SHALLOW_DEPTHS);
    eprintln!("{}", report(&curve));
    assert_growth(&curve);
    let (shallow_depth, shallow) = curve.first().expect("the shallow row");
    let (deep_depth, deep) = curve.last().expect("the deep row");
    assert!(
        deep.deepest_family_height > shallow.deepest_family_height,
        "the tallest family tree is taller at {deep_depth} staged events than at \
         {shallow_depth}: {} against {}",
        deep.deepest_family_height,
        shallow.deepest_family_height,
    );
}

/// The curve at the depths the consumer benchmark reports.
///
/// Nothing here is a latency claim. The assertions are on counts and on the
/// SHAPE of their growth, which a debug build reproduces exactly.
#[test]
#[ignore = "builds stores of up to 20,010 events; run with --ignored"]
fn one_event_append_cost_by_stream_depth() {
    let curve = shared_curve();
    assert_growth(curve);
    let _ = ON_DEMAND;
}

/// What must hold at every depth, whichever depths were measured.
fn assert_growth(curve: &[(usize, AppendCost)]) {
    let shallow = &curve.first().expect("the shallow row").1;
    let deep = &curve.last().expect("the deep row").1;
    let shallowest = curve.first().expect("the shallow row").0;
    let deepest = curve.last().expect("the deep row").0;

    assert_eq!(
        shallow.mutations, deep.mutations,
        "the coalesced mutation vector of one append does not depend on how many events the \
         stream already holds: {} at {shallowest} events, {} at {deepest}",
        shallow.mutations, deep.mutations,
    );
    assert_eq!(
        deep.allocated_pages, 0,
        "an ordinary append holds no savepoint, so every commit on this curve writes no \
         allocated-page list"
    );
    assert!(
        deep.node_writes > shallow.node_writes,
        "node writes per append grow with the stream: {} at {shallowest} events, {} at {deepest}",
        shallow.node_writes,
        deep.node_writes,
    );
    assert!(
        deep.freed_pages > shallow.freed_pages,
        "the commit's own copy-on-write page list grows with the stream: {} at {shallowest} \
         events, {} at {deepest}",
        shallow.freed_pages,
        deep.freed_pages,
    );
    // One commit runs exactly one tree pass per family, and that pass keeps its
    // rewrites in a map keyed by storage key, so a chunk on a path several
    // edits share is rewritten once for the commit rather than once per edit.
    // A non-zero count here would mean that had stopped being true.
    for (depth, cost) in curve {
        assert_eq!(
            cost.repeated_writes, 0,
            "at {depth} staged events an append rewrote the same chunk twice in one commit, so a \
             shared upper-level path is being paid for once per edit: {} repeats of {} rewrites",
            cost.repeated_writes, cost.node_writes,
        );
    }
    for window in curve.windows(2) {
        let (shallower, shallower_cost) = &window[0];
        let (deeper, deeper_cost) = &window[1];
        assert!(
            deeper_cost.freed_pages >= shallower_cost.freed_pages,
            "the freed-page list never falls as the stream lengthens: {} at {shallower} events, \
             {} at {deeper}",
            shallower_cost.freed_pages,
            deeper_cost.freed_pages,
        );
    }
}

/// What one event appended to a long stream is allowed to cost.
///
/// The promise: appending one event to a stream that already holds twenty
/// thousand events costs what appending one event costs. Not the same number to
/// the digit — an authenticated tree genuinely deepens as it fills, and a
/// root-to-leaf path through it is genuinely longer — but close enough that a
/// user who has been capturing for a month is not paying for the month.
///
/// The factor below reflects the one-event append target of at most 25 ms
/// warm p95 at 20,010
/// staged events. The measured tail-to-median ratio of that cell is 1.2316, so
/// the median may reach 20.3 ms. Of a shallow-stream append, the part that does
/// NOT grow with the stream — the statements, the rows and index postings and
/// logs, and the write-set preparation — is about 7.2 ms, and the part that
/// does is about 4.1 ms. So the growing part may reach 13.1 ms, which is 3.2
/// times what it costs on a short stream.
///
/// Stating it as a factor is what makes it grade the SHAPE rather than a
/// machine: a term that grows with the logarithm of the stream stays inside it,
/// a term that grows with the stream itself does not.
const AT_TWENTY_THOUSAND_EVENTS_AN_APPEND_MAY_COST: f64 = 3.2;

#[test]
#[ignore = "the fix this test pins is not implemented yet, and it builds stores \
            of up to 20,010 events; run with --ignored"]
fn one_event_appended_to_a_long_stream_costs_what_one_appended_to_a_short_stream_costs() {
    let curve = shared_curve();
    let (shallow_depth, shallow) = curve.first().expect("the shallow row");
    let (deep_depth, deep) = curve.last().expect("the deep row");

    let mut over = Vec::new();
    for (what, at_ten, at_twenty_thousand) in [
        (
            "pages the commit had to copy",
            shallow.freed_pages,
            deep.freed_pages,
        ),
        (
            "pages the proof's own walks copied",
            shallow.freed_pages - shallow.freed_pages_outside_the_proof,
            deep.freed_pages - deep.freed_pages_outside_the_proof,
        ),
        (
            "chunks the proof rewrote",
            shallow.node_writes,
            deep.node_writes,
        ),
        ("chunks the proof read", shallow.node_reads, deep.node_reads),
        (
            "bytes the proof rewrote",
            shallow.bytes_written,
            deep.bytes_written,
        ),
    ] {
        let factor = growth(at_ten, at_twenty_thousand);
        if factor > AT_TWENTY_THOUSAND_EVENTS_AN_APPEND_MAY_COST {
            over.push(format!(
                "  {what}: {at_ten} at {shallow_depth} events, {at_twenty_thousand} at \
                 {deep_depth} — {factor:.2}x"
            ));
        }
    }
    assert!(
        over.is_empty(),
        "one event appended to a stream of {deep_depth} costs more than \
         {AT_TWENTY_THOUSAND_EVENTS_AN_APPEND_MAY_COST:.1}x what one appended to a stream of \
         {shallow_depth} costs, so an append is being charged for the size of the store rather \
         than for its own content:\n{}",
        over.join("\n"),
    );
}
