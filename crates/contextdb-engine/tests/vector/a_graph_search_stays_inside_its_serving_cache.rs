//! A search that scores from serving pages is bounded by the cache's size.
//!
//! Only the graph route reads vector bodies out of serving pages; an
//! exhaustive search reads each row's own stored body and is independent of
//! the maintained generation. So this is about the graph route, and the
//! promise is that a partition whose pages do not all fit is answered by
//! cycling them through the cache rather than by growing past what the session
//! declared.
//!
//! The assertion has to be one that fails if the bound stops working, which
//! means comparing against the pages the same search touches when it has room
//! for all of them: a constrained cache must read strictly MORE pages than
//! there are distinct pages, because it re-reads what it evicted. Counting
//! fills alone proves nothing -- a cache that never evicts still fills every
//! page exactly once.

#[path = "passage_fixture.rs"]
mod passage_fixture;

use passage_fixture::{answer_ids, find_sql, open_file_reader, params};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const LIMIT: usize = 10;
/// Enough for the reader to open and bring the generation resident, and small
/// enough that the share it allows the serving cache cannot hold the
/// partition's ~3.8 MB of vector pages -- so the cache must evict and re-read.
const CONSTRAINED_LIMIT: usize = 16 * 1024 * 1024;

fn rows() -> usize {
    std::env::var("CONTEXTDB_P1_EXACT_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_250)
}

#[derive(Default)]
struct PageCounts {
    filled: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl contextdb_vector::hnsw::PagedVectorObserver for PageCounts {
    fn page_filled(&self) {
        self.filled.fetch_add(1, Ordering::Relaxed);
    }
    fn page_looked_up(&self, hit: bool) {
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// A constrained serving cache re-reads pages it evicted, and publishes the
/// same rows as a cache with room for the whole partition.
#[cfg(unix)]
#[test]
#[ignore = "seeds a 1,250 x 768 store, which is minutes and hundreds of megabytes -- too slow \
            for the default suite. The property it proves RUNS there as \
            contextdb-vector's a_serving_cache_smaller_than_its_vectors_cycles_pages_through_itself, \
            which drives decode_paged_durable_generation's cache_bytes directly and needs no \
            fixture. This one stays as the end-to-end diagnostic through a real ReadSession."]
fn a_graph_search_over_a_partition_larger_than_its_cache_answers_the_same_rows() {
    let (path, runtime) = passage_fixture::seeded(rows());
    let sql = find_sql("USE VECTOR INDEXED", LIMIT);

    // With room for every page, a cold search reads each distinct page once,
    // so its miss count IS the number of distinct pages the search touches.
    let spacious = open_file_reader(&path, &runtime, None);
    let roomy = Arc::new(PageCounts::default());
    let reference = contextdb_vector::hnsw::with_paged_vector_observer(roomy.clone(), || {
        spacious
            .execute(&sql, &params(0))
            .expect("the graph route answers")
    });
    let reference_ids = answer_ids(&reference);
    let distinct_pages = roomy.misses.load(Ordering::Relaxed);
    assert_eq!(
        reference_ids.len(),
        LIMIT,
        "the reference answers a full page"
    );
    assert!(
        distinct_pages > 0,
        "a cold graph search reads its pages from the source at least once"
    );
    drop(spacious);

    let constrained = open_file_reader(&path, &runtime, Some(CONSTRAINED_LIMIT));
    let tight = Arc::new(PageCounts::default());
    let answer = contextdb_vector::hnsw::with_paged_vector_observer(tight.clone(), || {
        constrained
            .execute(&sql, &params(0))
            .expect("a smaller serving cache does not stop the graph route answering")
    });
    let reread = tight.misses.load(Ordering::Relaxed);
    println!(
        "graph_cache rows={} distinct_pages={distinct_pages} constrained_misses={reread} \
         constrained_hits={} constrained_fills={}",
        rows(),
        tight.hits.load(Ordering::Relaxed),
        tight.filled.load(Ordering::Relaxed),
    );
    assert_eq!(
        answer_ids(&answer),
        reference_ids,
        "the rows a graph search publishes do not depend on how much of the partition its \
         serving cache can hold at once"
    );
    assert!(
        reread > distinct_pages,
        "a cache too small for the partition must re-read pages it evicted: {reread} source \
         reads against {distinct_pages} distinct pages means nothing was evicted, so the \
         declared bound is not what sized this cache"
    );
}
