//! Test-only accounting at the file reader's expensive decode and digest loops.
//!
//! This is deliberately scoped to one thread and one operation phase.  It is
//! not a process-wide census: a test that did not install an observer has no
//! value to read, and nested scopes restore their parent even while unwinding.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadWorkPhase {
    Open,
    Query,
    Commit,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadWorkCounters {
    /// Stored bytes handed to a row-version decode, before any projection.
    /// A read that decodes whole rows it will not return pays for their
    /// vector columns too, and this is where that shows up.
    pub stored_row_bytes: u64,
    pub decoded_rows: u64,
    /// Durable row reads by a version range or by its complete physical key.
    pub row_version_range_reads: u64,
    pub row_version_point_reads: u64,
    pub decoded_vectors: u64,
    pub vector_body_accesses: u64,
    /// Authenticated posting windows this read built, and the postings they
    /// walked. A find asks the graph for k rows; if these grow with the SIZE
    /// of a partition, or with the NUMBER of partitions, the read is paying
    /// for the scope instead of for the answer.
    pub verified_runs_built: u64,
    pub verified_postings_walked: u64,
    /// Reads (and physical index opens) those windows reconciled through. One
    /// statement's windows share one.
    pub verified_reads_opened: u64,
    /// Authenticated descents that proved a key prefix holds no posting.
    pub verified_prefix_probes: u64,
    pub hashed_rows: u64,
    pub hashed_vectors: u64,
    pub hashed_bytes: u64,
    pub authoritative_bodies_verified: u64,
    pub proof_nodes_read: u64,
    pub proof_nodes_hashed: u64,
    pub ordered_proof_nodes_read: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadWorkObservation {
    pub open: ReadWorkCounters,
    pub query: ReadWorkCounters,
    pub commit: ReadWorkCounters,
}

#[derive(Default)]
struct AtomicCounters {
    stored_row_bytes: AtomicU64,
    verified_runs_built: AtomicU64,
    verified_postings_walked: AtomicU64,
    verified_reads_opened: AtomicU64,
    verified_prefix_probes: AtomicU64,
    decoded_rows: AtomicU64,
    row_version_range_reads: AtomicU64,
    row_version_point_reads: AtomicU64,
    decoded_vectors: AtomicU64,
    vector_body_accesses: AtomicU64,
    hashed_rows: AtomicU64,
    hashed_vectors: AtomicU64,
    hashed_bytes: AtomicU64,
    authoritative_bodies_verified: AtomicU64,
    proof_nodes_read: AtomicU64,
    proof_nodes_hashed: AtomicU64,
    ordered_proof_nodes_read: AtomicU64,
}

impl AtomicCounters {
    fn snapshot(&self) -> ReadWorkCounters {
        ReadWorkCounters {
            stored_row_bytes: self.stored_row_bytes.load(Ordering::Relaxed),
            verified_runs_built: self.verified_runs_built.load(Ordering::Relaxed),
            verified_postings_walked: self.verified_postings_walked.load(Ordering::Relaxed),
            verified_reads_opened: self.verified_reads_opened.load(Ordering::Relaxed),
            verified_prefix_probes: self.verified_prefix_probes.load(Ordering::Relaxed),
            decoded_rows: self.decoded_rows.load(Ordering::Relaxed),
            row_version_range_reads: self.row_version_range_reads.load(Ordering::Relaxed),
            row_version_point_reads: self.row_version_point_reads.load(Ordering::Relaxed),
            decoded_vectors: self.decoded_vectors.load(Ordering::Relaxed),
            vector_body_accesses: self.vector_body_accesses.load(Ordering::Relaxed),
            hashed_rows: self.hashed_rows.load(Ordering::Relaxed),
            hashed_vectors: self.hashed_vectors.load(Ordering::Relaxed),
            hashed_bytes: self.hashed_bytes.load(Ordering::Relaxed),
            authoritative_bodies_verified: self
                .authoritative_bodies_verified
                .load(Ordering::Relaxed),
            proof_nodes_read: self.proof_nodes_read.load(Ordering::Relaxed),
            proof_nodes_hashed: self.proof_nodes_hashed.load(Ordering::Relaxed),
            ordered_proof_nodes_read: self.ordered_proof_nodes_read.load(Ordering::Relaxed),
        }
    }
}

struct ActiveObservation {
    counters: Arc<AtomicCounters>,
}

thread_local! {
    static ACTIVE: RefCell<Option<ActiveObservation>> = const { RefCell::new(None) };
}

/// An installed observation scope.  There is intentionally no global
/// "observed" accessor: unobserved work cannot be misreported as zero work.
pub struct ReadWorkObservationScope {
    phase: ReadWorkPhase,
    counters: Arc<AtomicCounters>,
    previous: Option<ActiveObservation>,
}

impl ReadWorkObservationScope {
    pub fn enter(phase: ReadWorkPhase) -> Self {
        let counters = Arc::new(AtomicCounters::default());
        let previous = ACTIVE.with(|active| {
            active.replace(Some(ActiveObservation {
                counters: Arc::clone(&counters),
            }))
        });
        Self {
            phase,
            counters,
            previous,
        }
    }

    pub fn phase(&self) -> ReadWorkPhase {
        self.phase
    }

    pub fn counters(&self) -> ReadWorkCounters {
        self.counters.snapshot()
    }
}

impl Drop for ReadWorkObservationScope {
    fn drop(&mut self) {
        let previous = self.previous.take();
        ACTIVE.with(|active| {
            let _departed = active.replace(previous);
        });
    }
}

fn with_active(note: impl FnOnce(&AtomicCounters)) {
    ACTIVE.with(|active| {
        if let Some(active) = active.borrow().as_ref() {
            note(&active.counters);
        }
    });
}

pub(crate) fn note_stored_row_bytes(bytes: usize) {
    with_active(|counters| {
        counters
            .stored_row_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    });
}

pub(crate) fn note_verified_run_built(postings: usize) {
    with_active(|counters| {
        counters.verified_runs_built.fetch_add(1, Ordering::Relaxed);
        counters
            .verified_postings_walked
            .fetch_add(postings as u64, Ordering::Relaxed);
    });
}

pub(crate) fn note_verified_read_opened() {
    with_active(|counters| {
        counters
            .verified_reads_opened
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_verified_prefix_probe() {
    with_active(|counters| {
        counters
            .verified_prefix_probes
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_decoded_row() {
    with_active(|counters| {
        counters.decoded_rows.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_row_version_range_read() {
    with_active(|counters| {
        counters
            .row_version_range_reads
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_row_version_point_read() {
    with_active(|counters| {
        counters
            .row_version_point_reads
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_decoded_vector() {
    with_active(|counters| {
        counters.decoded_vectors.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_vector_body_access() {
    with_active(|counters| {
        counters
            .vector_body_accesses
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_hashed_row() {
    with_active(|counters| {
        counters.hashed_rows.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_hashed_vector() {
    with_active(|counters| {
        counters.hashed_vectors.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_hashed_bytes(bytes: usize) {
    with_active(|counters| {
        counters
            .hashed_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    });
}

pub(crate) fn note_authoritative_body_verified() {
    with_active(|counters| {
        counters
            .authoritative_bodies_verified
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_proof_node_read() {
    with_active(|counters| {
        counters.proof_nodes_read.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_proof_node_hashed() {
    with_active(|counters| {
        counters.proof_nodes_hashed.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_ordered_proof_node_read() {
    with_active(|counters| {
        counters
            .ordered_proof_nodes_read
            .fetch_add(1, Ordering::Relaxed);
    });
}
