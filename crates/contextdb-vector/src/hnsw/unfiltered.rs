//! The unfiltered F32 graph walk. Both resident and saved graphs provide only
//! point identities, ordered neighbours and a fallible distance operation.
//! The descent, level-zero frontier, stopping rule and scratch ownership live
//! here once for both representations.
//!
//! Saved and resident layers compute the same walk distance for a query and a
//! stored vector before this walk admits, prunes or stops. That distance is
//! `1 - cosine`. When the query or the stored vector has zero norm, cosine is
//! undefined and both layers use distance 1, matching the published cosine
//! score of 0. The public score and the row-id tie-break run after the walk.

use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn search<E, S, D, A, R, G>(
    source: &mut G,
    query: &[f32],
    knbn: usize,
    ef: usize,
    mut before_source_entry: S,
    mut before_distance: D,
    mut acquire: A,
    mut release: R,
    admitted: &mut AdmittedScratch,
) -> std::result::Result<HnswSearchResult, HnswBoundedSearchError<E>>
where
    G: UnfilteredGraphSource<E>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
{
    before_source_entry().map_err(HnswBoundedSearchError::Callback)?;
    let Some(mut pivot_id) = source.entry_point() else {
        return Ok(HnswSearchResult {
            neighbours: Vec::new(),
        });
    };
    let mut pivot_distance = source.distance(
        pivot_id,
        query,
        &mut before_source_entry,
        &mut before_distance,
        &mut acquire,
        &mut release,
    )?;
    for layer in (1..=pivot_id.0).rev() {
        loop {
            before_source_entry().map_err(HnswBoundedSearchError::Callback)?;
            let count = source.neighbour_count(pivot_id, layer as usize).ok_or(
                HnswBoundedSearchError::SearchInvariant("upper-layer neighbours"),
            )?;
            let mut next_pivot = pivot_id;
            for position in 0..count {
                before_source_entry().map_err(HnswBoundedSearchError::Callback)?;
                let (next_id, _) = source
                    .neighbour_identity(pivot_id, layer as usize, position)
                    .ok_or(HnswBoundedSearchError::CapacityInvariant(
                        "upper-layer neighbour position",
                    ))?;
                let distance = source.distance(
                    next_id,
                    query,
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                )?;
                if distance < pivot_distance {
                    next_pivot = next_id;
                    pivot_distance = distance;
                }
            }
            if next_pivot == pivot_id {
                break;
            }
            pivot_id = next_pivot;
        }
    }

    let counts: [usize; 16] = std::array::from_fn(|layer| source.layer_points(layer));
    let point_count = counts.iter().sum::<usize>();
    let result_capacity = ef.max(knbn).min(point_count);
    let traversal_capacity = point_count;
    let reached_words = ReachedPoints::words_for_counts(&counts);
    for (container, capacity, element_bytes) in [
        (
            HnswSearchScratchContainer::CandidateHeap,
            traversal_capacity,
            std::mem::size_of::<PagedQueuePoint>(),
        ),
        (
            HnswSearchScratchContainer::ReturnHeap,
            result_capacity + 1,
            std::mem::size_of::<PagedQueuePoint>(),
        ),
        (
            HnswSearchScratchContainer::VisitedPoints,
            reached_words,
            std::mem::size_of::<u64>(),
        ),
    ] {
        admitted.admit(container, capacity, element_bytes, &mut acquire)?;
    }
    let mut work = PagedWalkWork::default();
    work.evaluations += 1;
    let mut candidates = BinaryHeap::with_capacity(traversal_capacity);
    let mut found = BinaryHeap::with_capacity(result_capacity + 1);
    let mut visited = ReachedPoints::from_counts(&counts, traversal_capacity);
    let pivot =
        PagedQueuePoint {
            origin_id: source.origin_id(pivot_id).ok_or(
                HnswBoundedSearchError::SearchInvariant("entry point origin"),
            )?,
            point_id: pivot_id,
            distance: pivot_distance,
        };
    candidates.push(PagedQueuePoint {
        distance: -pivot.distance,
        ..pivot
    });
    found.push(pivot);
    visited.insert(pivot_id);
    while let Some(candidate) = candidates.pop() {
        work.frontier_entries += 1;
        work.result_entries += 1;
        let worst = found
            .peek()
            .ok_or(HnswBoundedSearchError::SearchInvariant("return heap"))?;
        if found.len() >= result_capacity && -candidate.distance > worst.distance {
            break;
        }
        before_source_entry().map_err(HnswBoundedSearchError::Callback)?;
        let count = source.neighbour_count(candidate.point_id, 0).ok_or(
            HnswBoundedSearchError::SearchInvariant("layer-zero neighbours"),
        )?;
        for position in 0..count {
            before_source_entry().map_err(HnswBoundedSearchError::Callback)?;
            work.edges += 1;
            work.reached_entries += 1;
            let (next_id, origin_id) = source
                .neighbour_identity(candidate.point_id, 0, position)
                .ok_or(HnswBoundedSearchError::CapacityInvariant(
                "layer-zero neighbour position",
            ))?;
            if visited.contains(next_id) || visited.len() == traversal_capacity {
                continue;
            }
            visited.insert(next_id);
            let worst_distance = found
                .peek()
                .map(|item| item.distance)
                .ok_or(HnswBoundedSearchError::SearchInvariant("return heap"))?;
            let distance = source.distance(
                next_id,
                query,
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            )?;
            work.evaluations += 1;
            if distance < worst_distance || found.len() < result_capacity {
                let result = PagedQueuePoint {
                    origin_id,
                    point_id: next_id,
                    distance,
                };
                candidates.push(PagedQueuePoint {
                    distance: -distance,
                    ..result
                });
                found.push(result);
                if found.len() > result_capacity {
                    work.result_entries += 1;
                    found.pop();
                }
            }
        }
    }
    let found = found.into_sorted_vec();
    let last = knbn.min(found.len());
    drop(candidates);
    drop(visited);
    admitted.give_back_only(HnswSearchScratchContainer::CandidateHeap, &mut release);
    admitted.give_back_only(HnswSearchScratchContainer::VisitedPoints, &mut release);
    #[cfg(feature = "test-seams")]
    observe_paged_vector(|observer| observer.walk_compared(work));
    #[cfg(not(feature = "test-seams"))]
    let _ = work;
    acquire(HnswSearchScratchEvent::Reserve {
        container: HnswSearchScratchContainer::Result,
        previous_capacity: 0,
        requested_capacity: last,
        element_bytes: std::mem::size_of::<Neighbour>(),
    })
    .map_err(HnswBoundedSearchError::Callback)?;
    let mut result = Vec::with_capacity(last);
    result.extend(
        found
            .into_iter()
            .take(last)
            .map(|item| Neighbour::new(item.origin_id, item.distance, item.point_id)),
    );
    admitted.give_back_only(HnswSearchScratchContainer::ReturnHeap, &mut release);
    Ok(HnswSearchResult { neighbours: result })
}
