//! How many saved layers a statement still has to load, and how much room
//! those loads will need.
//!
//! A search over several partitions loads their saved layers -- a base graph,
//! and a change graph on top of it where one was sealed -- one after another,
//! and each load sizes its serving cache from whatever memory is free at the
//! moment it runs. First come, first served: the layers loaded early take a
//! full cache and the last one is left with a cache far smaller than its own
//! pages, so it re-reads itself many times over inside a single walk -- or,
//! when memory is tight, is left without room for its topology at all. Which
//! Context is slow, or refused, then depends on the order the statement
//! happened to admit partitions in, which is no answer at all.
//!
//! A statement therefore says up front how many layers it is going to load
//! and what their least loads come to, and each load leaves that room for the
//! loads after it and takes its share of what is free beyond it. The count is
//! of layers, not partitions: a partition with a base and a change is two
//! loads, and counting it once lets the first half of the layers take the
//! room the second half needed.
//!
//! A layer claims its share once. A load thrown away because a writer moved
//! the fresh tail under it is loaded again on the same claim; a load thrown
//! away because a publication moved its chain gives the claim back, since the
//! statement will load that layer again from where it now lives. The
//! declaration travels on the thread that is executing the statement, which is
//! the same thread that performs its loads; a load on a thread that was told
//! nothing keeps the whole-budget behaviour it always had.

use std::cell::Cell;

thread_local! {
    static REMAINING: Cell<usize> = const { Cell::new(0) };
    static RESERVED: Cell<usize> = const { Cell::new(0) };
    static RESERVED_PAGES: Cell<usize> = const { Cell::new(0) };
    static LAYER: Cell<LayerClaim> = const { Cell::new(LayerClaim::None) };
}

/// Declares that this thread's statement is about to load `layers` saved
/// layers whose least loads come to about `least_bytes`. The previous
/// declaration is restored when the guard drops, so a nested statement cannot
/// leave a stale share behind it.
#[must_use]
pub fn declare_layers_to_load(
    layers: usize,
    least_bytes: usize,
    full_page_bytes: usize,
) -> LayersToLoad {
    LayersToLoad {
        previous: REMAINING.with(|remaining| remaining.replace(layers)),
        previous_reserved: RESERVED.with(|reserved| reserved.replace(least_bytes)),
        previous_pages: RESERVED_PAGES.with(|reserved| reserved.replace(full_page_bytes)),
    }
}

pub struct LayersToLoad {
    previous: usize,
    previous_reserved: usize,
    previous_pages: usize,
}

impl Drop for LayersToLoad {
    fn drop(&mut self) {
        REMAINING.with(|remaining| remaining.set(self.previous));
        RESERVED.with(|reserved| reserved.set(self.previous_reserved));
        RESERVED_PAGES.with(|reserved| reserved.set(self.previous_pages));
    }
}

/// What the next load may take of what is free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Share {
    /// The share of what is free beyond `reserved_for_later`, as a divisor.
    /// `1` means "take what is free", which is what an undeclared load has
    /// always done.
    pub divisor: usize,
    /// Room the loads after this one still need for their least loads.
    pub reserved_for_later: usize,
    /// A conservative bound on the serving pages of the layers still to load.
    pub full_page_bytes_for_later: usize,
}

/// The claim of the layer this thread is loading, if a load is open.
#[derive(Debug, Clone, Copy)]
enum LayerClaim {
    /// No layer load is open: every claim is its own.
    None,
    /// A layer load is open and has not claimed yet.
    Open,
    /// The open layer load claimed `share`, taking `took_layer` from the
    /// layers left to load and `took_reserved` from the room kept for them.
    Claimed {
        share: Share,
        took_layer: bool,
        took_reserved: usize,
        took_pages: usize,
    },
}

/// Opens the load of one saved layer, from its first attempt until it is
/// installed or given up. Every attempt inside it loads on the one share the
/// first attempt claimed. Dropping the guard keeps that claim spent: the layer
/// was installed, is already resident, or the statement failed.
#[must_use]
pub fn begin_layer_load() -> LayerLoad {
    LayerLoad {
        previous: LAYER.with(|layer| layer.replace(LayerClaim::Open)),
    }
}

pub struct LayerLoad {
    previous: LayerClaim,
}

impl LayerLoad {
    /// The load was thrown away and this layer is still to load, from where a
    /// publication moved it: the layers left to load and the room kept for
    /// them get back what this layer's claim took, so its next load claims
    /// the same share again rather than a second one.
    pub fn give_back(self) {
        if let LayerClaim::Claimed {
            took_layer,
            took_reserved,
            took_pages,
            ..
        } = LAYER.with(|layer| layer.replace(LayerClaim::Open))
        {
            if took_layer {
                REMAINING.with(|remaining| remaining.set(remaining.get().saturating_add(1)));
            }
            RESERVED.with(|reserved| reserved.set(reserved.get().saturating_add(took_reserved)));
            RESERVED_PAGES.with(|reserved| reserved.set(reserved.get().saturating_add(took_pages)));
        }
    }
}

impl Drop for LayerLoad {
    fn drop(&mut self) {
        LAYER.with(|layer| layer.set(self.previous));
    }
}

/// Claim the next load's share, whose own least load was estimated at
/// `own_least_bytes`: one fewer layer is left to load, and its estimate no
/// longer needs keeping for later. A retry inside an open layer load gets the
/// share that layer already claimed and takes nothing more.
pub fn claim_share(own_least_bytes: usize, own_full_page_bytes: usize) -> Share {
    let open = match LAYER.with(Cell::get) {
        LayerClaim::Claimed { share, .. } => return share,
        LayerClaim::Open => true,
        LayerClaim::None => false,
    };
    let (divisor, took_layer) = REMAINING.with(|remaining| {
        let left = remaining.get();
        remaining.set(left.saturating_sub(1));
        (left.max(1), left != 0)
    });
    let (reserved_for_later, took_reserved) = RESERVED.with(|reserved| {
        let kept = reserved.get();
        let later = kept.saturating_sub(own_least_bytes);
        reserved.set(later);
        (later, kept - later)
    });
    let (full_page_bytes_for_later, took_pages) = RESERVED_PAGES.with(|reserved| {
        let kept = reserved.get();
        let later = kept.saturating_sub(own_full_page_bytes);
        reserved.set(later);
        (later, kept - later)
    });
    let share = Share {
        divisor,
        reserved_for_later,
        full_page_bytes_for_later,
    };
    if open {
        LAYER.with(|layer| {
            layer.set(LayerClaim::Claimed {
                share,
                took_layer,
                took_reserved,
                took_pages,
            })
        });
    }
    #[cfg(feature = "test-seams")]
    CLAIMS.with(|claims| claims.borrow_mut().push(share));
    share
}

#[cfg(feature = "test-seams")]
thread_local! {
    static CLAIMS: std::cell::RefCell<Vec<Share>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Every share this thread has claimed, oldest first, emptying the record: a
/// proof reads what each load of one statement was given.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn take_claims_for_test() -> Vec<Share> {
    CLAIMS.with(|claims| std::mem::take(&mut *claims.borrow_mut()))
}

/// The loads this thread's statement still has to make and the room kept for
/// them: nothing once no statement is declared.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn outstanding_for_test() -> (usize, usize) {
    (REMAINING.with(Cell::get), RESERVED.with(Cell::get))
}
