//! A declared read-memory limit stays a ceiling while serving pages are
//! borrowed by live searches, and giving serving pages back allocates nothing
//! the budget has not admitted.
//!
//! A search reads a serving page through its own handle with the cache
//! unlocked, so a page the cache lets go of can still be alive in a search
//! that is reading it. When a failed allocation makes the serving caches give
//! pages back, the memory handed to that allocation has to be memory that was
//! really freed: a page a search still holds is still allocated, and its
//! charge must stay held until that search lets it go.
//!
//! Two searches run over one saved graph through the vector store. Search A
//! is parked right after it installed a page into the serving cache, still
//! holding that page. Search C is parked the same way afterwards, so its page
//! is the most recent one and A's is not. From the main thread the budget is
//! then full: an allocation is refused, the serving caches give pages back,
//! and the allocation is tried again, exactly as the engine's accountant
//! does. Live heap bytes (a counting allocator) are compared with what the
//! budget and the two searches hold charged, while A still holds its page,
//! after A lets it go, and after everything is dropped. The allocations made
//! on the main thread while the pages are given back are counted too.
//!
//! It runs in its own test binary because it installs a counting allocator.

mod serving_page_charge;

use contextdb_vector::MemoryBudget;
use serving_page_charge::{
    ALLOCATED_WHILE_WATCHED, ALLOWANCE, CACHE_PAGES, PAGE, Parked, Plan, Reading, WATCHED,
    drop_and_check, fixture, live, parking, search, serial, vector,
};
use std::cell::Cell;
use std::sync::atomic::{AtomicIsize, Ordering};

/// Search A is parked on this page fill, well after the cache is full.
const A_PARKS_AT_FILL: usize = CACHE_PAGES + 4;

#[test]
fn a_page_a_search_still_holds_keeps_its_charge_when_the_cache_gives_it_back() {
    let _serial = serial();
    let fixture = fixture();
    let (budget, store) = (&fixture.budget, &fixture.store);

    let query_a = vector(1_000_001);
    let query_c = vector(2_000_002);
    let quiet = AtomicIsize::new(0);
    let (alone_a, _) = search(&fixture, &query_a, &quiet, Plan::default(), parking().0);
    let (alone_c, _) = search(&fixture, &query_c, &quiet, Plan::default(), parking().0);

    let held_a = AtomicIsize::new(0);
    let held_c = AtomicIsize::new(0);
    let (parking_a, a) = parking();
    let (parking_c, c) = parking();
    let reading = || Reading {
        charged: budget.used() as isize
            + held_a.load(Ordering::SeqCst)
            + held_c.load(Ordering::SeqCst),
        live: live(),
    };

    std::thread::scope(|scope| {
        let fixture = &fixture;
        let (query_a, query_c) = (&query_a, &query_c);
        let (held_a, held_c) = (&held_a, &held_c);
        let answer_a = scope.spawn(move || {
            let plan = Plan {
                holding_fill: A_PARKS_AT_FILL,
                past_it: true,
                ..Plan::default()
            };
            search(fixture, query_a, held_a, plan, parking_a).0
        });
        a.wait(Parked::HandedBackItsRead);
        // C's page is now the most recently used, so a cache cut down to one
        // slot keeps C's page and lets A's go while A still holds it.
        let answer_c = scope.spawn(move || {
            let plan = Plan {
                holding_fill: 1,
                ..Plan::default()
            };
            search(fixture, query_c, held_c, plan, parking_c).0
        });
        c.wait(Parked::HandedBackItsRead);

        // The budget is full: an allocation of what seven serving pages hold
        // is refused, the serving caches give back pages, and the
        // allocation is tried again against whatever that made free. This is
        // the retry the engine's accountant makes for a refused allocation.
        let before = reading();
        budget.limit.store(budget.used(), Ordering::SeqCst);
        let wanted = (CACHE_PAGES - 1) * PAGE;
        assert!(
            budget
                .try_allocate_for(wanted, "read", "retry", "")
                .is_err()
        );
        WATCHED.with(|watched| watched.set(true));
        let given = store.give_back_serving_pages(wanted);
        WATCHED.with(|watched| watched.set(false));
        let allocated_while_giving_back = ALLOCATED_WHILE_WATCHED.with(Cell::get);
        let retried = budget.try_allocate_for(wanted, "read", "retry", "").is_ok();
        let retry_memory = retried.then(|| vec![1_u8; wanted]);
        let holding = reading();

        // A lets its page go and parks before it takes another.
        a.resume();
        a.wait(Parked::PastItsPage);
        let released = reading();

        println!(
            "given_back={given} retried={retried} allocated_while_giving_back=\
             {allocated_while_giving_back}\n\
             while A holds its page: charged {:+} live {:+}\n\
             once A let it go:       charged {:+} live {:+}",
            holding.charged - before.charged,
            holding.live - before.live,
            released.charged - before.charged,
            released.live - before.live,
        );
        // Nothing is asserted while the searches are parked, so a failure
        // reports instead of leaving them waiting.
        let mut failures = Vec::new();
        if given == 0 {
            failures.push("the serving cache gave no pages back".to_string());
        }
        if holding.charged - before.charged < holding.live - before.live - ALLOWANCE {
            failures.push(format!(
                "while search A still holds a page the cache gave back, the memory held \
                 charged changed by {:+} bytes but live memory changed by {:+}: the give-back \
                 released a page that is still allocated and the retried allocation spent that \
                 room, so live memory exceeds the charge by {} bytes",
                holding.charged - before.charged,
                holding.live - before.live,
                (holding.live - before.live) - (holding.charged - before.charged),
            ));
        }
        if allocated_while_giving_back != 0 {
            failures.push(format!(
                "giving serving pages back allocated {allocated_while_giving_back} bytes the \
                 full budget never admitted"
            ));
        }
        if (released.charged - before.charged - (released.live - before.live)).abs() > ALLOWANCE {
            failures.push(format!(
                "once search A let the page go its charge must follow it: charged changed by \
                 {:+}, live by {:+}",
                released.charged - before.charged,
                released.live - before.live,
            ));
        }

        drop(retry_memory);
        if retried {
            budget.release(wanted);
        }
        budget.limit.store(usize::MAX, Ordering::SeqCst);
        a.resume();
        c.resume();
        let answer_a = answer_a.join().unwrap();
        let answer_c = answer_c.join().unwrap();
        assert!(failures.is_empty(), "{}", failures.join("\n"));
        assert_eq!(answer_a, alone_a, "search A answers as it does alone");
        assert_eq!(answer_c, alone_c, "search C answers as it does alone");
    });
    assert_eq!(
        held_a.load(Ordering::SeqCst),
        0,
        "search A gives back its charge"
    );
    assert_eq!(
        held_c.load(Ordering::SeqCst),
        0,
        "search C gives back its charge"
    );
    drop_and_check(fixture);
}
