//! A serving page stays charged for as long as it is allocated, whichever
//! way the serving cache stops holding it while a search is still reading it.
//!
//! A search reads a serving page through its own handle with the cache
//! unlocked. Two ordinary cache events can leave that search holding a page
//! the cache no longer holds:
//!
//! - replacement: another search installs a page into a full cache, and the
//!   slot it takes is the one holding the page the first search is reading;
//! - a duplicate miss: two searches miss the same page at once, both read
//!   it, and the cache keeps only the copy installed first.
//!
//! In both, the page still being read is allocated, so something must still
//! hold its charge until the reading search drops it. Each proof parks a
//! search holding such a page and compares live heap bytes (a counting
//! allocator) with what the budget and the searches hold charged.
//!
//! It runs in its own test binary because it installs a counting allocator.

mod serving_page_charge;

use serving_page_charge::{
    ALLOWANCE, CACHE_PAGES, Parked, Plan, Reading, drop_and_check, fixture, live, parking, search,
    serial, vector,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

/// The first search is parked on this page read, well after the cache is full.
const PARKS_AT_READ: usize = CACHE_PAGES + 4;

/// Search A is parked holding a page it has just read and installed. Search
/// C then runs to completion over the full cache, installing page after page
/// in place of the least recently used one. While A still holds its page,
/// what is live may not have grown past what is charged, and once A lets the
/// page go the charge and the memory fall together.
#[test]
fn a_page_a_search_is_reading_stays_charged_when_another_search_replaces_it() {
    let _serial = serial();
    let fixture = fixture();
    let budget = &fixture.budget;
    let query_a = vector(1_000_001);
    let query_c = vector(2_000_002);
    let quiet = AtomicIsize::new(0);
    let (alone_a, _) = search(&fixture, &query_a, &quiet, Plan::default(), parking().0);
    let (alone_c, _) = search(&fixture, &query_c, &quiet, Plan::default(), parking().0);

    let held_a = AtomicIsize::new(0);
    let held_c = AtomicIsize::new(0);
    let (parking_a, a) = parking();
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
                holding_fill: PARKS_AT_READ,
                past_it: true,
                ..Plan::default()
            };
            search(fixture, query_a, held_a, plan, parking_a).0
        });
        a.wait(Parked::HandedBackItsRead);
        let before = reading();
        let (answer_c, c_reads) = scope
            .spawn(move || search(fixture, query_c, held_c, Plan::default(), parking().0))
            .join()
            .unwrap();
        let replaced = reading();
        a.resume();
        a.wait(Parked::PastItsPage);
        let released = reading();

        println!(
            "search C read {c_reads} pages into a {CACHE_PAGES}-page cache\n\
             while A holds its page: charged {:+} live {:+}\n\
             once A let it go:       charged {:+} live {:+}",
            replaced.charged - before.charged,
            replaced.live - before.live,
            released.charged - before.charged,
            released.live - before.live,
        );
        let mut failures = Vec::new();
        if c_reads <= CACHE_PAGES {
            failures.push(format!(
                "search C read only {c_reads} pages, too few to cycle a {CACHE_PAGES}-page cache"
            ));
        }
        if replaced.charged - before.charged < replaced.live - before.live - ALLOWANCE {
            failures.push(format!(
                "while search A still reads its page and search C filled the cache over it, \
                 the memory held charged changed by {:+} bytes but live memory by {:+}: the \
                 page A reads lost its charge to the page that replaced it, so live memory \
                 exceeds the charge by {} bytes",
                replaced.charged - before.charged,
                replaced.live - before.live,
                (replaced.live - before.live) - (replaced.charged - before.charged),
            ));
        }
        if (released.charged - before.charged - (released.live - before.live)).abs() > ALLOWANCE {
            failures.push(format!(
                "once search A let its page go, the charge and the memory fall together: \
                 charged changed by {:+}, live by {:+}",
                released.charged - before.charged,
                released.live - before.live,
            ));
        }
        a.resume();
        let answer_a = answer_a.join().unwrap();
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

/// Search X is parked inside a page read, before the page is allocated.
/// Search Y, walking the same query, misses the same page, reads it, installs
/// it and is parked holding it. X then finishes its read and finds the page
/// already resident, so the cache does not keep X's copy -- but X goes on
/// reading it. X is parked again when it hands back the charge it read the
/// copy under: the copy must no longer be allocated by then, and once X has
/// moved on the charge and the memory have fallen together.
#[test]
fn a_duplicate_copy_a_search_is_reading_stays_charged_until_it_lets_it_go() {
    let _serial = serial();
    let fixture = fixture();
    let budget = &fixture.budget;
    let query = vector(1_000_001);
    let quiet = AtomicIsize::new(0);
    let (alone, _) = search(&fixture, &query, &quiet, Plan::default(), parking().0);

    let held_x = AtomicIsize::new(0);
    let held_y = AtomicIsize::new(0);
    let (parking_x, x) = parking();
    let (parking_y, y) = parking();
    let read = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
    let reading = || Reading {
        charged: budget.used() as isize
            + held_x.load(Ordering::SeqCst)
            + held_y.load(Ordering::SeqCst),
        live: live(),
    };

    std::thread::scope(|scope| {
        let fixture = &fixture;
        let query = &query;
        let (held_x, held_y) = (&held_x, &held_y);
        let x_read = Arc::clone(&read);
        let answer_x = scope.spawn(move || {
            let plan = Plan {
                in_read: PARKS_AT_READ,
                read: Some(x_read),
                holding_fill: PARKS_AT_READ,
                past_it: true,
                ..Plan::default()
            };
            search(fixture, query, held_x, plan, parking_x).0
        });
        x.wait(Parked::InItsRead);
        let offset = read.0.load(Ordering::SeqCst);
        let in_flight = read.1.load(Ordering::SeqCst) as isize;
        let answer_y = scope.spawn(move || {
            let plan = Plan {
                holding_offset: Some(offset),
                ..Plan::default()
            };
            search(fixture, query, held_y, plan, parking_y).0
        });
        y.wait(Parked::HandedBackItsRead);
        // X was charged for its read before parking inside it, but has not
        // allocated the page yet: that reservation is not yet memory.
        let mut before = reading();
        before.charged -= in_flight;
        x.resume();
        x.wait(Parked::HandedBackItsRead);
        let holding = reading();
        x.resume();
        x.wait(Parked::PastItsPage);
        let released = reading();

        println!(
            "both searches read the page at offset {offset}\n\
             when X handed back its read: charged {:+} live {:+}\n\
             once X moved on:            charged {:+} live {:+}",
            holding.charged - before.charged,
            holding.live - before.live,
            released.charged - before.charged,
            released.live - before.live,
        );
        let mut failures = Vec::new();
        if holding.charged - before.charged < holding.live - before.live - ALLOWANCE {
            failures.push(format!(
                "when search X handed back the charge for the copy the cache did not keep, the \
                 memory held charged had changed by {:+} bytes but live memory by {:+}: X \
                 handed back the charge for a copy it is still reading, so live memory exceeds \
                 the charge by {} bytes",
                holding.charged - before.charged,
                holding.live - before.live,
                (holding.live - before.live) - (holding.charged - before.charged),
            ));
        }
        if (released.charged - before.charged - (released.live - before.live)).abs() > ALLOWANCE {
            failures.push(format!(
                "once search X moved on, the charge and the memory fall together: \
                 charged changed by {:+}, live by {:+}",
                released.charged - before.charged,
                released.live - before.live,
            ));
        }
        x.resume();
        y.resume();
        let answer_x = answer_x.join().unwrap();
        let answer_y = answer_y.join().unwrap();
        assert!(failures.is_empty(), "{}", failures.join("\n"));
        assert_eq!(answer_x, alone, "search X answers as it does alone");
        assert_eq!(answer_y, alone, "search Y answers as it does alone");
    });
    assert_eq!(
        held_x.load(Ordering::SeqCst),
        0,
        "search X gives back its charge"
    );
    assert_eq!(
        held_y.load(Ordering::SeqCst),
        0,
        "search Y gives back its charge"
    );
    drop_and_check(fixture);
}
