//! The append instrument exists only in a build that asked for it.
//!
//! Every append-work counter and span is behind `test-seams`:
//! `append_work` is a `#[cfg(feature = "test-seams")]` module, the storage
//! fork's `commit_work` is one too, and each call site in the production path
//! stands inside a region that attribute guards. A build without the feature
//! therefore contains none of it — not a branch, not a clock read, not a
//! thread-local.
//!
//! That is a claim about source, so it is checked against source, by finding
//! what the attribute actually guards rather than by looking at the line above
//! a call. The other half of the claim — that switching the instrument ON does
//! not change the work the engine performs — is
//! `one_event_append_work_by_family`, which pins the same per-family mutation
//! vector, the same node writes and the same bytes with the spans present as
//! it did without them.

use std::path::{Path, PathBuf};

const ATTRIBUTE: &str = "#[cfg(feature = \"test-seams\")]";

/// The seam calls a production path makes, each qualified enough that an
/// unrelated API of the same name is not mistaken for one.
const SEAM_CALLS: [&str; 6] = [
    "observe_append_stage(",
    "commit_work::observe_commit_span(",
    "commit_work::observe_commit_span_count(",
    "commit_work::observe_commit(",
    "commit_work::note_freed_page(",
    "commit_work::set_tag(",
];

fn rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(entry) = pending.pop() {
        for item in std::fs::read_dir(&entry).expect("read a source tree") {
            let path = item.expect("read a source entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// Brackets inside a string or a character literal are text, not structure.
fn brackets_only(line: &str) -> String {
    let mut kept = String::new();
    let mut characters = line.chars().peekable();
    let mut quote: Option<char> = None;
    while let Some(character) = characters.next() {
        match quote {
            Some(open) => {
                if character == '\\' {
                    characters.next();
                } else if character == open {
                    quote = None;
                }
            }
            None => match character {
                '"' | '\'' => quote = Some(character),
                '/' if characters.peek() == Some(&'/') => break,
                '{' | '}' | '(' | ')' | '[' | ']' => kept.push(character),
                _ => {}
            },
        }
    }
    kept
}

/// The lines `#[cfg(feature = "test-seams")]` guards: the item or statement
/// that follows it, to the point where every bracket it opened is closed.
fn guarded_lines(lines: &[&str]) -> Vec<bool> {
    let mut guarded = vec![false; lines.len()];
    for (at, line) in lines.iter().enumerate() {
        if !line.trim().starts_with(ATTRIBUTE) {
            continue;
        }
        // Other attributes and doc comments may stand between the guard and
        // what it guards.
        let mut start = at + 1;
        while start < lines.len() {
            let trimmed = lines[start].trim();
            if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with("#[") {
                start += 1;
                continue;
            }
            break;
        }
        let mut depth = 0i32;
        for (index, guarded_line) in lines.iter().enumerate().skip(start) {
            guarded[index] = true;
            for bracket in brackets_only(guarded_line).chars() {
                depth += match bracket {
                    '{' | '(' | '[' => 1,
                    _ => -1,
                };
            }
            if depth <= 0 {
                break;
            }
        }
    }
    guarded
}

/// The two files that define the instrument are compiled only under the
/// feature — the guard is on their `mod` declaration — so the definitions
/// inside them need no attribute of their own.
fn wholly_gated(path: &Path) -> bool {
    path.ends_with("commit_work.rs") || path.ends_with("append_work.rs")
}

#[test]
fn every_append_measurement_call_is_behind_the_test_seams_feature() {
    let engine = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let storage = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../contextdb-redb/src")
        .canonicalize()
        .expect("the storage fork is a sibling of this crate");
    let mut ungated: Vec<String> = Vec::new();
    let mut found = 0usize;
    for path in rust_files(&engine).into_iter().chain(rust_files(&storage)) {
        if wholly_gated(&path) {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read a source file");
        if !SEAM_CALLS.iter().any(|call| text.contains(call)) {
            continue;
        }
        let lines = text.lines().collect::<Vec<_>>();
        let guarded = guarded_lines(&lines);
        for (number, line) in lines.iter().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            for call in SEAM_CALLS {
                if !line.contains(call) {
                    continue;
                }
                found += 1;
                if !guarded[number] {
                    ungated.push(format!(
                        "{}:{}: {}",
                        path.display(),
                        number + 1,
                        line.trim()
                    ));
                }
            }
        }
    }
    assert!(
        found >= 10,
        "the walk found only {found} seam calls, so it is looking in the wrong place rather than \
         finding a clean tree"
    );
    assert!(
        ungated.is_empty(),
        "an append-measurement seam is called from a production path that is not behind \
         `test-seams`, so a build that did not ask for the instrument would pay for it:\n{}",
        ungated.join("\n"),
    );
}

/// The two modules that publish the instrument are themselves declared behind
/// the feature, so nothing outside a `test-seams` build can even name them.
#[test]
fn the_instrument_modules_are_declared_behind_the_feature() {
    for (crate_root, module) in [
        ("src/lib.rs", "pub mod append_work;"),
        ("../contextdb-redb/src/lib.rs", "pub mod commit_work;"),
    ] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(crate_root);
        let text = std::fs::read_to_string(&path).expect("read a crate root");
        let lines = text.lines().collect::<Vec<_>>();
        let at = lines
            .iter()
            .position(|line| line.trim() == module)
            .unwrap_or_else(|| panic!("{} declares {module}", path.display()));
        assert!(
            guarded_lines(&lines)[at],
            "{} declares {module} outside the feature guard",
            path.display(),
        );
    }
}
