//! The maintained HNSW fork's recorded delta must describe the fork as it is.
//!
//! `crates/contextdb-hnsw/upstream-0.3.4.patch` is the record of everything
//! this repository changed in `hnsw_rs` 0.3.4, and an upgrade rebases that
//! record rather than the sources. A hunk whose result no longer matches the
//! file it claims to produce is therefore not a stale document: re-deriving the
//! fork from it silently reinstates whatever the source has since fixed. This
//! audit compares every hunk's result against the file it names, which needs
//! nothing but the two files in this repository.

use std::path::Path;

#[path = "audit_support/mod.rs"]
mod audit_support;
use audit_support::workspace_root;

/// One `@@ -a,b +c,d @@` header.
fn hunk_start(header: &str) -> Option<usize> {
    let after = header.split_once("+")?.1;
    let numbers = after
        .split(|c: char| !c.is_ascii_digit())
        .find(|part| !part.is_empty())?;
    numbers.parse::<usize>().ok()
}

#[test]
fn the_hnsw_fork_patch_produces_the_sources_it_names() {
    let fork = workspace_root().join("crates/contextdb-hnsw");
    let patch = std::fs::read_to_string(fork.join("upstream-0.3.4.patch"))
        .expect("the maintained fork records its delta from upstream");
    let mut target: Option<(String, Vec<String>)> = None;
    let mut line_number = 0usize;
    let mut checked_files = 0usize;
    let mut checked_lines = 0usize;

    for line in patch.lines() {
        if let Some(path) = line.strip_prefix("+++ b/") {
            let file = fork.join(path);
            let body = std::fs::read_to_string(&file)
                .unwrap_or_else(|error| panic!("the patch names {}: {error}", file.display()));
            target = Some((
                path.to_owned(),
                body.lines().map(str::to_owned).collect::<Vec<_>>(),
            ));
            checked_files += 1;
            continue;
        }
        if line.starts_with("--- ") {
            continue;
        }
        let Some((path, body)) = target.as_ref() else {
            continue;
        };
        if line.starts_with("@@") {
            line_number = hunk_start(line).unwrap_or_else(|| {
                panic!("{path} has a hunk header this audit cannot read: {line}")
            });
            continue;
        }
        // A removal describes the upstream file, not this one.
        if let Some(rest) = line.strip_prefix('-') {
            let _ = rest;
            continue;
        }
        let produced = match line.strip_prefix('+').or_else(|| line.strip_prefix(' ')) {
            Some(produced) => produced,
            // A "\\ No newline at end of file" marker, or the blank line some
            // diff writers emit for an empty context line.
            None if line.is_empty() => "",
            None => continue,
        };
        let actual = body.get(line_number.saturating_sub(1)).map(String::as_str);
        assert_eq!(
            actual,
            Some(produced),
            "crates/contextdb-hnsw/upstream-0.3.4.patch disagrees with {path} at line \
             {line_number}: the patch produces {produced:?} and the file holds {actual:?}. \
             Regenerate the patch as MAINTENANCE.md describes; a patch that no longer \
             describes the fork reinstates whatever the source has since fixed."
        );
        line_number += 1;
        checked_lines += 1;
    }

    assert!(
        checked_files >= 2 && checked_lines > 100,
        "this audit must really read the recorded delta: {checked_files} files, \
         {checked_lines} produced lines"
    );
    assert!(
        Path::new(&fork.join("MAINTENANCE.md")).exists(),
        "the fork records how to regenerate its delta"
    );
}
