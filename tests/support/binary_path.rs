//! Shared workspace-binary resolution for the acceptance and integration
//! test binaries (`tests/acceptance.rs` and `tests/integration.rs`), both
//! `[[test]]` targets of `contextdb-engine` -- this file is included by
//! `#[path]` into each, so it compiles once per test binary from one
//! physical source instead of being copy-pasted per test root.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::SystemTime;

const BIN_PROFILE_ENV_VAR: &str = "CONTEXTDB_TEST_BIN_PROFILE";

pub(crate) fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

/// The directory `cargo build`/`cargo test` actually placed the workspace's
/// binaries in. A build that runs with `CARGO_TARGET_DIR` set to a directory
/// other than `<repo>/target` still has that variable in its own process environment
/// when it runs the compiled test binary, since child processes inherit it
/// from whoever invoked `cargo test`; honoring it here is what lets the
/// binary this same build just produced actually be found.
fn target_root() -> PathBuf {
    let configured = std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from);
    resolve_target_dir(configured.as_deref(), &workspace_root())
}

/// Anchors a possibly-relative `CARGO_TARGET_DIR` against `workspace_root`
/// -- the directory the supported invocation always runs `cargo` from --
/// into the absolute path Cargo actually built into. Cargo resolves a
/// relative `CARGO_TARGET_DIR` against ITS OWN working directory at
/// invocation, not the test binary's package directory; the test process
/// runs from the latter, so using the configured value as-is here silently
/// pointed a relative value at the wrong directory. An already-absolute
/// value names its own directory outright and needs no anchor.
fn resolve_target_dir(configured: Option<&Path>, workspace_root: &Path) -> PathBuf {
    match configured {
        Some(configured) if configured.is_absolute() => configured.to_path_buf(),
        Some(configured) => workspace_root.join(configured),
        None => workspace_root.join("target"),
    }
}

pub(crate) fn binary_name(binary: &str) -> String {
    if cfg!(windows) {
        format!("{binary}.exe")
    } else {
        binary.to_string()
    }
}

/// Resolves `binary` (e.g. `"contextdb"`) to a freshly built workspace
/// binary. `target/debug` and `target/release` can each hold a binary left
/// over from an unrelated earlier build; picking either one blind risks
/// running stale code, which can hide a live regression (false pass) just
/// as easily as it can report one that no longer exists (false fail). The
/// rule: when both profiles have the binary, the one with the newer mtime
/// wins (that is the one a build just produced), and the choice is always
/// printed so a failure is traceable to the exact binary that ran.
/// `CONTEXTDB_TEST_BIN_PROFILE` (`debug` or `release`) pins one profile
/// explicitly and fails loudly if that profile's binary is missing.
pub(crate) fn resolve_workspace_binary(binary: &str) -> PathBuf {
    let name = binary_name(binary);
    let target_root = target_root();
    let debug = target_root.join("debug").join(&name);
    let release = target_root.join("release").join(&name);

    if let Ok(profile) = std::env::var(BIN_PROFILE_ENV_VAR) {
        let (label, path) = match profile.as_str() {
            "debug" => ("debug", debug),
            "release" => ("release", release),
            other => panic!(
                "{BIN_PROFILE_ENV_VAR}={other:?} is not a recognized profile; use \"debug\" or \"release\""
            ),
        };
        assert!(
            path.exists(),
            "contextdb {binary} binary must be built before tests run ({BIN_PROFILE_ENV_VAR}={profile} requested {}); run `cargo build --profile {profile} -p contextdb-cli -p contextdb-server` first.",
            path.display()
        );
        eprintln!(
            "contextdb test harness: using {label} {binary} at {} ({BIN_PROFILE_ENV_VAR}={profile})",
            path.display()
        );
        return path;
    }

    match (mtime(&debug), mtime(&release)) {
        (Some(debug_mtime), Some(release_mtime)) => {
            let (label, chosen) = if release_mtime > debug_mtime {
                ("release", release)
            } else {
                ("debug", debug)
            };
            eprintln!(
                "contextdb test harness: using {label} {binary} at {} (newer of debug/release)",
                chosen.display()
            );
            chosen
        }
        (Some(_), None) => {
            eprintln!(
                "contextdb test harness: using debug {binary} at {}",
                debug.display()
            );
            debug
        }
        (None, Some(_)) => {
            eprintln!(
                "contextdb test harness: using release {binary} at {}",
                release.display()
            );
            release
        }
        (None, None) => panic!(
            "contextdb {binary} binary must be built before tests run; looked for {} and {}. Run `cargo build -p contextdb-cli -p contextdb-server` (add --release for the release profile) before invoking CLI/server tests directly.",
            debug.display(),
            release.display()
        ),
    }
}

fn mtime(path: &Path) -> Option<SystemTime> {
    path.metadata().and_then(|meta| meta.modified()).ok()
}

#[cfg(test)]
mod resolve_target_dir_tests {
    //! `target_root()` returned a configured `CARGO_TARGET_DIR` as-is. Cargo
    //! runs a test binary from its package directory, not the workspace
    //! root it was invoked from, so a relative configured value resolved
    //! under the package instead of the workspace root Cargo actually built
    //! into -- existence checks and binary launches then missed the real build or
    //! picked up a stale binary left over in the wrong place. These proofs
    //! exercise the pure resolution rule against synthetic inputs, never the
    //! real `CARGO_TARGET_DIR` -- mutating that for real would race every
    //! other test in this same binary that depends on it for its own
    //! correct binary resolution.
    use super::*;

    #[test]
    fn an_absolute_configured_target_dir_is_used_unchanged() {
        let workspace_root = Path::new("/workspace/contextdb");
        let configured = Path::new("/var/tmp/some-other-target");
        assert_eq!(
            resolve_target_dir(Some(configured), workspace_root),
            configured.to_path_buf(),
            "an absolute CARGO_TARGET_DIR names its own directory outright"
        );
    }

    #[test]
    fn a_relative_configured_target_dir_is_anchored_to_the_workspace_root() {
        let workspace_root = Path::new("/workspace/contextdb");
        let configured = Path::new(".custom-build-dir/target");
        assert_eq!(
            resolve_target_dir(Some(configured), workspace_root),
            workspace_root.join(".custom-build-dir/target"),
            "a relative CARGO_TARGET_DIR must resolve against the workspace root Cargo was \
             invoked from -- the supported invocation base -- not the test binary's own package \
             directory"
        );
    }

    #[test]
    fn no_configured_target_dir_falls_back_to_workspace_root_target() {
        let workspace_root = Path::new("/workspace/contextdb");
        assert_eq!(
            resolve_target_dir(None, workspace_root),
            workspace_root.join("target"),
            "with CARGO_TARGET_DIR unset, the plain build's own target directory is used"
        );
    }
}
