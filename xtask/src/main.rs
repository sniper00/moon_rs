//! Workspace task runner (the `cargo-xtask` pattern).
//!
//! Run via the cargo alias defined in `.cargo/config.toml`:
//!
//! ```text
//! cargo xtask build [name...]    # fetch (per lock) + build + install Lua C extensions
//! cargo xtask update [name...]   # resolve each `ref` to a commit, write extensions.lock
//! cargo xtask list               # show the registry + lock status
//! cargo xtask clean [name...]    # remove cached extension checkouts
//! ```
//!
//! Lua C extensions (e.g. `thrift`) are NOT vendored in this repo: their source
//! lives in separate GitHub repos, declared in `extensions.toml`. `build` clones
//! the locked commit into `.extensions/<name>/<rev>/`, compiles the standalone
//! `cdylib`, and installs it into `clib/<name>.<ext>` so moon_rs can load it via
//! `require "<name>"`.
//!
//! ## ABI safety: `[patch]` redirect of `moon-base`
//! External extensions declare `moon-base` as a git dependency on this repo.
//! At build time xtask injects a Cargo `[patch]` (via `--config`) that redirects
//! that dependency to the **local working-copy** `crates/moon-base`, so every
//! extension is compiled against the exact same Lua 5.5 FFI / `Buffer` ABI as the
//! host. The downloaded source is never modified.
//!
//! Reproducibility: `build` only trusts `extensions.lock` (pinned commits). Run
//! `update` to move to a newer `ref`.

use std::collections::BTreeMap;
use std::env::consts::{DLL_PREFIX, DLL_SUFFIX};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::Command;

const MANIFEST_FILE: &str = "extensions.toml";
const LOCK_FILE: &str = "extensions.lock";
const CACHE_DIR: &str = ".extensions";
const CLIB_DIR: &str = "clib";
const MOON_BASE_CRATE: &str = "crates/moon-base";

type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// `extensions.toml` — the human-maintained registry / trust list.
#[derive(serde::Deserialize)]
struct Manifest {
    /// Git URL this repo is published at; extensions declare `moon-base` from
    /// here, and it is the `[patch]` key xtask overrides with the local path.
    moon_base_git: String,
    #[serde(default)]
    extensions: BTreeMap<String, ExtSpec>,
}

#[derive(serde::Deserialize, Clone)]
struct ExtSpec {
    git: String,
    /// Tag or branch; resolved to a commit by `update`.
    r#ref: String,
    #[serde(default = "dot")]
    subdir: String,
    /// The crate's `[lib] name`; installed as `clib/<lib>.<ext>`.
    lib: String,
}

/// `extensions.lock` — machine-generated, pins each extension to a commit.
#[derive(serde::Deserialize, serde::Serialize, Default)]
struct Lock {
    #[serde(default)]
    extensions: BTreeMap<String, LockEntry>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone)]
struct LockEntry {
    git: String,
    rev: String,
}

fn dot() -> String {
    ".".to_string()
}

struct Flags {
    offline: bool,
    force: bool,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("xtask: error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut task = None;
    let mut names = Vec::new();
    let mut flags = Flags {
        offline: false,
        force: false,
    };
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--offline" => flags.offline = true,
            "--force" => flags.force = true,
            "-h" | "--help" | "help" => {
                print_usage();
                return Ok(());
            }
            _ if task.is_none() => task = Some(arg),
            _ => names.push(arg),
        }
    }

    let repo_root = repo_root();
    match task.as_deref() {
        Some("build") => cmd_build(&repo_root, &names, &flags),
        Some("update") => cmd_update(&repo_root, &names),
        Some("list") => cmd_list(&repo_root),
        Some("clean") => cmd_clean(&repo_root, &names),
        None => {
            print_usage();
            Ok(())
        }
        Some(other) => {
            print_usage();
            Err(format!("unknown task `{other}`").into())
        }
    }
}

fn print_usage() {
    eprintln!(
        "cargo xtask <task> [name...] [--offline] [--force]\n\n\
         Tasks:\n  \
         build [name...]    Fetch (per lock) + build + install Lua C extension(s) into clib/.\n  \
         update [name...]   Resolve each ref to a commit and write {LOCK_FILE}.\n  \
         list               Show the registry and lock status.\n  \
         clean [name...]    Remove cached checkouts under {CACHE_DIR}/.\n\n\
         With no name, the command applies to every extension in {MANIFEST_FILE}.\n\
         Flags: --offline (use cache only, never hit the network), --force (re-fetch)."
    );
}

// --------------------------------------------------------------------------
// Commands
// --------------------------------------------------------------------------

fn cmd_build(repo_root: &Path, names: &[String], flags: &Flags) -> Result<()> {
    let manifest = load_manifest(repo_root)?;
    let lock = load_lock(repo_root)?;
    let selected = select(&manifest, names)?;

    for (name, spec) in selected {
        let entry = lock.extensions.get(&name).ok_or_else(|| {
            format!("`{name}` is not locked; run `cargo xtask update {name}` first")
        })?;
        let checkout = ensure_checkout(repo_root, &name, entry, flags)?;
        build_and_install(repo_root, &manifest, &spec, &checkout)?;
    }
    Ok(())
}

fn cmd_update(repo_root: &Path, names: &[String]) -> Result<()> {
    let manifest = load_manifest(repo_root)?;
    let mut lock = load_lock(repo_root)?;
    let selected = select(&manifest, names)?;

    for (name, spec) in selected {
        let git = effective_git(&name, &spec);
        let rev = resolve_rev(&git, &spec.r#ref)?;
        println!("locked {name}: {} @ {} -> {rev}", spec.r#ref, short(&rev));
        lock.extensions.insert(name, LockEntry { git, rev });
    }
    save_lock(repo_root, &lock)?;
    println!("wrote {LOCK_FILE}");
    Ok(())
}

fn cmd_list(repo_root: &Path) -> Result<()> {
    let manifest = load_manifest(repo_root)?;
    let lock = load_lock(repo_root)?;
    if manifest.extensions.is_empty() {
        println!("(no extensions registered in {MANIFEST_FILE})");
        return Ok(());
    }
    for (name, spec) in &manifest.extensions {
        let locked = match lock.extensions.get(name) {
            Some(e) => format!("locked {}", short(&e.rev)),
            None => "unlocked".to_string(),
        };
        println!("{name:<12} {} @ {} [{locked}]", spec.git, spec.r#ref);
    }
    Ok(())
}

fn cmd_clean(repo_root: &Path, names: &[String]) -> Result<()> {
    let cache = repo_root.join(CACHE_DIR);
    if names.is_empty() {
        if cache.exists() {
            std::fs::remove_dir_all(&cache)?;
            println!("removed {}", cache.display());
        }
    } else {
        for name in names {
            let dir = cache.join(name);
            if dir.exists() {
                std::fs::remove_dir_all(&dir)?;
                println!("removed {}", dir.display());
            }
        }
    }
    Ok(())
}

// --------------------------------------------------------------------------
// Fetch + build
// --------------------------------------------------------------------------

/// Ensure the locked commit is checked out under `.extensions/<name>/<rev>` and
/// return the checkout path. Reuses an existing checkout unless `--force`.
fn ensure_checkout(
    repo_root: &Path,
    name: &str,
    entry: &LockEntry,
    flags: &Flags,
) -> Result<PathBuf> {
    let dest = repo_root.join(CACHE_DIR).join(name).join(&entry.rev);

    if dest.exists() && !flags.force {
        return Ok(dest);
    }
    if flags.offline {
        return Err(format!(
            "`{name}` @ {} not in cache and --offline was given",
            short(&entry.rev)
        )
        .into());
    }
    if dest.exists() {
        std::fs::remove_dir_all(&dest)?;
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }

    println!("fetching {name}: {} @ {}", entry.git, short(&entry.rev));
    git(&["clone", "--quiet", &entry.git])?
        .arg(&dest)
        .run("git clone")?;
    git(&["-C"])?
        .arg(&dest)
        .args(["checkout", "--quiet", &entry.rev])
        .run("git checkout")?;
    Ok(dest)
}

fn build_and_install(
    repo_root: &Path,
    manifest: &Manifest,
    spec: &ExtSpec,
    checkout: &Path,
) -> Result<()> {
    let crate_dir = checkout.join(&spec.subdir);
    let manifest_path = crate_dir.join("Cargo.toml");
    if !manifest_path.exists() {
        return Err(format!("{} not found", manifest_path.display()).into());
    }

    // Redirect the extension's `moon-base` git dependency to the local crate so
    // it is built against this repo's exact Lua FFI / Buffer ABI.
    let local_moon_base = repo_root.join(MOON_BASE_CRATE);
    let patch = format!(
        "patch.\"{}\".moon-base.path=\"{}\"",
        manifest.moon_base_git,
        local_moon_base.display()
    );

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    Command::new(&cargo)
        .args(["build", "--release", "--manifest-path"])
        .arg(&manifest_path)
        .arg("--config")
        .arg(&patch)
        .status()?
        .ok_or("cargo build failed")?;

    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| crate_dir.join("target"));
    let built = format!("{DLL_PREFIX}{}{DLL_SUFFIX}", spec.lib);
    let src = target_dir.join("release").join(&built);

    let out_dir = repo_root.join(CLIB_DIR);
    std::fs::create_dir_all(&out_dir)?;
    // Lua loads the bare module name, so strip the platform `lib` prefix.
    let dest = out_dir.join(format!("{}{DLL_SUFFIX}", spec.lib));
    std::fs::copy(&src, &dest)
        .map_err(|e| format!("copy {} -> {}: {e}", src.display(), dest.display()))?;

    println!("installed {}", dest.display());
    Ok(())
}

/// Resolve a tag/branch (or pass through a full commit sha) to a commit sha.
fn resolve_rev(git_url: &str, r#ref: &str) -> Result<String> {
    if r#ref.len() == 40 && r#ref.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(r#ref.to_string());
    }
    let out = git(&["ls-remote", git_url, r#ref])?
        .output()
        .map_err(|e| format!("git ls-remote: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "git ls-remote {git_url} {ref} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim(),
            ref = r#ref
        )
        .into());
    }
    let text = String::from_utf8_lossy(&out.stdout);
    // Prefer the peeled annotated-tag line (`<sha>\trefs/tags/<ref>^{}`).
    let mut sha = None;
    for line in text.lines() {
        let Some((s, r)) = line.split_once('\t') else {
            continue;
        };
        if r.ends_with("^{}") {
            sha = Some(s.to_string());
            break;
        }
        if sha.is_none() {
            sha = Some(s.to_string());
        }
    }
    sha.ok_or_else(|| format!("ref `{}` not found in {git_url}", r#ref).into())
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

/// Allow overriding an extension's git URL for local development / mirrors via
/// `XTASK_<NAME>_GIT` without editing the committed manifest.
fn effective_git(name: &str, spec: &ExtSpec) -> String {
    std::env::var(format!("XTASK_{}_GIT", name.to_uppercase())).unwrap_or_else(|_| spec.git.clone())
}

fn select(manifest: &Manifest, names: &[String]) -> Result<Vec<(String, ExtSpec)>> {
    if names.is_empty() {
        return Ok(manifest
            .extensions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect());
    }
    names
        .iter()
        .map(|n| {
            manifest
                .extensions
                .get(n)
                .map(|s| (n.clone(), s.clone()))
                .ok_or_else(|| format!("unknown extension `{n}` (not in {MANIFEST_FILE})").into())
        })
        .collect()
}

fn load_manifest(repo_root: &Path) -> Result<Manifest> {
    let path = repo_root.join(MANIFEST_FILE);
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
    Ok(toml::from_str(&text).map_err(|e| format!("parsing {MANIFEST_FILE}: {e}"))?)
}

fn load_lock(repo_root: &Path) -> Result<Lock> {
    let path = repo_root.join(LOCK_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => Ok(toml::from_str(&text).map_err(|e| format!("parsing {LOCK_FILE}: {e}"))?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Lock::default()),
        Err(e) => Err(format!("reading {}: {e}", path.display()).into()),
    }
}

fn save_lock(repo_root: &Path, lock: &Lock) -> Result<()> {
    let text = format!(
        "# Generated by `cargo xtask update`. Pins each extension to a commit.\n{}",
        toml::to_string(lock)?
    );
    std::fs::write(repo_root.join(LOCK_FILE), text)?;
    Ok(())
}

fn git(args: &[&str]) -> Result<Command> {
    let mut c = Command::new("git");
    c.args(args);
    Ok(c)
}

fn short(rev: &str) -> &str {
    &rev[..rev.len().min(10)]
}

/// Repo root = the parent of this `xtask` crate directory.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate must live one level below the repo root")
        .to_path_buf()
}

// Small ergonomic extensions for running child processes.
trait CommandExt {
    fn run(&mut self, what: &str) -> Result<()>;
}

impl CommandExt for Command {
    fn run(&mut self, what: &str) -> Result<()> {
        self.status()?.ok_or(what)?;
        Ok(())
    }
}

trait StatusExt {
    fn ok_or(self, what: &str) -> Result<()>;
}

impl StatusExt for std::process::ExitStatus {
    fn ok_or(self, what: &str) -> Result<()> {
        if self.success() {
            Ok(())
        } else {
            Err(format!("{what} failed ({self})").into())
        }
    }
}
