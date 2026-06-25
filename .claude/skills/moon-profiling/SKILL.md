---
name: moon-profiling
description: >-
  Performance profiling and A/B benchmarking for moon_rs Rust↔Lua modules.
  Covers samply (end-to-end CPU profiling with Lua/FFI flame graphs), pprof
  (pure-Rust flame graphs via criterion/pprof crate), and A/B comparison
  scripts (ab_zset.py). Use when diagnosing a performance regression, evaluating
  an optimization, or profiling any moon_rs native module.
---

# moon_rs: Performance Profiling & Benchmarking

Toolkit for profiling Rust↔Lua native modules in moon_rs. Covers three tiers:
end-to-end system profiling (samply), pure-Rust micro-benchmarking (pprof), and
A/B comparison testing.

Tested on: macOS (arm64, darwin), no sudo.

## 0. Prerequisite: build with the `profiling` profile

The `release` profile has `strip = true`, which removes all symbols — samply,
`atos`, and pprof can only show raw addresses. The repo provides a dedicated
profile (workspace-level `Cargo.toml`):

```toml
[profile.profiling]
inherits = "release"
strip = false
debug = 1
```

**Always use `--profile profiling` for any profiling work:**

```bash
cargo build --profile profiling          # output in target/profiling/
```

## 1. samply — end-to-end sampled flame graph (with Lua/FFI)

Best for answering: "what's the time breakdown of the entire benchmark —
Rust vs Lua VM vs allocator?"

```bash
# Record (headless save, no browser)
samply record --save-only --no-open -o /tmp/zset.json.gz -- \
    ./target/profiling/moon_rs assets/benchmark/benchmark_zset.lua 1000000 1000000

# Option A: interactive flame graph / call tree / timeline in browser
samply load /tmp/zset.json.gz

# Option B: headless text symbolication (self-time by function), for no-GUI environments
python3 assets/benchmark/analyze_samply.py /tmp/zset.json.gz ./target/profiling/moon_rs actor-bootstrap 35
```

`analyze_samply.py <profile> <binary> [thread_name] [top_n]` (see
[analyze_samply.py](analyze_samply.py)):
- Filters to the named thread only (zset benchmarks run on `actor-bootstrap`,
  **not** `main`).
- Symbolicates module-relative addresses via `atos` (uses `__TEXT` base
  `0x100000000`).
- Aggregates self-time by leaf frame, stripping Rust `::h<hash>` suffixes.

(`parse_profile.py` is a symbol-free generic Firefox-profiler parser that
only gives raw addresses; prefer `analyze_samply.py`.)

## 2. pprof — pure-Rust flame graph (zero Lua/FFI noise)

Best for answering: "which code inside the skiplist (or any Rust data structure)
is hot?" — call stacks contain **no Lua interpreter frames**.

`pprof` is a dev-dependency of `moon-runtime`. `lua_zset.rs` has an `#[ignore]`d
profiling test case that directly drives the private `ZSet`:

```bash
cargo test -p moon-runtime --profile profiling \
    lua_zset::tests::profile_flamegraph -- --ignored --nocapture
```

- Produces one SVG per operation:
  `target/profile/zset_{build_insert,rank,key_by_rank,update_reposition,erase_reinsert}.svg`.
  Open in a browser to zoom/search.
- Scale is configurable via env vars: `ZSET_N` (default 1e6), `ZSET_OPS`
  (default 1e6).

Headless hotspot extraction:

```bash
grep -oE '<title>[^<]*</title>' target/profile/zset_rank.svg | sed -E 's/<\/?title>//g'
```

## 3. A/B comparison — is an optimization actually effective?

`ab_zset.py` runs two binaries interleaved N rounds on the same machine and
reports per-operation mean and **best-of-N** (see [ab_zset.py](ab_zset.py)):

```bash
# Build baseline (e.g. FLAG = true)
cargo build --release && cp target/release/moon_rs /tmp/moon_a
# Toggle FLAG = false, rebuild
cargo build --release && cp target/release/moon_rs /tmp/moon_b
# Compare: ab_zset.py [rounds] [baseline_binary] [candidate_binary]
python3 assets/benchmark/ab_zset.py 10 /tmp/moon_b /tmp/moon_a
```

## Tools reference

| Tool | Location | Purpose |
| --- | --- | --- |
| `analyze_samply.py` | `assets/benchmark/analyze_samply.py` | Symbolicate samply profiles with `atos`; filter by thread; aggregate self-time |
| `ab_zset.py` | `assets/benchmark/ab_zset.py` | A/B comparison: interleave two binaries N rounds, report mean + best-of-N |
| `parse_profile.py` | `assets/benchmark/parse_profile.py` | Generic Firefox-profiler JSON parser (raw addresses, no symbolication) |

Copies of the scripts are included here for reference: [analyze_samply.py](analyze_samply.py),
[ab_zset.py](ab_zset.py), [parse_profile.py](parse_profile.py).

## Gotchas & methodology

- **`release` `strip=true` strips symbols** → always use `--profile profiling` for profiling.
- **Self-time % in flame graphs is misleading after inlining/reordering.**
  A frame dropping from 77% to 0% after a code change doesn't prove it got
  faster — check whether **total sample count** actually decreased. (Once
  misjudged an "optimization" this way; wall-clock was unchanged.)
- **Only trust A/B best-of-N wall-clock to judge an optimization.** `mean` is
  heavily polluted by occasional slow runs (observed mean +16.7% while
  best-of −0.1%).
- **zset benchmarks run on the `actor-bootstrap` thread**, not `main`. Select
  the correct thread in samply analysis.
- **Skiplist traversal is a tight dependent-load chain** (next pointer depends
  on the just-loaded node). Software prefetch has almost no lookahead and
  showed zero benefit in testing. Further speedups must come from data
  structure footprint / level height, not micro-optimization.

## Snapshot findings (N=1e6 for zset)

- 93% of time in 5 skiplist functions; FFI/Lua ~5%; allocator ~0%.
- `score`/`has` faster than C++; `rank`/`update`/`erase` ~1.3–1.6× C++, with
  the bottleneck being pointer-chasing cache misses (intrinsic data structure
  cost).
