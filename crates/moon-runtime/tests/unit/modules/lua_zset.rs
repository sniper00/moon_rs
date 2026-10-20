use super::*;

fn keys_in_order(zset: &ZSet) -> Vec<i64> {
    let mut out = Vec::new();
    let mut node = zset.zsl.begin();
    while node != NIL {
        out.push(zset.zsl.key_of(node));
        node = zset.zsl.forward(node, 0);
    }
    out
}

#[test]
fn descending_score_then_timestamp_then_key() {
    let mut z = ZSet::new(usize::MAX, false);
    z.update(1, 100, 10);
    z.update(2, 200, 5);
    z.update(3, 100, 8); // same score as key 1, earlier timestamp
    z.update(4, 100, 8); // same score & timestamp as key 3, larger key

    // 200 first; then score 100 group ordered by timestamp asc then key asc.
    assert_eq!(keys_in_order(&z), vec![2, 3, 4, 1]);
    assert_eq!(z.size(), 4);
}

#[test]
fn rank_and_score_and_has() {
    let mut z = ZSet::new(usize::MAX, false);
    z.update(1, 100, 1);
    z.update(2, 200, 1);
    z.update(3, 50, 1);

    assert_eq!(z.rank(2), 1);
    assert_eq!(z.rank(1), 2);
    assert_eq!(z.rank(3), 3);
    assert_eq!(z.rank(999), 0); // missing

    assert_eq!(z.score(2), 200);
    assert_eq!(z.score(999), 0);
    assert!(z.has(1));
    assert!(!z.has(999));
}

#[test]
fn update_existing_repositions() {
    let mut z = ZSet::new(usize::MAX, false);
    z.update(1, 100, 1);
    z.update(2, 200, 1);
    z.update(3, 50, 1);
    assert_eq!(z.rank(1), 2);

    // Bump key 1 to the top.
    z.update(1, 300, 1);
    assert_eq!(z.rank(1), 1);
    assert_eq!(z.score(1), 300);
    assert_eq!(z.size(), 3);
    assert_eq!(keys_in_order(&z), vec![1, 2, 3]);

    // Drop key 1 to the bottom.
    z.update(1, 10, 1);
    assert_eq!(z.rank(1), 3);
    assert_eq!(keys_in_order(&z), vec![2, 3, 1]);
}

#[test]
fn reverse_orders_ascending() {
    let mut z = ZSet::new(usize::MAX, true);
    z.update(1, 100, 1);
    z.update(2, 200, 1);
    z.update(3, 50, 1);

    // Lower score ranks first in reverse mode.
    assert_eq!(z.rank(3), 1);
    assert_eq!(z.rank(1), 2);
    assert_eq!(z.rank(2), 3);
    // Score is reported un-negated.
    assert_eq!(z.score(2), 200);
    assert_eq!(keys_in_order(&z), vec![3, 1, 2]);
}

#[test]
fn eviction_at_max_count() {
    let mut z = ZSet::new(3, false);
    z.update(1, 100, 1);
    z.update(2, 200, 1);
    z.update(3, 50, 1);
    assert_eq!(z.size(), 3);

    // 300 is better than the worst (key 3, score 50) -> inserted, key 3 evicted.
    z.update(4, 300, 1);
    assert_eq!(z.size(), 3);
    assert!(!z.has(3));
    assert_eq!(keys_in_order(&z), vec![4, 2, 1]);

    // 10 is worse than the current worst -> rejected.
    z.update(5, 10, 1);
    assert_eq!(z.size(), 3);
    assert!(!z.has(5));
}

#[test]
fn erase_removes() {
    let mut z = ZSet::new(usize::MAX, false);
    z.update(1, 100, 1);
    z.update(2, 200, 1);
    assert_eq!(z.erase(1), 1);
    assert_eq!(z.erase(1), 0);
    assert_eq!(z.size(), 1);
    assert_eq!(z.rank(2), 1);
}

#[test]
fn key_by_rank_bounds() {
    let mut z = ZSet::new(usize::MAX, false);
    z.update(1, 100, 1);
    z.update(2, 200, 1);
    z.update(3, 50, 1);

    assert_eq!(z.key_by_rank(1), Some(2));
    assert_eq!(z.key_by_rank(2), Some(1));
    assert_eq!(z.key_by_rank(3), Some(3));
    assert_eq!(z.key_by_rank(4), None);
}

#[test]
fn range_positive_negative_and_reverse() {
    let mut z = ZSet::new(usize::MAX, false);
    for (k, s) in [(1, 100), (2, 200), (3, 50), (4, 300), (5, 10)] {
        z.update(k, s, 1);
    }
    // Order: 4(300), 2(200), 1(100), 3(50), 5(10)
    assert_eq!(keys_in_order(&z), vec![4, 2, 1, 3, 5]);

    assert_eq!(
        z.range(1, 2, false)
            .unwrap()
            .map(|keys| keys.collect::<Vec<_>>()),
        Some(vec![4, 2])
    );
    assert_eq!(
        z.range(1, 3, true)
            .unwrap()
            .map(|keys| keys.collect::<Vec<_>>()),
        Some(vec![5, 3, 1])
    );
    // Negative indices follow the original C++ semantics: the argument is
    // first decremented (1-based -> 0-based) and then offset by llen, so
    // range(-2, -1) maps to 0-based [2, 3] => ranks 3 and 4 => keys 1, 3.
    assert_eq!(
        z.range(-2, -1, false)
            .unwrap()
            .map(|keys| keys.collect::<Vec<_>>()),
        Some(vec![1, 3])
    );
    // out of range -> None
    assert_eq!(
        z.range(10, 20, false)
            .unwrap()
            .map(|keys| keys.collect::<Vec<_>>()),
        None
    );
    // clamped end: start=4 (0-based 3) through end -> keys at ranks 4,5
    assert_eq!(
        z.range(4, 100, false)
            .unwrap()
            .map(|keys| keys.collect::<Vec<_>>()),
        Some(vec![3, 5])
    );
}

#[test]
fn ignores_zero_key_and_zero_capacity() {
    let mut z = ZSet::new(usize::MAX, false);
    z.update(0, 100, 1);
    assert_eq!(z.size(), 0);

    let mut z0 = ZSet::new(0, false);
    z0.update(1, 100, 1);
    assert_eq!(z0.size(), 0);
}

#[test]
fn large_set_rank_consistency() {
    let mut z = ZSet::new(usize::MAX, false);
    let n = 2000i64;
    for k in 1..=n {
        z.update(k, k, 1); // score == key
    }
    assert_eq!(z.size(), n as usize);
    // Highest score (== n) is rank 1.
    assert_eq!(z.rank(n), 1);
    assert_eq!(z.rank(1), n as usize);
    assert_eq!(z.key_by_rank(1), Some(n));
    assert_eq!(z.key_by_rank(n as usize), Some(1));
}

/// Pure-Rust CPU flamegraph of the skiplist, with zero Lua/FFI in the
/// stacks. Ignored by default; run explicitly with an optimized profile:
///
/// ```text
/// cargo test -p moon-runtime --profile profiling \
///     lua_zset::tests::profile_flamegraph -- --ignored --nocapture
/// ```
///
/// Writes one SVG per operation under `target/profile/`. Open them in a
/// browser (zoomable / searchable). Tune size via env `ZSET_N` / `ZSET_OPS`.
#[test]
#[ignore = "manual profiling; emits flamegraph SVGs"]
fn profile_flamegraph() {
    use std::io::Write;

    let n: i64 = std::env::var("ZSET_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let ops: usize = std::env::var("ZSET_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);

    // Local xorshift so key selection cost stays out of the skiplist frames.
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move || {
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        seed.wrapping_mul(0x2545_F491_4F6C_DD1D)
    };

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/profile");
    std::fs::create_dir_all(&out_dir).unwrap();

    // Run `body` under a fresh process-wide sampling profiler and write an SVG.
    let profile = |name: &str, body: &mut dyn FnMut()| {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(1000)
            .blocklist(&["libc", "libgcc", "pthread", "vdso", "libdyld"])
            .build()
            .expect("start profiler");
        body();
        let report = guard.report().build().expect("build report");
        let path = out_dir.join(format!("zset_{name}.svg"));
        let file = std::fs::File::create(&path).unwrap();
        report.flamegraph(file).expect("write flamegraph");
        println!(
            "[flamegraph] {name:14} -> {}",
            path.canonicalize().unwrap().display()
        );
    };

    let mut z = ZSet::new(usize::MAX, false);

    profile("build_insert", &mut || {
        for k in 1..=n {
            let s = (next() % 1_000_000) as i64;
            z.update(k, s, 1);
        }
    });

    profile("rank", &mut || {
        let mut acc = 0usize;
        for _ in 0..ops {
            let k = (next() % n as u64) as i64 + 1;
            acc = acc.wrapping_add(z.rank(k));
        }
        std::hint::black_box(acc);
    });

    profile("key_by_rank", &mut || {
        let mut acc = 0i64;
        for _ in 0..ops {
            let r = (next() % n as u64) as usize + 1;
            acc = acc.wrapping_add(z.key_by_rank(r).unwrap_or(0));
        }
        std::hint::black_box(acc);
    });

    profile("update_reposition", &mut || {
        for _ in 0..ops {
            let k = (next() % n as u64) as i64 + 1;
            let s = (next() % 1_000_000) as i64;
            z.update(k, s, 1);
        }
    });

    // erase + reinsert keeps the set size stable across the loop.
    profile("erase_reinsert", &mut || {
        for _ in 0..ops {
            let k = (next() % n as u64) as i64 + 1;
            let s = z.score(k);
            if z.erase(k) == 1 {
                z.update(k, s, 1);
            }
        }
    });

    std::io::stdout().flush().ok();
    assert_eq!(z.size(), n as usize);
}

#[test]
fn range_iterator_tracks_remaining_length_and_extreme_bounds() {
    let mut z = ZSet::new(10, false);
    z.update(1, 3, 0);
    z.update(2, 2, 0);
    z.update(3, 1, 0);
    let mut keys = z.range(i64::MIN, i64::MAX, false).unwrap().unwrap();
    assert_eq!(keys.len(), 3);
    assert_eq!(keys.next(), Some(1));
    assert_eq!(keys.len(), 2);
    assert_eq!(keys.collect::<Vec<_>>(), [2, 3]);
    assert!(z.range(i64::MAX, i64::MIN, true).unwrap().is_none());
    assert_eq!(
        z.range(1, i64::MAX, true)
            .unwrap()
            .unwrap()
            .collect::<Vec<_>>(),
        [3, 2, 1]
    );
}
