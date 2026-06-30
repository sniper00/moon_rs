//! Criterion port of the reference C++ `aoi/stress_aoi.cpp` suite (S1..S25).
//!
//! Each C++ stress test mixed setup + a timed workload + console reporting.
//! Here the *timed workload* of every scenario becomes a Criterion benchmark:
//! the untimed pre-state is built in an `iter_batched` setup closure
//! (`BatchSize::PerIteration`, so large maps are not duplicated), and the
//! measured routine performs exactly the operations the C++ version timed.
//! `Throughput::Elements(N)` is set so Criterion reports the same ops/s figure
//! the C++ suite printed.
//!
//! Run:  cargo bench -p moon-game
//! One:  cargo bench -p moon-game -- S6

use std::collections::HashSet;
use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};

use moon_game::aoi::{
    Aoi, ENABLE_LEAVE_EVENT, FIXED as F, Handle, MARKER as M, WATCHER as W,
};
use moon_game::math::{Rect, Vec2};

const WM: i32 = W | M;
const MF: i32 = M | F;

/// Tiny deterministic xorshift64* RNG. We don't need to reproduce the exact
/// values of C++ `std::mt19937` (impossible across implementations); we only
/// need a stable, cheap pseudo-random stream so each run hits the same tiles.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    #[inline]
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    /// `randi(rng, n)` equivalent: uniform in `[0, n)`.
    #[inline]
    fn range(&mut self, n: i32) -> i32 {
        (self.next_u64() % n as u64) as i32
    }
    /// Uniform float in `[0, max)`.
    #[inline]
    fn rangef(&mut self, max: f32) -> f32 {
        (self.next_u64() >> 11) as f32 / (1u64 << 53) as f32 * max
    }
}

#[inline]
#[allow(clippy::too_many_arguments)] // mirrors the C++ `insert(handle, x, y, w, h, layer, mode)`
fn ins(a: &mut Aoi, h: Handle, x: i32, y: i32, w: i32, hh: i32, layer: i32, mode: i32) {
    a.insert(h, x, y, w, hh, layer, mode, 0);
}

#[inline]
fn clampi(v: i32, lo: i32, hi: i32) -> i32 {
    v.clamp(lo, hi)
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("aoi_stress");

    // ── S1. Mass insert: 1000 watchers + 5000 markers ───────────────────────
    g.throughput(Throughput::Elements(6000));
    g.bench_function("S1_mass_insert_1000W_5000M", |b| {
        b.iter_batched(
            || (Aoi::new(0, 0, 10000, 100), Rng::new(42)),
            |(mut aoi, mut rng)| {
                for i in 0..1000 {
                    ins(&mut aoi, i, rng.range(10000), rng.range(10000), 200, 200, 0, W);
                }
                for i in 0..5000 {
                    ins(&mut aoi, 10000 + i, rng.range(10000), rng.range(10000), 0, 0, 0, M);
                }
                black_box(aoi.size());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S2. 500 W|M mutual visibility ───────────────────────────────────────
    g.throughput(Throughput::Elements(500));
    g.bench_function("S2_mutual_visibility_500", |b| {
        b.iter_batched(
            || Aoi::new(0, 0, 5000, 50),
            |mut aoi| {
                for i in 1..=500i32 {
                    let x = 2500 + (i % 50);
                    let y = 2500 + (i / 50);
                    aoi.clear_event();
                    ins(&mut aoi, i as i64, x, y, 300, 300, 0, WM);
                }
                black_box(aoi.size());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S3. Tick simulation: 200W + 1000M, 50 ticks of marker movement ──────
    g.throughput(Throughput::Elements(50 * 1000));
    g.bench_function("S3_tick_sim_50x1000_marker_moves", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 5000, 100);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                let mut rng = Rng::new(123);
                for i in 0..200 {
                    ins(&mut aoi, i, rng.range(5000), rng.range(5000), 400, 400, 0, W);
                }
                for i in 0..1000 {
                    ins(&mut aoi, 10000 + i, rng.range(5000), rng.range(5000), 0, 0, 0, M);
                }
                (aoi, rng)
            },
            |(mut aoi, mut rng)| {
                for _tick in 0..50 {
                    aoi.clear_event();
                    for i in 0..1000 {
                        let m = *aoi.find(10000 + i).unwrap();
                        let nx = clampi(m.x + rng.range(41) - 20, 0, 4999);
                        let ny = clampi(m.y + rng.range(41) - 20, 0, 4999);
                        aoi.update(10000 + i, nx, ny, 0, 0, 0);
                    }
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S4. Watcher full-map sweep over 200 markers ─────────────────────────
    g.throughput(Throughput::Elements(20 * 100));
    g.bench_function("S4_watcher_full_map_sweep", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 1000, 50);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                for i in 0..200i32 {
                    ins(&mut aoi, (1000 + i) as i64, (i * 47) % 1000, (i * 71) % 1000, 0, 0, 0, M);
                }
                ins(&mut aoi, 1, 0, 0, 200, 200, 0, W);
                aoi
            },
            |mut aoi| {
                let mut y = 0;
                while y < 1000 {
                    let mut x = 0;
                    while x < 1000 {
                        aoi.clear_event();
                        aoi.update(1, x, y, 200, 200, 0);
                        x += 10;
                    }
                    y += 50;
                }
                black_box(aoi.size());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S5. Rapid insert+erase, 10000 cycles ────────────────────────────────
    g.throughput(Throughput::Elements(10000));
    g.bench_function("S5_rapid_insert_erase_10000", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 2000, 50);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                for i in 1..=10 {
                    ins(&mut aoi, i, 1000, 1000, 400, 400, 0, W);
                }
                aoi
            },
            |mut aoi| {
                for i in 0..10000i32 {
                    let id = 50000i64 + i as i64;
                    aoi.clear_event();
                    ins(&mut aoi, id, 1000 + (i % 100) - 50, 1000 + (i % 100) - 50, 0, 0, 0, M);
                    aoi.erase(id, true);
                }
                black_box(aoi.size());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S6. 100 overlapping watchers, 1 marker on a circular path ───────────
    g.throughput(Throughput::Elements(200));
    g.bench_function("S6_100_watchers_marker_orbit", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 2000, 50);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                for i in 1..=100i32 {
                    ins(&mut aoi, i as i64, 1000 + (i % 10) * 5, 1000 + (i / 10) * 5, 600, 600, 0, W);
                }
                ins(&mut aoi, 500, 1000, 1000, 0, 0, 0, M);
                aoi
            },
            |mut aoi| {
                for step in 0..200 {
                    aoi.clear_event();
                    let x = 1000 + (50.0 * (step as f64 * 0.1).sin()) as i32;
                    let y = 1000 + (50.0 * (step as f64 * 0.1).cos()) as i32;
                    aoi.update(500, x, y, 0, 0, 0);
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S7. Dense + sparse region migration ─────────────────────────────────
    g.throughput(Throughput::Elements(100));
    g.bench_function("S7_dense_sparse_migration", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 10000, 100);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                for i in 1..=200i32 {
                    ins(&mut aoi, i as i64, 500 + (i % 20) * 25, 500 + (i / 20) * 25, 300, 300, 0, WM);
                }
                for i in 0..50i32 {
                    ins(&mut aoi, (5000 + i) as i64, 3000 + i * 100, 5000, 0, 0, 0, M);
                }
                aoi
            },
            |mut aoi| {
                for i in 1..=50i32 {
                    aoi.clear_event();
                    aoi.update(i as i64, 3000 + i * 100, 5000, 300, 300, 0);
                }
                for i in 1..=50i32 {
                    aoi.clear_event();
                    aoi.update(i as i64, 500 + (i % 20) * 25, 500 + (i / 20) * 25, 300, 300, 0);
                }
                black_box(aoi.size());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S8. Mass hide/show toggle: 50W + 500M, 5 rounds ─────────────────────
    g.throughput(Throughput::Elements(5 * 1000));
    g.bench_function("S8_hide_show_toggle", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 5000, 100);
                for i in 1..=50 {
                    ins(&mut aoi, i, 2500, 2500, 1000, 1000, 0, W);
                }
                for i in 0..500i32 {
                    ins(&mut aoi, (1000 + i) as i64, 2000 + (i % 50) * 20, 2000 + (i / 50) * 20, 0, 0, 0, M);
                }
                aoi
            },
            |mut aoi| {
                for _round in 0..5 {
                    aoi.clear_event();
                    for i in 0..500 {
                        aoi.set_hide(1000 + i, true);
                    }
                    aoi.clear_event();
                    for i in 0..500 {
                        aoi.set_hide(1000 + i, false);
                    }
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S9. Watcher view oscillation: 100 markers, 50 zoom cycles ───────────
    g.throughput(Throughput::Elements(100));
    g.bench_function("S9_view_oscillation", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 2000, 50);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                ins(&mut aoi, 1, 1000, 1000, 400, 400, 0, W);
                for i in 0..100i32 {
                    let dist = 50 + i * 5;
                    let angle = i as f64 * 0.618 * 2.0 * std::f64::consts::PI;
                    let x = clampi(1000 + (dist as f64 * angle.cos()) as i32, 0, 1999);
                    let y = clampi(1000 + (dist as f64 * angle.sin()) as i32, 0, 1999);
                    ins(&mut aoi, (1000 + i) as i64, x, y, 0, 0, 0, M);
                }
                aoi
            },
            |mut aoi| {
                for _cycle in 0..50 {
                    aoi.clear_event();
                    aoi.update(1, 1000, 1000, 800, 800, 0);
                    aoi.clear_event();
                    aoi.update(1, 1000, 1000, 100, 100, 0);
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S10. Mass query: 5000 markers, 1000 box queries ─────────────────────
    g.throughput(Throughput::Elements(1000));
    g.bench_function("S10_mass_query", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 10000, 100);
                for i in 0..5000i32 {
                    ins(&mut aoi, i as i64, (i * 73) % 10000, (i * 137) % 10000, 0, 0, 0, M);
                }
                (aoi, Rng::new(99))
            },
            |(aoi, mut rng)| {
                let mut total = 0usize;
                for _q in 0..1000 {
                    let qx = rng.range(10000);
                    let qy = rng.range(10000);
                    let qr = Rect::new(qx - 250, qy - 250, 500, 500);
                    let mut found: HashSet<Handle> = HashSet::new();
                    aoi.query(qx, qy, 500, 500, |h, is_edge| {
                        if is_edge {
                            let m = aoi.find(h).unwrap();
                            if !qr.contains_point(m.x, m.y) {
                                return;
                            }
                        }
                        found.insert(h);
                    });
                    total += found.len();
                }
                black_box(total);
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S11. Mass raycast: 200 fixed obstacles, 500 rays ────────────────────
    g.throughput(Throughput::Elements(500));
    g.bench_function("S11_mass_raycast", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 5000, 100);
                let mut rng = Rng::new(77);
                for i in 0..200 {
                    ins(&mut aoi, i, 100 + rng.range(4800), 100 + rng.range(4800), 30, 30, 0, MF);
                }
                (aoi, rng)
            },
            |(aoi, mut rng)| {
                let mut hits = 0usize;
                for _r in 0..500 {
                    let s = Vec2::new(rng.rangef(4999.0), rng.rangef(4999.0));
                    let e = Vec2::new(rng.rangef(4999.0), rng.rangef(4999.0));
                    if aoi.raycast(s, e, 10.0).is_some() {
                        hits += 1;
                    }
                }
                black_box(hits);
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S12. Layered objects: 10 layers x 50 markers, 10 layer switches ─────
    g.throughput(Throughput::Elements(10));
    g.bench_function("S12_layered_switches", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 5000, 100);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                for layer in 0..10 {
                    for i in 0..50 {
                        ins(
                            &mut aoi,
                            layer as i64 * 1000 + i,
                            2500 + ((i as i32) % 10) * 30,
                            2500 + ((i as i32) / 10) * 30,
                            0,
                            0,
                            layer,
                            M,
                        );
                    }
                }
                ins(&mut aoi, 99999, 2500, 2500, 800, 800, 0, W);
                aoi
            },
            |mut aoi| {
                for nl in 1..=10 {
                    aoi.clear_event();
                    aoi.update(99999, 2500, 2500, 800, 800, nl);
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S13. All objects moving: 100 W|M, 30 ticks ──────────────────────────
    g.throughput(Throughput::Elements(30 * 100));
    g.bench_function("S13_all_moving_100WM", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 5000, 100);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                let mut rng = Rng::new(256);
                for i in 1..=100 {
                    ins(&mut aoi, i, rng.range(5000), rng.range(5000), 300, 300, 0, WM);
                }
                (aoi, rng)
            },
            |(mut aoi, mut rng)| {
                for _tick in 0..30 {
                    aoi.clear_event();
                    for i in 1..=100 {
                        let o = *aoi.find(i).unwrap();
                        let nx = clampi(o.x + rng.range(61) - 30, 0, 4999);
                        let ny = clampi(o.y + rng.range(61) - 30, 0, 4999);
                        aoi.update(i, nx, ny, 300, 300, 0);
                    }
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S14. Fixed marker version stress: insert/update/erase 100 ───────────
    g.throughput(Throughput::Elements(300));
    g.bench_function("S14_fixed_version_stress", |b| {
        b.iter_batched(
            || Aoi::new(0, 0, 2000, 100),
            |mut aoi| {
                for i in 0..100 {
                    ins(&mut aoi, i, 550, 550, 0, 0, 0, MF);
                }
                for i in 0..100 {
                    aoi.update(i, 551, 551, 0, 0, 0);
                }
                for i in 0..100 {
                    aoi.erase(i, true);
                }
                black_box(aoi.size());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S15. Range marker coverage: insert+erase 50 large markers ───────────
    g.throughput(Throughput::Elements(100));
    g.bench_function("S15_range_marker_coverage", |b| {
        b.iter_batched(
            || Aoi::new(0, 0, 5000, 100),
            |mut aoi| {
                for i in 0..50i32 {
                    ins(&mut aoi, i as i64, 500 + i * 80, 2500, 400, 400, 0, M);
                }
                for i in 0..50 {
                    aoi.erase(i, true);
                }
                black_box(aoi.size());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S16. Enter/leave pairing: 50 markers, 100 ticks, then erase ─────────
    g.throughput(Throughput::Elements(100 * 50));
    g.bench_function("S16_enter_leave_pairing", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 2000, 50);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                ins(&mut aoi, 1, 1000, 1000, 400, 400, 0, W);
                for i in 0..50i32 {
                    ins(&mut aoi, (1000 + i) as i64, 100 + i * 30, 100, 0, 0, 0, M);
                }
                aoi
            },
            |mut aoi| {
                for tick in 0..100i32 {
                    aoi.clear_event();
                    for i in 0..50i32 {
                        let (x, y) = if tick < 50 {
                            (100 + i * 30 + tick * 20, 100 + tick * 20)
                        } else {
                            (100 + i * 30 + (100 - tick) * 20, 100 + (100 - tick) * 20)
                        };
                        aoi.update((1000 + i) as i64, clampi(x, 0, 1999), clampi(y, 0, 1999), 0, 0, 0);
                    }
                }
                aoi.clear_event();
                for i in 0..50 {
                    aoi.erase(1000 + i, true);
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S17. Large map, small tiles (1,000,000 tiles): watcher sweep ────────
    g.throughput(Throughput::Elements(20));
    g.bench_function("S17_large_map_small_tiles", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 10000, 10);
                ins(&mut aoi, 1, 5000, 5000, 100, 100, 0, W);
                ins(&mut aoi, 2, 5010, 5010, 0, 0, 0, M);
                aoi
            },
            |mut aoi| {
                let mut x = 0;
                while x < 10000 {
                    aoi.clear_event();
                    aoi.update(1, x, 5000, 100, 100, 0);
                    x += 500;
                }
                black_box(aoi.size());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S18. Repeated clear+rebuild: 200 objects x 20 rounds ────────────────
    g.throughput(Throughput::Elements(20 * 200));
    g.bench_function("S18_clear_rebuild", |b| {
        b.iter_batched(
            || Aoi::new(0, 0, 2000, 50),
            |mut aoi| {
                for _round in 0..20 {
                    for i in 0..200i32 {
                        ins(&mut aoi, i as i64, (i * 37) % 2000, (i * 71) % 2000, 200, 200, 0, WM);
                    }
                    aoi.clear();
                }
                black_box(aoi.size());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S19. Teleport storm: 300 W|M, 50 rounds of random teleports ─────────
    g.throughput(Throughput::Elements(50 * 300));
    g.bench_function("S19_teleport_storm", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 5000, 100);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                let mut rng = Rng::new(999);
                for i in 0..300 {
                    ins(&mut aoi, i, rng.range(5000), rng.range(5000), 300, 300, 0, WM);
                }
                (aoi, rng)
            },
            |(mut aoi, mut rng)| {
                for _round in 0..50 {
                    aoi.clear_event();
                    for i in 0..300 {
                        aoi.update(i, rng.range(5000), rng.range(5000), 300, 300, 0);
                    }
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S20. for_each_all over 800 objects (marker + watcher filters) ───────
    g.throughput(Throughput::Elements(800 * 2));
    g.bench_function("S20_for_each_all", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 5000, 100);
                for i in 0..500i32 {
                    ins(&mut aoi, i as i64, (i * 41) % 5000, (i * 67) % 5000, 0, 0, 0, M);
                }
                for i in 500..700i32 {
                    ins(&mut aoi, i as i64, (i * 31) % 5000, (i * 53) % 5000, 200, 200, 0, W);
                }
                for i in 700..800i32 {
                    ins(&mut aoi, i as i64, (i * 23) % 5000, (i * 47) % 5000, 200, 200, 0, WM);
                }
                aoi
            },
            |aoi| {
                let mut markers: HashSet<Handle> = HashSet::new();
                let mut watchers: HashSet<Handle> = HashSet::new();
                aoi.for_each_all(|h, _, _, _, _| {
                    markers.insert(h);
                }, M);
                aoi.for_each_all(|h, _, _, _, _| {
                    watchers.insert(h);
                }, W);
                black_box(markers.len() + watchers.len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S21. Dense scene: 1W + 1000M insert (enter) then erase (leave) ──────
    g.throughput(Throughput::Elements(2000));
    g.bench_function("S21_dense_insert_erase", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 2000, 50);
                ins(&mut aoi, 1, 1000, 1000, 2000, 2000, 0, W);
                aoi
            },
            |mut aoi| {
                aoi.clear_event();
                for i in 0..1000i32 {
                    ins(&mut aoi, (2000 + i) as i64, i % 2000, (i * 3) % 2000, 0, 0, 0, M);
                }
                aoi.set_option(ENABLE_LEAVE_EVENT);
                aoi.clear_event();
                for i in 0..1000 {
                    aoi.erase(2000 + i, true);
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S22. Mixed view-size watchers: 100W + 200M, 30 ticks ────────────────
    g.throughput(Throughput::Elements(30 * 100));
    g.bench_function("S22_mixed_view_sizes", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 5000, 100);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                for i in 0..100i32 {
                    ins(&mut aoi, i as i64, 2500, 2500, 50 + i * 10, 50 + i * 10, 0, W);
                }
                for i in 0..200i32 {
                    ins(&mut aoi, (1000 + i) as i64, 2000 + (i * 17) % 1000, 2000 + (i * 31) % 1000, 0, 0, 0, M);
                }
                (aoi, Rng::new(88))
            },
            |(mut aoi, mut rng)| {
                for _tick in 0..30 {
                    aoi.clear_event();
                    for i in 0..100 {
                        let w = *aoi.find(i).unwrap();
                        let nx = clampi(w.x + rng.range(101) - 50, 0, 4999);
                        let ny = clampi(w.y + rng.range(101) - 50, 0, 4999);
                        aoi.update(i, nx, ny, w.w, w.h, 0);
                    }
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S23. Tiny tiles (100x100, tile=1): 99 watcher moves ─────────────────
    g.throughput(Throughput::Elements(99));
    g.bench_function("S23_tiny_tiles", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(0, 0, 100, 1);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                ins(&mut aoi, 1, 50, 50, 20, 20, 0, W);
                ins(&mut aoi, 2, 55, 55, 0, 0, 0, M);
                aoi
            },
            |mut aoi| {
                for i in 0..99 {
                    aoi.clear_event();
                    aoi.update(1, i, 50, 20, 20, 0);
                }
                black_box(aoi.size());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S24. Single-tile map: 100 W|M insert + update + clear ───────────────
    g.throughput(Throughput::Elements(200));
    g.bench_function("S24_single_tile_map", |b| {
        b.iter_batched(
            || Aoi::new(0, 0, 1000, 1000),
            |mut aoi| {
                for i in 0..100i32 {
                    ins(&mut aoi, i as i64, i * 9, i * 9, 500, 500, 0, WM);
                }
                aoi.clear_event();
                for i in 0..100i32 {
                    aoi.update(i as i64, 500 + i, 500 + i, 500, 500, 0);
                }
                let ev = aoi.events().len();
                aoi.clear();
                black_box(ev);
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    // ── S25. Non-zero origin (-5000,-5000): 200 W|M, 50 ticks ───────────────
    g.throughput(Throughput::Elements(50 * 200));
    g.bench_function("S25_nonzero_origin", |b| {
        b.iter_batched(
            || {
                let mut aoi = Aoi::new(-5000, -5000, 10000, 100);
                aoi.set_option(ENABLE_LEAVE_EVENT);
                let mut rng = Rng::new(314);
                for i in 0..200 {
                    ins(&mut aoi, i, -5000 + rng.range(10000), -5000 + rng.range(10000), 300, 300, 0, WM);
                }
                (aoi, rng)
            },
            |(mut aoi, mut rng)| {
                for _tick in 0..50 {
                    aoi.clear_event();
                    for i in 0..200 {
                        let o = *aoi.find(i).unwrap();
                        let nx = clampi(o.x + rng.range(101) - 50, -5000, 4999);
                        let ny = clampi(o.y + rng.range(101) - 50, -5000, 4999);
                        aoi.update(i, nx, ny, 300, 300, 0);
                    }
                }
                black_box(aoi.events().len());
                aoi // returned so criterion drops it *outside* the timed region
            },
            BatchSize::PerIteration,
        );
    });

    g.finish();
}

criterion_group! {
    name = benches;
    // Macro-benchmarks: a few samples are enough and keep total runtime sane.
    config = Criterion::default().sample_size(10);
    targets = bench
}
criterion_main!(benches);
