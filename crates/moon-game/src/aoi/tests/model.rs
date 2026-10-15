//! Deliberately slow reference model: enumerate all objects, without using
//! grid geometry, tile membership, or the production visibility predicates.
use super::*;

#[derive(Clone, Copy)]
struct Entry {
    obj: AoiObject,
    present: bool,
    attached: bool,
}

fn bounds(o: &AoiObject, origin: (i32, i32)) -> Option<[i64; 4]> {
    let (ox, oy) = (i64::from(origin.0), i64::from(origin.1));
    let left = (i64::from(o.x) - i64::from(o.w / 2)).clamp(ox, ox + 100);
    let right = (i64::from(o.x) + i64::from(o.w / 2)).clamp(ox, ox + 100);
    let bottom = (i64::from(o.y) - i64::from(o.h / 2)).clamp(oy, oy + 100);
    let top = (i64::from(o.y) + i64::from(o.h / 2)).clamp(oy, oy + 100);
    (left < right && bottom < top).then_some([left, bottom, right, top])
}

fn geometry(w: &AoiObject, m: &AoiObject, origin: (i32, i32)) -> bool {
    let Some([l, b, r, t]) = bounds(w, origin) else {
        return false;
    };
    if m.mode & W != 0 || m.w == 0 || m.h == 0 {
        l <= i64::from(m.x) && i64::from(m.x) < r && b <= i64::from(m.y) && i64::from(m.y) < t
    } else {
        bounds(m, origin).is_some_and(|[ml, mb, mr, mt]| l < mr && ml < r && b < mt && mb < t)
    }
}

fn visible(w: &Entry, m: &Entry, origin: (i32, i32)) -> bool {
    w.present
        && m.present
        && w.attached
        && m.attached
        && w.obj.mode & W != 0
        && m.obj.mode & M != 0
        && m.obj.mode & H == 0
        && w.obj.layer <= m.obj.layer
        && geometry(&w.obj, &m.obj, origin)
}

struct Rng(u64);
impl Rng {
    fn next(&mut self, n: u64) -> i32 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((self.0 >> 32) % n) as i32
    }
}

#[test]
fn randomized_lifecycle_matches_full_visibility_model() {
    for seed in 1..=24 {
        let origin = [(0, 0), (10, -10), (-23, 17)][seed as usize % 3];
        let leave = seed % 2 == 0;
        let self_events = seed % 3 == 0;
        let mut rng = Rng(seed);
        let tile_size = [2, 5, 10, 20, 25, 50][seed as usize % 6];
        let mut a = Aoi::new(origin.0, origin.1, 100, tile_size);
        a.set_option(
            if leave { ENABLE_LEAVE_EVENT } else { 0 }
                | if self_events { ENABLE_SELF_EVENT } else { 0 },
        );
        let mut objects = Vec::new();
        for handle in 1..=24 {
            let obj = AoiObject {
                handle,
                user: handle * 10,
                x: origin.0 + rng.next(100),
                y: origin.1 + rng.next(100),
                w: rng.next(81),
                h: rng.next(81),
                layer: rng.next(4),
                mode: [W, M, W | M, M | F, M | H][rng.next(5) as usize],
            };
            assert!(a.insert_object(obj));
            objects.push(Entry {
                obj,
                present: true,
                attached: true,
            });
        }
        for step in 0..500 {
            a.clear_event();
            let before = objects.clone();
            let index = rng.next(24) as usize;
            let old = before[index];
            let handle = old.obj.handle;
            let mut cause = rng.next(11);
            let entry = &mut objects[index];
            let changed;
            match cause {
                0..=2 | 10 => {
                    let (mut x, mut y, mut w, mut h, mut layer) =
                        (old.obj.x, old.obj.y, old.obj.w, old.obj.h, old.obj.layer);
                    match cause {
                        0 => {
                            x = origin.0 + rng.next(140) - 20;
                            y = origin.1 + rng.next(140) - 20;
                        }
                        1 => {
                            w = rng.next(81);
                            h = rng.next(81);
                        }
                        2 => layer = rng.next(4),
                        _ => {
                            x = origin.0 + rng.next(140) - 20;
                            y = origin.1 + rng.next(140) - 20;
                            w = rng.next(81);
                            h = rng.next(81);
                            layer = rng.next(4);
                        }
                    }
                    let result = match cause {
                        0 => a.set_position(handle, x, y),
                        1 => a.set_view(handle, w, h),
                        2 => a.set_layer(handle, layer),
                        _ => a.update(handle, x, y, w, h, layer),
                    };
                    assert_eq!(result, old.present);
                    changed = result;
                    if result {
                        entry.obj.x = x.clamp(origin.0, origin.0 + 99);
                        entry.obj.y = y.clamp(origin.1, origin.1 + 99);
                        entry.obj.w = w;
                        entry.obj.h = h;
                        entry.obj.layer = layer;
                        entry.attached = true;
                    }
                    cause = if old.attached { 0 } else { 4 };
                }
                3 => {
                    changed = a.detach(handle);
                    assert_eq!(changed, old.present && old.attached);
                    entry.attached = false;
                }
                4 => {
                    changed = a.attach(handle);
                    assert_eq!(changed, old.present && !old.attached);
                    if changed {
                        entry.attached = true;
                    }
                }
                5 => {
                    changed = old.present && old.obj.mode & M != 0;
                    if changed {
                        entry.obj.mode ^= H;
                        a.set_hide(handle, entry.obj.mode & H != 0);
                    }
                }
                6 => {
                    changed = a.remove(handle);
                    assert_eq!(changed, old.present);
                    entry.present = false;
                    entry.attached = false;
                }
                7 => {
                    let mut obj = old.obj;
                    obj.x = obj.x.clamp(origin.0, origin.0 + 99);
                    obj.y = obj.y.clamp(origin.1, origin.1 + 99);
                    changed = a.insert_object(obj);
                    assert_eq!(changed, !old.present);
                    if changed {
                        entry.obj = obj;
                        entry.present = true;
                        entry.attached = true;
                    }
                    cause = 4;
                }
                8 => {
                    changed = old.present && old.obj.mode & M != 0;
                    if changed {
                        a.fire_event(handle, 99);
                    }
                }
                _ => {
                    changed = old.present;
                    a.erase(handle, false);
                    if changed {
                        entry.attached = false;
                        entry.obj.x = 0;
                        entry.obj.y = 0;
                        entry.obj.w = 0;
                        entry.obj.h = 0;
                        entry.obj.layer = 0;
                    }
                    cause = 3;
                }
            }
            if cause == 4 && changed {
                // attach also clamps legacy soft-deleted zero coordinates.
                entry.obj.x = entry.obj.x.clamp(origin.0, origin.0 + 99);
                entry.obj.y = entry.obj.y.clamp(origin.1, origin.1 + 99);
            }
            let mut expected = Vec::new();
            if changed {
                for other in 0..objects.len() {
                    if other == index {
                        continue;
                    }
                    let was = visible(&before[other], &old, origin);
                    let is = visible(&objects[other], &objects[index], origin);
                    // Notifications emitted by the changed marker.
                    let event = match cause {
                        0 if !was && is => Some(EVENT_ENTER),
                        0 if was && !is && leave => Some(EVENT_LEAVE),
                        0 if was && is => Some(EVENT_POS),
                        3 | 6 if was => Some(EVENT_LEAVE),
                        4 if is => Some(EVENT_ENTER),
                        5 if was && !is => Some(EVENT_LEAVE),
                        5 if !was && is => Some(EVENT_ENTER),
                        8 if is => Some(99),
                        _ => None,
                    };
                    if let Some(id) = event {
                        expected.push((id, objects[other].obj.handle, handle));
                    }
                    // Fixed markers intentionally do not generate events from
                    // watcher changes; marker-triggered events above still do.
                    if objects[other].obj.mode & F != 0 {
                        continue;
                    }
                    let was = visible(&old, &before[other], origin);
                    let is = visible(&objects[index], &objects[other], origin);
                    let layer_leave = geometry(&objects[index].obj, &objects[other].obj, origin);
                    let event = match cause {
                        0 if !was && is => Some(EVENT_ENTER),
                        0 if was && !is && (leave || layer_leave) => Some(EVENT_LEAVE),
                        4 if is => Some(EVENT_ENTER),
                        _ => None,
                    };
                    if let Some(id) = event {
                        expected.push((id, handle, objects[other].obj.handle));
                    }
                }
            }
            if self_events && changed {
                let now = objects[index];
                let was = visible(&old, &old, origin);
                let is = visible(&now, &now, origin);
                let event = match cause {
                    0 if !was && is => Some(EVENT_ENTER),
                    0 if was && !is && leave => Some(EVENT_LEAVE),
                    0 if was && is => Some(EVENT_POS),
                    3 | 6 if was => Some(EVENT_LEAVE),
                    4 if is && now.obj.mode & F == 0 => Some(EVENT_ENTER),
                    5 if was && !is => Some(EVENT_LEAVE),
                    5 if !was && is => Some(EVENT_ENTER),
                    8 if is => Some(99),
                    _ => None,
                };
                if let Some(id) = event {
                    expected.push((id, handle, handle));
                }
            }
            let mut actual: Vec<_> = a
                .events()
                .iter()
                .map(|e| (e.eventid, e.watcher, e.marker))
                .collect();
            actual.sort_unstable();
            expected.sort_unstable();
            assert_eq!(
                actual, expected,
                "seed={seed} step={step} cause={cause} handle={handle}"
            );
            assert_eq!(
                a.objects().count(),
                objects.iter().filter(|o| o.present).count()
            );
            for entry in &objects {
                assert_eq!(
                    a.find(entry.obj.handle).copied(),
                    entry.present.then_some(entry.obj)
                );
                assert_eq!(
                    a.is_attached(entry.obj.handle),
                    entry.present && entry.attached
                );
            }
            let query = AoiObject {
                x: origin.0 + 50,
                y: origin.1 + 50,
                w: 50,
                h: 50,
                layer: 0,
                mode: W,
                handle: 0,
                user: 0,
            };
            let mut expected_query: Vec<_> = objects
                .iter()
                .filter(|m| {
                    m.present
                        && m.attached
                        && m.obj.mode & M != 0
                        && geometry(&query, &m.obj, origin)
                })
                .map(|m| m.obj.handle)
                .collect();
            let mut actual_query = Vec::new();
            a.query_rect(Rect::new(origin.0 + 25, origin.1 + 25, 50, 50), |m| {
                actual_query.push(m.handle)
            });
            expected_query.sort_unstable();
            actual_query.sort_unstable();
            assert_eq!(
                actual_query, expected_query,
                "query seed={seed} step={step}"
            );
        }
    }
}

#[test]
fn sampled_rays_match_all_fixed_footprints() {
    for seed in 1..=6 {
        let origin = (-23, 17);
        let mut a = Aoi::new(origin.0, origin.1, 100, 10);
        let mut rng = Rng(seed);
        let mut objects = Vec::new();
        for handle in 1..=24 {
            let obj = AoiObject {
                handle,
                user: 0,
                layer: 0,
                x: origin.0 + rng.next(100),
                y: origin.1 + rng.next(100),
                w: rng.next(40),
                h: rng.next(40),
                mode: [M | F, W | M | F, M | F | H, M][rng.next(4) as usize],
            };
            assert!(a.insert_object(obj));
            objects.push(obj);
        }
        for row in 0..100 {
            let y = (origin.1 + row) as f32;
            for reverse in [false, true] {
                let (start, end, direction) = if reverse {
                    ((origin.0 + 99) as f32, origin.0 as f32, -1.0)
                } else {
                    (origin.0 as f32, (origin.0 + 99) as f32, 1.0)
                };
                let mut earliest = Vec::new();
                for sample in 0..198 {
                    let x = start + direction * sample as f32 * 0.5;
                    for o in &objects {
                        if o.mode & F != 0
                            && x >= o.x as f32
                            && x < (o.x + o.w) as f32
                            && y >= o.y as f32
                            && y < (o.y + o.h) as f32
                        {
                            earliest.push(o.handle);
                        }
                    }
                    if !earliest.is_empty() {
                        break;
                    }
                }
                let actual = a.raycast(Vec2::new(start, y), Vec2::new(end, y), 0.5);
                match actual {
                    Some(handle) => assert!(
                        earliest.contains(&handle),
                        "seed={seed} row={row} reverse={reverse}"
                    ),
                    None => assert!(
                        earliest.is_empty(),
                        "seed={seed} row={row} reverse={reverse}"
                    ),
                }
            }
        }
    }
}

#[test]
fn sampled_rays_match_model_across_tile_boundaries() {
    for origin in [(0, 0), (-23, 17), (35, -61)] {
        for tile_size in [1, 10, 120] {
            let mut a = Aoi::new(origin.0, origin.1, 120, tile_size);
            let mut rng = Rng(71);
            let mut objects = Vec::new();
            for handle in 1..=24 {
                let obj = AoiObject {
                    handle,
                    x: origin.0 + rng.next(120),
                    y: origin.1 + rng.next(120),
                    w: rng.next(40),
                    h: rng.next(40),
                    mode: [M | F, W | M | F, M | F | H, M][rng.next(4) as usize],
                    layer: 0,
                    user: 0,
                };
                assert!(a.insert_object(obj));
                objects.push(obj);
            }
            for ray in 0..200 {
                // Fractional coordinates on both sides of zero, starts outside
                // the map, arbitrary directions and steps larger than a tile.
                let start = Vec2::new(
                    origin.0 as f32 + rng.next(800) as f32 * 0.25 - 40.0,
                    origin.1 as f32 + rng.next(800) as f32 * 0.25 - 40.0,
                );
                let end = Vec2::new(
                    origin.0 as f32 + rng.next(800) as f32 * 0.25 - 40.0,
                    origin.1 as f32 + rng.next(800) as f32 * 0.25 - 40.0,
                );
                let step = [0.25, 1.0, 10.0, 160.0][ray % 4];
                let direction = (end - start).normalized();
                let mut distance = 0.0;
                let mut earliest = Vec::new();
                while distance < start.distance(&end) {
                    let point = start + direction * distance;
                    for obj in &objects {
                        if obj.mode & F != 0
                            && point.x >= obj.x as f32
                            && point.x < (i64::from(obj.x) + i64::from(obj.w)) as f32
                            && point.y >= obj.y as f32
                            && point.y < (i64::from(obj.y) + i64::from(obj.h)) as f32
                        {
                            earliest.push(obj.handle);
                        }
                    }
                    if !earliest.is_empty() {
                        break;
                    }
                    distance += step;
                }
                let actual = a.raycast(start, end, step);
                assert!(
                    match actual {
                        Some(handle) => earliest.contains(&handle),
                        None => earliest.is_empty(),
                    },
                    "origin={origin:?} tile_size={tile_size} ray={ray}: {actual:?}, expected {earliest:?}"
                );
            }
        }
    }
}

#[test]
fn point_visibility_matches_model_at_coordinate_limits() {
    for origin in [
        (0, 0),
        (-23, 17),
        (i32::MIN, i32::MIN),
        (i32::MAX - 100, i32::MAX - 100),
    ] {
        let mut a = Aoi::new(origin.0, origin.1, 100, 10);
        for offset in [0, 1, 50, 99] {
            for size in [0, 1, 2, 3, 100, i32::MAX] {
                let watcher = AoiObject {
                    handle: 1,
                    x: origin.0 + offset,
                    y: origin.1 + offset,
                    w: size,
                    h: size,
                    mode: W,
                    layer: 0,
                    user: 0,
                };
                assert!(a.insert_object(watcher));
                for dx in [0, 1, 49, 50, 51, 98, 99] {
                    for dy in [0, 1, 49, 50, 51, 98, 99] {
                        let marker = AoiObject {
                            handle: 2,
                            x: origin.0 + dx,
                            y: origin.1 + dy,
                            w: 0,
                            h: 0,
                            mode: M,
                            layer: 0,
                            user: 0,
                        };
                        a.clear_event();
                        assert!(a.insert_object(marker));
                        assert_eq!(
                            a.events().len(),
                            usize::from(geometry(&watcher, &marker, origin)),
                            "origin={origin:?} offset={offset} size={size} marker=({dx},{dy})"
                        );
                        assert!(a.remove(2));
                    }
                }
                assert!(a.remove(1));
            }
        }
    }
}
