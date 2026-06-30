//! Tests ported faithfully from the reference C++ suite (`aoi/test_aoi.cpp`).
//! Each test mirrors a `TEST(...)` case so behavior can be cross-checked.

use super::*;
use crate::math::Vec2;

const W: i32 = WATCHER;
const M: i32 = MARKER;
const F: i32 = FIXED;
const H: i32 = HIDE;

/// Insert helper with `user = 0` (the C++ `insert(...)` without extra args).
fn ins(a: &mut Aoi, handle: Handle, x: i32, y: i32, w: i32, h: i32, layer: i32, mode: i32) -> bool {
    a.insert(handle, x, y, w, h, layer, mode, 0)
}

fn has_event(a: &Aoi, eid: i32, watcher: Handle, marker: Handle) -> bool {
    a.events()
        .iter()
        .any(|e| e.eventid == eid && e.watcher == watcher && e.marker == marker)
}

fn count_events(a: &Aoi, eid: i32) -> usize {
    a.events().iter().filter(|e| e.eventid == eid).count()
}

fn count_event(a: &Aoi, eid: i32, watcher: Handle, marker: Handle) -> usize {
    a.events()
        .iter()
        .filter(|e| e.eventid == eid && e.watcher == watcher && e.marker == marker)
        .count()
}

// ── 1. Basic insert / enter ──────────────────────────────────────────────────

#[test]
fn insert_watcher_then_marker_produces_enter() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.clear_event();
    assert!(ins(&mut a, 1, 50, 50, 40, 40, 0, W));
    assert!(a.events().is_empty());

    a.clear_event();
    assert!(ins(&mut a, 2, 55, 55, 0, 0, 0, M));
    assert_eq!(a.events().len(), 1);
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn insert_marker_then_watcher_produces_enter() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.clear_event();
    assert!(ins(&mut a, 2, 55, 55, 0, 0, 0, M));
    assert!(a.events().is_empty());

    a.clear_event();
    assert!(ins(&mut a, 1, 50, 50, 40, 40, 0, W));
    assert_eq!(a.events().len(), 1);
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn insert_marker_outside_view_no_event() {
    let mut a = Aoi::new(0, 0, 100, 10);
    assert!(ins(&mut a, 1, 50, 50, 20, 20, 0, W));
    a.clear_event();
    assert!(ins(&mut a, 2, 10, 10, 0, 0, 0, M));
    assert!(a.events().is_empty());
}

// ── 2. Duplicate / out of bounds ─────────────────────────────────────────────

#[test]
fn insert_duplicate_handle_fails() {
    let mut a = Aoi::new(0, 0, 100, 10);
    assert!(ins(&mut a, 1, 50, 50, 20, 20, 0, W));
    assert!(!ins(&mut a, 1, 60, 60, 20, 20, 0, M));
}

#[test]
fn insert_out_of_bounds_fails() {
    let mut a = Aoi::new(0, 0, 100, 10);
    assert!(!ins(&mut a, 1, -1, 50, 20, 20, 0, W));
    assert!(!ins(&mut a, 2, 50, -1, 20, 20, 0, W));
    assert!(!ins(&mut a, 3, 100, 50, 20, 20, 0, W));
    assert!(!ins(&mut a, 4, 50, 100, 20, 20, 0, W));
}

#[test]
fn insert_at_origin_and_max_edge() {
    let mut a = Aoi::new(0, 0, 100, 10);
    assert!(ins(&mut a, 1, 0, 0, 20, 20, 0, W | M));
    assert!(ins(&mut a, 2, 99, 99, 10, 10, 0, W | M));
}

// ── 3. Erase ─────────────────────────────────────────────────────────────────

#[test]
fn erase_marker_produces_leave() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);

    a.clear_event();
    a.erase(2, true);
    assert_eq!(a.events().len(), 1);
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
    assert!(!a.has_object(2));
}

#[test]
fn erase_watcher_no_event() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);

    a.clear_event();
    a.erase(1, true);
    assert!(a.events().is_empty());
    assert!(!a.has_object(1));
}

#[test]
fn erase_nonexistent_no_crash() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.clear_event();
    a.erase(999, true);
    assert!(a.events().is_empty());
}

#[test]
fn erase_soft_delete() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 0, 0, 0, M);
    a.clear_event();
    a.erase(1, false);
    assert!(a.has_object(1));
    let obj = a.find(1).unwrap();
    assert!(obj.w == 0 && obj.h == 0);
}

// ── 4. Update — marker movement ──────────────────────────────────────────────

#[test]
fn update_marker_enter_view() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 10, 10, 0, 0, 0, M);
    a.clear_event();
    assert!(a.update(2, 50, 50, 0, 0, 0));
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn update_marker_leave_view() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    a.clear_event();
    assert!(a.update(2, 10, 10, 0, 0, 0));
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
}

#[test]
fn update_marker_pos_event() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    a.clear_event();
    assert!(a.update(2, 56, 56, 0, 0, 0));
    assert!(has_event(&a, EVENT_POS, 1, 2));
}

#[test]
fn update_marker_cross_tile() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 60, 60, 0, W);
    ins(&mut a, 2, 45, 45, 0, 0, 0, M);
    a.clear_event();
    assert!(a.update(2, 55, 55, 0, 0, 0));
    assert!(has_event(&a, EVENT_POS, 1, 2));
}

#[test]
fn update_marker_same_tile_pos_event() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 51, 51, 0, 0, 0, M);
    a.clear_event();
    assert!(a.update(2, 52, 52, 0, 0, 0));
    assert!(has_event(&a, EVENT_POS, 1, 2));
    let obj = a.find(2).unwrap();
    assert!(obj.x == 52 && obj.y == 52);
}

// ── 5. Update — watcher movement / view change ───────────────────────────────

#[test]
fn update_watcher_zoom_in() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 50, 50, 60, 60, 0, W);
    ins(&mut a, 2, 25, 25, 0, 0, 0, M);
    a.clear_event();
    assert!(a.update(1, 50, 50, 20, 20, 0));
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
}

#[test]
fn update_watcher_zoom_out() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, W);
    ins(&mut a, 2, 25, 25, 0, 0, 0, M);
    a.clear_event();
    assert!(a.update(1, 50, 50, 60, 60, 0));
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn update_watcher_move_partial_overlap() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 30, 50, 20, 20, 0, W);
    ins(&mut a, 2, 65, 50, 0, 0, 0, M);
    a.clear_event();
    assert!(a.update(1, 60, 50, 20, 20, 0));
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn update_watcher_no_change() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    a.clear_event();
    assert!(a.update(1, 50, 50, 20, 20, 0));
    assert!(a.events().is_empty());
}

#[test]
fn update_negative_dimensions_rejected() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, W);
    assert!(!a.update(1, 50, 50, -1, 20, 0));
    assert!(!a.update(1, 50, 50, 20, -1, 0));
}

#[test]
fn update_nonexistent_returns_false() {
    let mut a = Aoi::new(0, 0, 100, 10);
    assert!(!a.update(999, 50, 50, 20, 20, 0));
}

#[test]
fn update_clamps_to_map_bounds() {
    let mut a = Aoi::new(10, 10, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, W | M);
    a.clear_event();
    assert!(a.update(1, 200, 200, 20, 20, 0));
    let obj = a.find(1).unwrap();
    assert!(obj.x == 109 && obj.y == 109);
}

#[test]
fn update_clamp_below_origin() {
    let mut a = Aoi::new(10, 10, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, W | M);
    assert!(a.update(1, -100, -100, 20, 20, 0));
    let obj = a.find(1).unwrap();
    assert!(obj.x == 10 && obj.y == 10);
}

#[test]
fn update_negative_layer_no_change() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 7, W);
    assert!(a.update(1, 50, 50, 20, 20, -1));
    assert_eq!(a.find(1).unwrap().layer, 7);
}

// ── 6. Layer filtering ───────────────────────────────────────────────────────

#[test]
fn layer_watcher_cannot_see_lower_layer_marker() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.clear_event();
    ins(&mut a, 1, 50, 50, 40, 40, 5, W);
    a.clear_event();
    ins(&mut a, 2, 55, 55, 0, 0, 3, M);
    assert!(a.events().is_empty());
}

#[test]
fn layer_watcher_can_see_same_or_higher_layer() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 3, W);
    a.clear_event();
    ins(&mut a, 2, 55, 55, 0, 0, 3, M);
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
    a.clear_event();
    ins(&mut a, 3, 56, 56, 0, 0, 5, M);
    assert!(has_event(&a, EVENT_ENTER, 1, 3));
}

#[test]
fn layer_change_triggers_events() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 50, 50, 40, 40, 5, W);
    ins(&mut a, 2, 55, 55, 0, 0, 3, M);

    a.clear_event();
    assert!(a.update(1, 50, 50, 40, 40, 2));
    assert!(has_event(&a, EVENT_ENTER, 1, 2));

    a.clear_event();
    assert!(a.update(1, 50, 50, 40, 40, 5));
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
}

// ── 7. Hide ──────────────────────────────────────────────────────────────────

#[test]
fn hide_marker_produces_leave() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    a.clear_event();
    a.set_hide(2, true);
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
}

#[test]
fn show_marker_produces_enter() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    a.clear_event();
    a.set_hide(2, true);
    a.clear_event();
    a.set_hide(2, false);
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn hide_already_hidden_no_event() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    a.set_hide(2, true);
    a.clear_event();
    a.set_hide(2, true);
    assert!(a.events().is_empty());
}

#[test]
fn hidden_marker_insert_no_event() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    a.clear_event();
    ins(&mut a, 2, 55, 55, 0, 0, 0, M | H);
    assert!(a.events().is_empty());
}

// ── 8. Fixed marker — version ────────────────────────────────────────────────

#[test]
fn fixed_marker_updates_tile_version() {
    let mut a = Aoi::new(0, 0, 100, 10);
    let idx = a.get_index(5, 5);
    let v0 = a.get_version(idx);
    ins(&mut a, 1, 55, 55, 0, 0, 0, M | F);
    let v1 = a.get_version(idx);
    assert!(v1 > v0);
    a.erase(1, true);
    let v2 = a.get_version(idx);
    assert!(v2 > v1);
}

#[test]
fn fixed_marker_skipped_in_update_watcher() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 55, 55, 0, 0, 0, M | F);
    a.clear_event();
    ins(&mut a, 2, 50, 50, 40, 40, 0, W);
    assert!(a.events().is_empty());
}

#[test]
fn fixed_hidden_no_version_change() {
    let mut a = Aoi::new(0, 0, 100, 10);
    let idx = a.get_index(5, 5);
    let v0 = a.get_version(idx);
    ins(&mut a, 1, 55, 55, 0, 0, 0, M | F | H);
    let v1 = a.get_version(idx);
    assert_eq!(v1, v0);
}

// ── 9. Query ─────────────────────────────────────────────────────────────────

#[test]
fn query_finds_markers_in_range() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 0, 0, 0, M);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    ins(&mut a, 3, 10, 10, 0, 0, 0, M);

    let query_rect = a.make_rect(50, 50, 20, 20);
    let mut found = Vec::new();
    a.query(50, 50, 20, 20, |mh, is_edge| {
        let obj = *a.find(mh).unwrap();
        if is_edge && !query_rect.contains_point(obj.x, obj.y) {
            return;
        }
        found.push(mh);
    });
    assert!(found.contains(&1));
    assert!(found.contains(&2));
    assert!(!found.contains(&3));
}

#[test]
fn query_empty_area() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 10, 10, 0, 0, 0, M);
    let query_rect = a.make_rect(80, 80, 10, 10);
    let mut found = Vec::new();
    a.query(80, 80, 10, 10, |mh, is_edge| {
        let obj = *a.find(mh).unwrap();
        if is_edge && !query_rect.contains_point(obj.x, obj.y) {
            return;
        }
        found.push(mh);
    });
    assert!(found.is_empty());
}

#[test]
fn query_zero_size_returns_nothing() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 0, 0, 0, M);
    let mut found = Vec::new();
    a.query(50, 50, 0, 0, |mh, _| found.push(mh));
    assert!(found.is_empty());
}

// ── 10. fire_event ───────────────────────────────────────────────────────────

#[test]
fn fire_event_by_handle() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    a.clear_event();
    a.fire_event(2, EVENT_ENTER);
    assert_eq!(a.events().len(), 1);
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn fire_event_by_position() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    a.clear_event();
    a.fire_event_pos(55, 55, 42);
    assert_eq!(a.events().len(), 1);
    let e = a.events()[0];
    assert_eq!(e.eventid, 42);
    assert_eq!(e.watcher, 1);
    assert_eq!(e.marker, 0);
}

#[test]
fn fire_event_nonexistent_no_crash() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.clear_event();
    a.fire_event(999, EVENT_ENTER);
    assert!(a.events().is_empty());
}

// ── 11. Options ──────────────────────────────────────────────────────────────

#[test]
fn leave_event_disabled_by_default() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    a.clear_event();
    a.update(2, 10, 10, 0, 0, 0);
    assert!(!has_event(&a, EVENT_LEAVE, 1, 2));
}

#[test]
fn enable_leave_event_works() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    a.clear_event();
    a.update(2, 10, 10, 0, 0, 0);
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
}

#[test]
fn self_event_disabled_by_default() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.clear_event();
    ins(&mut a, 1, 50, 50, 40, 40, 0, W | M);
    assert!(!has_event(&a, EVENT_ENTER, 1, 1));
}

#[test]
fn enable_self_event_works() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_SELF_EVENT);
    a.clear_event();
    ins(&mut a, 1, 50, 50, 40, 40, 0, W | M);
    assert!(has_event(&a, EVENT_ENTER, 1, 1));
}

// ── 12. Misc ─────────────────────────────────────────────────────────────────

#[test]
fn range_marker_registered_to_multiple_tiles() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // range marker (marker-only, w,h>0) is registered to every tile it overlaps
    ins(&mut a, 1, 50, 50, 40, 40, 0, M);
    let mut tiles = 0;
    for y in 0..10 {
        for x in 0..10 {
            if a.markers(a.get_index(x, y)).contains(&1) {
                tiles += 1;
            }
        }
    }
    assert!(tiles > 1, "range marker should span multiple tiles, got {tiles}");
}

#[test]
fn range_marker_erase_cleans_all_tiles() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, M);
    a.erase(1, true);
    // every tile must be marker-free afterwards
    for y in 0..10 {
        for x in 0..10 {
            assert!(a.markers(a.get_index(x, y)).is_empty());
        }
    }
}

#[test]
fn clear_removes_everything() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    a.clear();
    assert_eq!(a.size(), 0);
    assert!(!a.has_object(1));
    assert!(!a.has_object(2));
}

#[test]
fn multiple_watchers_same_marker() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 52, 52, 40, 40, 0, W);
    a.clear_event();
    ins(&mut a, 3, 55, 55, 0, 0, 0, M);
    assert!(has_event(&a, EVENT_ENTER, 1, 3));
    assert!(has_event(&a, EVENT_ENTER, 2, 3));
}

#[test]
fn for_each_all_filters_by_mode() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    let mut markers = Vec::new();
    a.for_each_all(|h, _, _, _, _| markers.push(h), M);
    assert!(markers.contains(&2));
    assert!(!markers.contains(&1));
}

#[test]
fn get_index_and_markers() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 55, 55, 0, 0, 0, M);
    let idx = a.get_index(5, 5);
    assert!(a.markers(idx).contains(&1));
}

#[test]
fn events_accumulate_without_clear() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    a.clear_event();
    ins(&mut a, 2, 55, 55, 0, 0, 0, M);
    ins(&mut a, 3, 56, 56, 0, 0, 0, M);
    assert_eq!(count_events(&a, EVENT_ENTER), 2);
}

// ── Raycast ──────────────────────────────────────────────────────────────────

#[test]
fn raycast_hit_fixed_marker() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, M | F);
    let hit = a.raycast(Vec2::new(10.0, 50.0), Vec2::new(90.0, 50.0), 5.0);
    assert_eq!(hit, Some(1));
}

#[test]
fn raycast_miss() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, M | F);
    let hit = a.raycast(Vec2::new(10.0, 80.0), Vec2::new(90.0, 80.0), 5.0);
    assert_eq!(hit, None);
}

#[test]
fn raycast_skips_non_fixed() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, M);
    let hit = a.raycast(Vec2::new(10.0, 50.0), Vec2::new(90.0, 50.0), 5.0);
    assert_eq!(hit, None);
}

#[test]
fn raycast_hits_nearest_first() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 30, 50, 10, 10, 0, M | F);
    ins(&mut a, 2, 70, 50, 10, 10, 0, M | F);
    let hit = a.raycast(Vec2::new(10.0, 50.0), Vec2::new(90.0, 50.0), 3.0);
    assert_eq!(hit, Some(1));
}

#[test]
fn raycast_zero_distance() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, M | F);
    let hit = a.raycast(Vec2::new(50.0, 50.0), Vec2::new(50.0, 50.0), 5.0);
    assert_eq!(hit, None);
}

#[test]
fn raycast_diagonal() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, M | F);
    let hit = a.raycast(Vec2::new(10.0, 10.0), Vec2::new(90.0, 90.0), 3.0);
    assert_eq!(hit, Some(1));
}

// ── watcher+marker combo / general path ──────────────────────────────────────

#[test]
fn watcher_marker_combo_mutual_visibility() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.clear_event();
    ins(&mut a, 1, 50, 50, 40, 40, 0, W | M);
    a.clear_event();
    ins(&mut a, 2, 52, 52, 40, 40, 0, W | M);
    // 1 sees 2 (newly inserted marker enters 1's view) and 2 sees 1
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
    assert!(has_event(&a, EVENT_ENTER, 2, 1));
}

#[test]
fn erase_watcher_marker_combo() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 55, 55, 0, 0, 0, W | M);
    a.clear_event();
    a.erase(2, true);
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
    assert_eq!(a.size(), 1);
}

#[test]
fn update_watcher_general_path_disjoint() {
    let mut a = Aoi::new(0, 0, 200, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 30, 30, 20, 20, 0, W); // view (20..40, 20..40)
    ins(&mut a, 2, 25, 25, 0, 0, 0, M); // inside old view
    ins(&mut a, 3, 155, 155, 0, 0, 0, M); // inside new view
    a.clear_event();
    assert!(a.update(1, 150, 150, 20, 20, 0)); // disjoint move
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
    assert!(has_event(&a, EVENT_ENTER, 1, 3));
}

#[test]
fn single_tile_map() {
    let mut a = Aoi::new(0, 0, 10, 10);
    ins(&mut a, 1, 5, 5, 10, 10, 0, W);
    a.clear_event();
    ins(&mut a, 2, 6, 6, 0, 0, 0, M);
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn nonzero_origin() {
    let mut a = Aoi::new(1000, 1000, 100, 10);
    ins(&mut a, 1, 1050, 1050, 40, 40, 0, W);
    a.clear_event();
    ins(&mut a, 2, 1055, 1055, 0, 0, 0, M);
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn zero_view_watcher_sees_nothing() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 0, 0, 0, W);
    a.clear_event();
    ins(&mut a, 2, 50, 50, 0, 0, 0, M);
    assert!(a.events().is_empty());
}

#[test]
fn version_monotonic_increase() {
    let mut a = Aoi::new(0, 0, 100, 10);
    let idx = a.get_index(5, 5);
    let v0 = a.get_version(idx);
    ins(&mut a, 1, 55, 55, 0, 0, 0, M | F);
    let v1 = a.get_version(idx);
    a.fire_event(1, EVENT_POS);
    let v2 = a.get_version(idx);
    assert!(v1 > v0 && v2 > v1);
}

#[test]
fn version_unchanged_for_nonfixed() {
    let mut a = Aoi::new(0, 0, 100, 10);
    let idx = a.get_index(5, 5);
    let v0 = a.get_version(idx);
    ins(&mut a, 1, 55, 55, 0, 0, 0, M);
    assert_eq!(a.get_version(idx), v0);
}

// ── A1: range marker movement (regression) ───────────────────────────────────

#[test]
fn range_marker_move_keeps_membership_consistent() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // range marker covering tiles (3..8, 3..8)
    ins(&mut a, 1, 50, 50, 40, 40, 0, M);
    // overlapping move that used to corrupt membership / panic on debug_assert
    assert!(a.update(1, 60, 60, 40, 40, 0)); // new coverage (4..9, 4..9)

    // tile only in the old coverage must be cleared
    assert!(!a.markers(a.get_index(3, 3)).contains(&1));
    // tile only in the new coverage must be present
    assert!(a.markers(a.get_index(8, 8)).contains(&1));
    // overlap tile stays present
    assert!(a.markers(a.get_index(5, 5)).contains(&1));

    // membership exactly equals the new 5x5 coverage
    let mut tiles = 0;
    for y in 0..10 {
        for x in 0..10 {
            if a.markers(a.get_index(x, y)).contains(&1) {
                tiles += 1;
            }
        }
    }
    assert_eq!(tiles, 25);
}

#[test]
fn range_marker_move_then_erase_no_corruption() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, M);
    assert!(a.update(1, 30, 70, 20, 20, 0)); // shrink + move
    a.erase(1, true); // must not panic / underflow
    assert_eq!(a.size(), 0);
    for y in 0..10 {
        for x in 0..10 {
            assert!(a.markers(a.get_index(x, y)).is_empty());
        }
    }
}

#[test]
fn range_marker_grow_and_shrink_extent() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 0, 0, 0, M); // starts as a point marker (tile 5,5)
    assert_eq!(a.markers(a.get_index(5, 5)).len(), 1);

    // grow into a range marker
    assert!(a.update(1, 50, 50, 40, 40, 0));
    let mut grown = 0;
    for y in 0..10 {
        for x in 0..10 {
            if a.markers(a.get_index(x, y)).contains(&1) {
                grown += 1;
            }
        }
    }
    assert!(grown > 1);

    // shrink back to a point marker
    assert!(a.update(1, 50, 50, 0, 0, 0));
    let mut shrunk = 0;
    for y in 0..10 {
        for x in 0..10 {
            if a.markers(a.get_index(x, y)).contains(&1) {
                shrunk += 1;
            }
        }
    }
    assert_eq!(shrunk, 1);
    assert!(a.markers(a.get_index(5, 5)).contains(&1));
}

// ── A2: marker self layer-change events ───────────────────────────────────────

#[test]
fn marker_layer_rise_into_view_produces_enter_not_pos() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // watcher layer 5; marker layer 3 → invisible (watcher.layer > marker.layer)
    ins(&mut a, 1, 50, 50, 40, 40, 5, W);
    ins(&mut a, 2, 55, 55, 0, 0, 3, M);
    a.clear_event();
    // raise marker layer to 5 → now visible: must be ENTER, not POS
    assert!(a.update(2, 55, 55, 0, 0, 5));
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
    assert!(!has_event(&a, EVENT_POS, 1, 2));
}

#[test]
fn marker_layer_drop_out_of_view_produces_leave() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    // watcher layer 5; marker layer 5 → visible
    ins(&mut a, 1, 50, 50, 40, 40, 5, W);
    ins(&mut a, 2, 55, 55, 0, 0, 5, M);
    a.clear_event();
    // drop marker layer to 3 → now invisible: must emit LEAVE
    assert!(a.update(2, 55, 55, 0, 0, 3));
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
}

#[test]
fn marker_layer_drop_without_leave_option_is_silent() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // leave events disabled (default): dropping out of view emits nothing
    ins(&mut a, 1, 50, 50, 40, 40, 5, W);
    ins(&mut a, 2, 55, 55, 0, 0, 5, M);
    a.clear_event();
    assert!(a.update(2, 55, 55, 0, 0, 3));
    assert!(!has_event(&a, EVENT_LEAVE, 1, 2));
    assert!(!has_event(&a, EVENT_POS, 1, 2));
}

#[test]
fn marker_move_and_layer_change_combined() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 50, 50, 40, 40, 5, W); // view (30..70), layer 5
    ins(&mut a, 2, 35, 35, 0, 0, 5, M); // visible
    a.clear_event();
    // move still inside view but drop layer out of visibility → leave
    assert!(a.update(2, 60, 60, 0, 0, 3));
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
}

// ── Area-intersection visibility for range markers ───────────────────────────

#[test]
fn watcher_sees_range_marker_by_area_not_center() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // watcher view [15,35)x[40,60); does NOT contain the marker center (50,50)
    ins(&mut a, 1, 25, 50, 20, 20, 0, W);
    a.clear_event();
    // range marker area [30,70)x[30,70) overlaps the watcher view on the edge
    ins(&mut a, 2, 50, 50, 40, 40, 0, M);
    // with area semantics this is visible (center-point semantics would miss it)
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn watcher_moves_into_range_marker_area() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 2, 50, 50, 40, 40, 0, M); // static range marker, area [30,70)
    ins(&mut a, 1, 5, 5, 10, 10, 0, W); // far away, sees nothing
    a.clear_event();
    // move the watcher so its view [15,35)x[40,60) intersects the marker area
    assert!(a.update(1, 25, 50, 20, 20, 0));
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn range_marker_enter_is_deduped_for_overlapping_watcher() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // large watcher overlapping many of the marker's tiles
    ins(&mut a, 1, 50, 50, 60, 60, 0, W);
    a.clear_event();
    ins(&mut a, 2, 50, 50, 40, 40, 0, M);
    // exactly one enter despite spanning multiple shared tiles
    assert_eq!(count_event(&a, EVENT_ENTER, 1, 2), 1);
}

#[test]
fn range_marker_move_into_view_is_deduped() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 60, 60, 0, W); // big view [20,80)
    ins(&mut a, 2, 95, 5, 4, 4, 0, M); // tiny range marker far away
    a.clear_event();
    // move marker so its area [30,70) intersects the watcher view
    assert!(a.update(2, 50, 50, 40, 40, 0));
    assert_eq!(count_event(&a, EVENT_ENTER, 1, 2), 1);
}

#[test]
fn range_marker_move_out_of_view_leaves_by_area() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 50, 50, 60, 60, 0, W); // big view [20,80)
    ins(&mut a, 2, 50, 50, 40, 40, 0, M); // area [30,70), visible
    a.clear_event();
    // move marker far away so its area no longer intersects the view
    assert!(a.update(2, 95, 5, 4, 4, 0));
    assert_eq!(count_event(&a, EVENT_LEAVE, 1, 2), 1);
}

#[test]
fn range_marker_erase_leaves_by_area() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 25, 50, 20, 20, 0, W); // view [15,35)x[40,60)
    ins(&mut a, 2, 50, 50, 40, 40, 0, M); // area overlaps view edge
    a.clear_event();
    a.erase(2, true);
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
}

#[test]
fn range_marker_hide_show_by_area() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 25, 50, 20, 20, 0, W); // view [15,35)x[40,60)
    ins(&mut a, 2, 50, 50, 40, 40, 0, M); // area overlaps view edge
    a.clear_event();
    a.set_hide(2, true);
    assert!(has_event(&a, EVENT_LEAVE, 1, 2));
    a.clear_event();
    a.set_hide(2, false);
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn point_marker_outside_range_area_no_event() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // watcher view [15,35)x[40,60); a range marker whose area does NOT reach it
    ins(&mut a, 1, 25, 50, 20, 20, 0, W);
    a.clear_event();
    ins(&mut a, 2, 80, 80, 10, 10, 0, M); // area [75,85)x[75,85), no overlap
    assert!(!has_event(&a, EVENT_ENTER, 1, 2));
}

// ── Area semantics: boundary / half-open edge cases ──────────────────────────

#[test]
fn range_marker_area_touching_view_edge_x_is_not_seen() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // watcher view [50,70); range marker area [30,50): they touch at x=50 only
    ins(&mut a, 1, 60, 50, 20, 20, 0, W);
    a.clear_event();
    ins(&mut a, 2, 40, 50, 20, 20, 0, M);
    // half-open intersection excludes a pure edge touch
    assert!(!has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn range_marker_area_touching_view_edge_y_is_not_seen() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // watcher view y[50,70); marker area y[30,50): touch at y=50
    ins(&mut a, 1, 50, 60, 20, 20, 0, W);
    a.clear_event();
    ins(&mut a, 2, 50, 40, 20, 20, 0, M);
    assert!(!has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn range_marker_area_one_unit_overlap_is_seen() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // watcher view [49,69); marker area [30,50): overlap on [49,50)
    ins(&mut a, 1, 59, 50, 20, 20, 0, W);
    a.clear_event();
    ins(&mut a, 2, 40, 50, 20, 20, 0, M);
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn watcher_view_fully_contains_range_marker() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 80, 80, 0, W); // view [10,90)
    a.clear_event();
    ins(&mut a, 2, 50, 50, 10, 10, 0, M); // area [45,55) inside
    assert_eq!(count_event(&a, EVENT_ENTER, 1, 2), 1);
}

#[test]
fn range_marker_area_fully_contains_watcher_view() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 2, 50, 50, 80, 80, 0, M); // area [10,90)
    a.clear_event();
    ins(&mut a, 1, 50, 50, 10, 10, 0, W); // view [45,55) inside the area
    assert_eq!(count_event(&a, EVENT_ENTER, 1, 2), 1);
}

#[test]
fn range_marker_area_clamped_at_map_corner() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // marker box would extend below origin; area clamps to [0,15)
    ins(&mut a, 2, 5, 5, 20, 20, 0, M);
    a.clear_event();
    ins(&mut a, 1, 10, 10, 10, 10, 0, W); // view [5,15)
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn range_marker_odd_dimension_uses_half_extent() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // w=21 -> w/2=10 -> area [40,60); a watcher just outside at x>=60 sees nothing
    ins(&mut a, 1, 70, 50, 20, 20, 0, W); // view [60,80), touches at 60 -> excluded
    a.clear_event();
    ins(&mut a, 2, 50, 50, 21, 21, 0, M);
    assert!(!has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn range_marker_covering_entire_map_seen_from_corner() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 2, 50, 50, 100, 100, 0, M); // area [0,100)
    a.clear_event();
    ins(&mut a, 1, 95, 5, 10, 10, 0, W); // corner watcher
    assert_eq!(count_event(&a, EVENT_ENTER, 1, 2), 1);
}

#[test]
fn nonzero_origin_range_marker_area() {
    let mut a = Aoi::new(1000, 1000, 100, 10);
    ins(&mut a, 1, 1025, 1050, 20, 20, 0, W); // view [1015,1035)x[1040,1060)
    a.clear_event();
    ins(&mut a, 2, 1050, 1050, 40, 40, 0, M); // area [1030,1070)
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn marker_with_zero_height_is_point_not_range() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // w>0 but h==0 => point marker at its center, not an area
    ins(&mut a, 1, 20, 50, 20, 20, 0, W); // view [10,30) does NOT contain center (50,50)
    a.clear_event();
    ins(&mut a, 2, 50, 50, 40, 0, 0, M);
    assert!(!has_event(&a, EVENT_ENTER, 1, 2));
}

// ── Area semantics: watcher view changes vs range markers ─────────────────────

#[test]
fn watcher_zoom_in_leaves_range_marker_by_area() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 2, 15, 50, 10, 10, 0, M); // area [10,20)x[45,55)
    ins(&mut a, 1, 50, 50, 80, 80, 0, W); // view [10,90) sees the marker
    a.clear_event();
    assert!(a.update(1, 50, 50, 20, 20, 0)); // shrink to [40,60): no longer overlaps
    assert_eq!(count_event(&a, EVENT_LEAVE, 1, 2), 1);
}

#[test]
fn watcher_zoom_out_enters_range_marker_by_area() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 2, 15, 50, 10, 10, 0, M); // area [10,20)x[45,55)
    ins(&mut a, 1, 50, 50, 20, 20, 0, W); // view [40,60) does not overlap
    a.clear_event();
    assert!(a.update(1, 50, 50, 80, 80, 0)); // expand to [10,90): now overlaps
    assert_eq!(count_event(&a, EVENT_ENTER, 1, 2), 1);
}

#[test]
fn watcher_general_move_with_range_markers_dedup() {
    let mut a = Aoi::new(0, 0, 200, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 2, 30, 30, 20, 20, 0, M); // area [20,40)
    ins(&mut a, 3, 150, 150, 20, 20, 0, M); // area [140,160)
    ins(&mut a, 1, 30, 30, 20, 20, 0, W); // sees marker 2
    a.clear_event();
    assert!(a.update(1, 150, 150, 20, 20, 0)); // disjoint move -> general path
    assert_eq!(count_event(&a, EVENT_LEAVE, 1, 2), 1);
    assert_eq!(count_event(&a, EVENT_ENTER, 1, 3), 1);
}

// ── Area semantics: range marker resize / movement events ─────────────────────

#[test]
fn range_marker_grow_in_place_enters_watcher() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 20, 50, 20, 20, 0, W); // view [10,30)x[40,60)
    ins(&mut a, 2, 50, 50, 10, 10, 0, M); // area [45,55): no overlap
    a.clear_event();
    assert!(a.update(2, 50, 50, 60, 60, 0)); // grow to area [20,80): now overlaps
    assert_eq!(count_event(&a, EVENT_ENTER, 1, 2), 1);
}

#[test]
fn range_marker_shrink_in_place_leaves_watcher() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 20, 50, 20, 20, 0, W); // view [10,30)
    ins(&mut a, 2, 50, 50, 60, 60, 0, M); // area [20,80): overlaps
    a.clear_event();
    assert!(a.update(2, 50, 50, 10, 10, 0)); // shrink to [45,55): no overlap
    assert_eq!(count_event(&a, EVENT_LEAVE, 1, 2), 1);
}

#[test]
fn range_marker_move_within_view_pos_dedup() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 80, 80, 0, W); // view [10,90)
    ins(&mut a, 2, 40, 40, 10, 10, 0, M); // area inside
    a.clear_event();
    assert!(a.update(2, 45, 45, 10, 10, 0)); // still inside view
    assert_eq!(count_event(&a, EVENT_POS, 1, 2), 1);
}

#[test]
fn multiple_range_markers_one_watcher() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 80, 80, 0, W); // big view
    a.clear_event();
    ins(&mut a, 2, 30, 30, 10, 10, 0, M);
    ins(&mut a, 3, 60, 60, 10, 10, 0, M);
    assert_eq!(count_event(&a, EVENT_ENTER, 1, 2), 1);
    assert_eq!(count_event(&a, EVENT_ENTER, 1, 3), 1);
}

#[test]
fn fire_event_range_marker_dedup_to_multiple_watchers() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 80, 80, 0, W);
    ins(&mut a, 2, 48, 48, 80, 80, 0, W);
    ins(&mut a, 3, 50, 50, 40, 40, 0, M); // range marker both watchers see
    a.clear_event();
    a.fire_event(3, 99);
    assert_eq!(count_event(&a, 99, 1, 3), 1);
    assert_eq!(count_event(&a, 99, 2, 3), 1);
}

#[test]
fn range_marker_layer_filter() {
    let mut a = Aoi::new(0, 0, 100, 10);
    // watcher layer 5 cannot see a range marker on a lower layer 3
    ins(&mut a, 1, 50, 50, 80, 80, 5, W);
    a.clear_event();
    ins(&mut a, 2, 50, 50, 40, 40, 3, M);
    assert!(!has_event(&a, EVENT_ENTER, 1, 2));
    // a same/lower-layer watcher can
    ins(&mut a, 3, 50, 50, 80, 80, 3, W);
    assert!(has_event(&a, EVENT_ENTER, 3, 2));
}

// ── General lifecycle edge cases ──────────────────────────────────────────────

#[test]
fn double_erase_no_crash() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W | M);
    a.erase(1, true);
    a.erase(1, true); // second erase is a no-op
    assert_eq!(a.size(), 0);
}

#[test]
fn insert_then_immediate_erase_range_marker_is_clean() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, M);
    a.erase(1, true);
    assert_eq!(a.size(), 0);
    for y in 0..10 {
        for x in 0..10 {
            assert!(a.markers(a.get_index(x, y)).is_empty());
        }
    }
}

#[test]
fn soft_erase_then_reupdate_range_marker() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W); // view [30,70) overlaps marker area
    ins(&mut a, 2, 50, 50, 40, 40, 0, M); // visible by area
    a.erase(2, false); // soft delete -> geometry zeroed, unlinked
    assert!(a.has_object(2));
    // moving it back into view should produce an enter again
    a.clear_event();
    assert!(a.update(2, 50, 50, 40, 40, 0));
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn view_larger_than_map_is_clamped() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 1000, 1000, 0, W); // view clamps to the whole map
    a.clear_event();
    ins(&mut a, 2, 5, 5, 0, 0, 0, M);
    assert!(has_event(&a, EVENT_ENTER, 1, 2));
}

#[test]
fn stress_many_objects() {
    let mut a = Aoi::new(0, 0, 1000, 10);
    for i in 0..500 {
        let x = (i * 7) % 1000;
        let y = (i * 13) % 1000;
        ins(&mut a, i as Handle, x, y, 40, 40, 0, W | M);
    }
    assert_eq!(a.size(), 500);
    // move them all once; must not panic and registry stays consistent
    for i in 0..500 {
        let x = (i * 11) % 1000;
        let y = (i * 17) % 1000;
        a.update(i as Handle, x, y, 40, 40, 0);
    }
    assert_eq!(a.size(), 500);
}

// ── CPU profiling (pprof flame graphs) ───────────────────────────────────────
//
// Generates per-scenario flame graphs for the throughput-bound stress cases so
// we can see where the remaining time goes (HashSet churn, event push, geometry,
// etc.). Build optimized + with symbols, single test thread:
//
//   cargo test -p moon-game --profile profiling \
//       aoi::tests::profile_flamegraph -- --ignored --nocapture --test-threads=1
//
// SVGs land in target/profile/aoi_<scenario>.svg. Per-run wall time is set via
// AOI_PROF_MS (default 2500 ms each).
#[test]
#[ignore]
fn profile_flamegraph() {
    use std::fs::File;
    use std::time::{Duration, Instant};

    let dur = Duration::from_millis(
        std::env::var("AOI_PROF_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2500),
    );
    let outdir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/profile");
    std::fs::create_dir_all(outdir).unwrap();

    let profile = |name: &str, dur: Duration, f: &mut dyn FnMut()| {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(1000)
            .blocklist(&["libc", "libdyld", "libpthread", "libsystem"])
            .build()
            .unwrap();
        let start = Instant::now();
        let mut iters = 0u64;
        while start.elapsed() < dur {
            f();
            iters += 1;
        }
        let elapsed = start.elapsed();
        let report = guard.report().build().unwrap();
        let out = format!("{outdir}/aoi_{name}.svg");
        report.flamegraph(File::create(&out).unwrap()).unwrap();

        // Aggregate leaf (self-time) samples per innermost symbol.
        let mut self_time: std::collections::HashMap<String, isize> = Default::default();
        for (frames, count) in report.data.iter() {
            if let Some(sym) = frames.frames.first().and_then(|leaf| leaf.first()) {
                *self_time.entry(format!("{sym}")).or_default() += *count;
            }
        }
        let total: isize = self_time.values().copied().sum();
        let mut v: Vec<_> = self_time.into_iter().collect();
        v.sort_by_key(|(_, c)| -*c);
        use std::io::Write;
        let mut txt = String::new();
        txt.push_str(&format!(
            "[{name}] {iters} iters in {elapsed:?}, {total} self-time samples\n"
        ));
        for (sym, c) in v.iter().take(18) {
            txt.push_str(&format!(
                "  {:5.1}%  {}\n",
                100.0 * (*c as f64) / total as f64,
                sym
            ));
        }
        eprint!("{txt}");
        File::create(format!("{outdir}/aoi_{name}.txt"))
            .unwrap()
            .write_all(txt.as_bytes())
            .unwrap();
    };

    // S5: rapid insert+erase of point markers seen by 10 overlapping watchers.
    {
        let mut a = Aoi::new(0, 0, 2000, 50);
        a.set_option(ENABLE_LEAVE_EVENT);
        for i in 1..=10 {
            ins(&mut a, i, 1000, 1000, 400, 400, 0, W);
        }
        let mut k = 0i64;
        profile("s5_insert_erase", dur, &mut || {
            let id = 50000 + (k % 10000);
            k += 1;
            let off = (k % 100) as i32 - 50;
            a.clear_event();
            ins(&mut a, id, 1000 + off, 1000 + off, 0, 0, 0, M);
            a.erase(id, true);
        });
    }

    // S8: mass hide/show of 500 point markers under 50 large watchers.
    {
        let mut a = Aoi::new(0, 0, 5000, 100);
        for i in 1..=50i32 {
            ins(&mut a, i as Handle, 2500, 2500, 1000, 1000, 0, W);
        }
        for i in 0..500i32 {
            ins(&mut a, (1000 + i) as Handle, 2000 + (i % 50) * 20, 2000 + (i / 50) * 20, 0, 0, 0, M);
        }
        profile("s8_hide_show", dur, &mut || {
            for i in 0..500i32 {
                a.set_hide((1000 + i) as Handle, true);
            }
            for i in 0..500i32 {
                a.set_hide((1000 + i) as Handle, false);
            }
        });
    }

    // S2: build 500 mutually-visible W|M objects from empty (insert + enter burst).
    profile("s2_mutual", dur, &mut || {
        let mut a = Aoi::new(0, 0, 5000, 50);
        for i in 1..=500i32 {
            let x = 2500 + (i % 50);
            let y = 2500 + (i / 50);
            a.clear_event();
            ins(&mut a, i as Handle, x, y, 300, 300, 0, W | M);
        }
        std::hint::black_box(a.size());
    });

    // S6: one point marker orbiting through 100 overlapping watchers (pos churn).
    {
        let mut a = Aoi::new(0, 0, 2000, 50);
        a.set_option(ENABLE_LEAVE_EVENT);
        for i in 1..=100i32 {
            ins(&mut a, i as Handle, 1000 + (i % 10) * 5, 1000 + (i / 10) * 5, 600, 600, 0, W);
        }
        ins(&mut a, 500, 1000, 1000, 0, 0, 0, M);
        let mut step = 0i64;
        profile("s6_orbit", dur, &mut || {
            let x = 1000 + (50.0 * (step as f64 * 0.1).sin()) as i32;
            let y = 1000 + (50.0 * (step as f64 * 0.1).cos()) as i32;
            step += 1;
            a.clear_event();
            a.update(500, x, y, 0, 0, 0);
        });
    }

    // S3: 200 large watchers + 1000 point markers, 50 ticks of random marker
    // movement (the marker-move / emit-to-watchers path under moderate fan-out).
    {
        let mut a = Aoi::new(0, 0, 5000, 100);
        a.set_option(ENABLE_LEAVE_EVENT);
        for i in 0..200i64 {
            ins(&mut a, i, ((i * 37) % 5000) as i32, ((i * 53) % 5000) as i32, 400, 400, 0, W);
        }
        for i in 0..1000i64 {
            ins(&mut a, 10000 + i, ((i * 41) % 5000) as i32, ((i * 67) % 5000) as i32, 0, 0, 0, M);
        }
        let mut s = 0x9e3779b97f4a7c15u64;
        profile("s3_tick", dur, &mut || {
            for _tick in 0..50 {
                a.clear_event();
                for i in 0..1000i64 {
                    let m = *a.find(10000 + i).unwrap();
                    s ^= s << 13;
                    s ^= s >> 7;
                    s ^= s << 17;
                    let nx = (m.x + (s % 41) as i32 - 20).clamp(0, 4999);
                    s ^= s << 13;
                    s ^= s >> 7;
                    s ^= s << 17;
                    let ny = (m.y + (s % 41) as i32 - 20).clamp(0, 4999);
                    a.update(10000 + i, nx, ny, 0, 0, 0);
                }
            }
        });
    }

    // S22: 100 watchers of mixed view sizes + 200 point markers, 30 ticks of
    // random watcher movement (watcher-move + per-marker scan, range overlap).
    {
        let mut a = Aoi::new(0, 0, 5000, 100);
        a.set_option(ENABLE_LEAVE_EVENT);
        for i in 0..100i32 {
            ins(&mut a, i as Handle, 2500, 2500, 50 + i * 10, 50 + i * 10, 0, W);
        }
        for i in 0..200i32 {
            ins(&mut a, (1000 + i) as Handle, 2000 + (i * 17) % 1000, 2000 + (i * 31) % 1000, 0, 0, 0, M);
        }
        let mut s = 0xd1b54a32d192ed03u64;
        profile("s22_mixed_view", dur, &mut || {
            for _tick in 0..30 {
                a.clear_event();
                for i in 0..100i64 {
                    let w = *a.find(i).unwrap();
                    s ^= s << 13;
                    s ^= s >> 7;
                    s ^= s << 17;
                    let nx = (w.x + (s % 101) as i32 - 50).clamp(0, 4999);
                    s ^= s << 13;
                    s ^= s >> 7;
                    s ^= s << 17;
                    let ny = (w.y + (s % 101) as i32 - 50).clamp(0, 4999);
                    a.update(i, nx, ny, w.w, w.h, 0);
                }
            }
        });
    }

    // S12: 10 layers x 50 point markers + 1 big watcher; 10 layer switches
    // (Vtm::Layer scan of a single dense tile cluster).
    {
        let mut a = Aoi::new(0, 0, 5000, 100);
        a.set_option(ENABLE_LEAVE_EVENT);
        for layer in 0..10i32 {
            for i in 0..50i64 {
                ins(
                    &mut a,
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
        ins(&mut a, 99999, 2500, 2500, 800, 800, 0, W);
        profile("s12_layers", dur, &mut || {
            for nl in 1..=10 {
                a.clear_event();
                a.update(99999, 2500, 2500, 800, 800, nl);
            }
        });
    }

    // S20: traverse the whole grid twice (for_each_all marker + watcher filters)
    // over 800 objects — pure slab/tile iteration, no event emission.
    {
        let mut a = Aoi::new(0, 0, 5000, 100);
        for i in 0..500i32 {
            ins(&mut a, i as Handle, (i * 41) % 5000, (i * 67) % 5000, 0, 0, 0, M);
        }
        for i in 500..700i32 {
            ins(&mut a, i as Handle, (i * 31) % 5000, (i * 53) % 5000, 200, 200, 0, W);
        }
        for i in 700..800i32 {
            ins(&mut a, i as Handle, (i * 23) % 5000, (i * 47) % 5000, 200, 200, 0, W | M);
        }
        profile("s20_foreach", dur, &mut || {
            let mut acc = 0u64;
            a.for_each_all(|h, _, _, _, _| acc = acc.wrapping_add(h as u64), M);
            a.for_each_all(|h, _, _, _, _| acc = acc.wrapping_add(h as u64), W);
            std::hint::black_box(acc);
        });
    }

    // S24: single-tile map (whole map is one tile). 100 W|M insert + update +
    // clear — worst-case fan-out: every object shares one tile.
    profile("s24_single_tile", dur, &mut || {
        let mut a = Aoi::new(0, 0, 1000, 1000);
        for i in 0..100i64 {
            ins(&mut a, i, (i * 9) as i32, (i * 9) as i32, 500, 500, 0, W | M);
        }
        a.clear_event();
        for i in 0..100i64 {
            a.update(i, 500 + i as i32, 500 + i as i32, 500, 500, 0);
        }
        std::hint::black_box(a.events().len());
        a.clear();
    });
}
