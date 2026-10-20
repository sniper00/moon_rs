use super::*;

#[test]
fn tile_traversals_preserve_order_and_skip_empty_rectangles() {
    let rect = TileRect(Rect::new(2, 3, 3, 2));
    let mut cells = Vec::new();
    rect.for_each(|x, y| cells.push((x, y)));
    assert_eq!(cells, [(2, 3), (3, 3), (4, 3), (2, 4), (3, 4), (4, 4)]);
    cells.clear();
    rect.for_each_column_major(|x, y| cells.push((x, y)));
    assert_eq!(cells, [(2, 3), (2, 4), (3, 3), (3, 4), (4, 3), (4, 4)]);
    for empty in [Rect::new(2, 3, 0, 2), Rect::new(2, 3, 3, 0)] {
        TileRect(empty).for_each(|_, _| panic!("visited empty rectangle"));
        TileRect(empty).for_each_column_major(|_, _| panic!("visited empty rectangle"));
    }
}

#[test]
fn boundary_scan_matches_full_tile_geometry_at_coordinate_limits() {
    for origin in [(0, 0), (-23, 17), (i32::MIN, i32::MAX - 100)] {
        for tile_size in [1, 10, 25] {
            let grid = Grid::new(AoiConfig {
                origin,
                map_size: 100,
                tile_size,
            })
            .unwrap();
            let outers = [
                TileRect(Rect::new(0, 0, grid.count, grid.count)),
                grid.view_tiles(origin.0 + 30, origin.1 + 70, 47, 35),
                TileRect::default(),
            ];
            for offset in [0, 1, 9, 50, 99] {
                for w in [0, 1, 2, 11, 40, 101, i32::MAX] {
                    for h in [0, 1, 11, 40, 101] {
                        let (x, y) = (origin.0 + offset, origin.1 + offset * 7 % 100);
                        let view = grid.view(x, y, w, h);
                        let interior = grid.interior_tiles(view, grid.view_tiles(x, y, w, h));
                        assert_eq!(interior, grid.interior_tiles(view, grid.tiles(view)));
                        for outer in outers {
                            let mut actual = Vec::new();
                            for strip in outer.outside(interior) {
                                for x in strip.left()..strip.right() {
                                    for y in strip.bottom()..strip.top() {
                                        actual.push((x, y));
                                    }
                                }
                            }
                            actual.sort_unstable();
                            let mut expected = Vec::new();
                            for tx in outer.left()..outer.right() {
                                for ty in outer.bottom()..outer.top() {
                                    // Independent, wide-arithmetic full scan of
                                    // tile geometry rather than rounded indices.
                                    let left = i64::from(origin.0) + i64::from(tx * tile_size);
                                    let bottom = i64::from(origin.1) + i64::from(ty * tile_size);
                                    let contained = i64::from(view.left()) <= left
                                        && i64::from(view.bottom()) <= bottom
                                        && left + i64::from(tile_size) <= i64::from(view.right())
                                        && bottom + i64::from(tile_size) <= i64::from(view.top());
                                    if !contained {
                                        expected.push((tx, ty));
                                    }
                                }
                            }
                            assert_eq!(
                                actual, expected,
                                "{origin:?} tile={tile_size} view={view:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn watcher_same_tile_move_keeps_exact_boundary_events() {
    let mut a = Aoi::new(-73, 19, 1000, 100);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 177, 269, 40, 40, 0, W);
    ins(&mut a, 2, 157, 269, 0, 0, 0, M);
    ins(&mut a, 3, 197, 269, 0, 0, 0, M);
    // The range starts exactly at the old right edge; a one-unit move enters it.
    ins(&mut a, 4, 202, 269, 10, 10, 0, M);
    a.clear_event();
    assert!(a.set_position(1, 178, 269));
    let mut events: Vec<_> = a.events().iter().map(|e| (e.eventid, e.marker)).collect();
    events.sort_unstable();
    assert_eq!(
        events,
        [(EVENT_ENTER, 3), (EVENT_ENTER, 4), (EVENT_LEAVE, 2)]
    );
    // Membership must remain intact for subsequent marker-triggered events.
    a.clear_event();
    a.fire_event(3, 99);
    assert_eq!(a.events(), &[AoiEvent::new(99, 1, 3)]);
}

#[test]
fn dedup_marks_follow_slot_reuse_and_clear() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 80, 80, 0, W);
    ins(&mut a, 2, 50, 50, 60, 60, 0, M);
    let slot = a.objects.index[&2];
    let mut found = Vec::new();
    a.query_rect(Rect::new(0, 0, 100, 100), |obj| found.push(obj.handle));
    assert_eq!(found, [2]);
    assert!(a.remove(2));
    a.clear_event();
    ins(&mut a, 3, 50, 50, 60, 60, 0, M);
    assert_eq!(a.objects.index[&3], slot);
    assert_eq!(a.events(), &[AoiEvent::new(EVENT_ENTER, 1, 3)]);
    for _ in 0..2 {
        found.clear();
        a.query_rect(Rect::new(0, 0, 100, 100), |obj| found.push(obj.handle));
        assert_eq!(found, [3]);
        a.clear_event();
        a.fire_event(3, 99);
        assert_eq!(a.events(), &[AoiEvent::new(99, 1, 3)]);
    }
    a.clear();
    ins(&mut a, 4, 50, 50, 80, 80, 0, W);
    ins(&mut a, 5, 50, 50, 60, 60, 0, M);
    assert_eq!(a.events(), &[AoiEvent::new(EVENT_ENTER, 4, 5)]);
    found.clear();
    a.query_rect(Rect::new(0, 0, 100, 100), |obj| found.push(obj.handle));
    assert_eq!(found, [5]);
}

#[test]
fn query_objects_borrows_unfiltered_tile_candidates() {
    let mut a = Aoi::new(-50, -50, 100, 10);
    ins(&mut a, 1, 0, 0, 0, 0, 0, M);
    // Outside the exact query, but inside one of its boundary tiles.
    ins(&mut a, 2, 15, 0, 0, 0, 0, M);
    assert!(a.insert(3, 0, 0, 20, 20, 7, M | F | H, 123));
    ins(&mut a, 4, 0, 0, 20, 20, 0, W);
    ins(&mut a, 5, 0, 0, 0, 0, 0, M);
    ins(&mut a, 6, 0, 0, 0, 0, 0, M);
    assert!(a.detach(5));
    assert!(a.remove(6));
    let events = a.events().to_vec();

    let mut found = Vec::new();
    a.query_objects(0, 0, 20, 20, |obj, is_edge| {
        assert!(std::ptr::eq(obj, a.find(obj.handle).unwrap()));
        if obj.handle == 3 {
            assert_eq!((obj.layer, obj.mode, obj.user), (7, M | F | H, 123));
        }
        found.push((obj.handle, is_edge));
    });
    found.sort_unstable();
    let mut expected = vec![(1, false), (2, true), (3, false)];
    expected.extend([(3, true); 8]);
    assert_eq!(found, expected);
    assert_eq!(a.events(), events);

    // The compatibility API retains the same multiplicity and edge flags.
    let mut handles = Vec::new();
    a.query(0, 0, 20, 20, |id, edge| handles.push((id, edge)));
    handles.sort_unstable();
    assert_eq!(handles, expected);
}

#[test]
fn query_objects_empty_views_do_not_visit_candidates() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 20, 20, 0, M);
    for (w, h) in [(0, 0), (0, 20), (20, 0), (-1, 20), (20, -1)] {
        a.query_objects(50, 50, w, h, |_, _| panic!("empty query {w}x{h}"));
    }
}

#[test]
fn soft_deleted_point_can_rejoin_its_zeroed_tile() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 0, 0, 0, M);
    a.erase(1, false);
    assert!(!a.is_attached(1));
    assert!(a.set_position(1, 5, 5));
    let mut found = Vec::new();
    a.query_rect(Rect::new(0, 0, 10, 10), |o| found.push(o.handle));
    assert_eq!(found, [1]);
    a.erase(1, false);
    a.erase(1, false);
    assert!(a.remove(1));
    assert!(!a.remove(1));
}

#[test]
fn detached_objects_keep_geometry_and_do_not_emit() {
    let mut a = Aoi::new(10, -20, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, W);
    ins(&mut a, 2, 50, 50, 20, 20, 0, M | F);
    let before = *a.find(2).unwrap();
    assert!(a.detach(2));
    assert_eq!(*a.find(2).unwrap(), before);
    a.clear_event();
    a.fire_event(2, 99);
    a.set_hide(2, true);
    a.set_hide(2, false);
    assert!(a.events().is_empty());
    assert!(a.attach(2));
    assert!(!a.attach(2));
    assert_eq!(a.events(), &[AoiEvent::new(EVENT_ENTER, 1, 2)]);
}

#[test]
fn empty_view_leaves_range_marker() {
    for (w, h) in [(0, 0), (0, 10), (10, 0), (1, 1)] {
        let mut a = Aoi::new(0, 0, 100, 10);
        a.set_option(ENABLE_LEAVE_EVENT);
        ins(&mut a, 1, 50, 50, 40, 40, 0, W);
        ins(&mut a, 2, 50, 50, 40, 40, 0, M);
        a.clear_event();
        assert!(a.set_view(1, w, h));
        assert_eq!(a.events(), &[AoiEvent::new(EVENT_LEAVE, 1, 2)], "{w}x{h}");
    }
}

#[test]
fn range_spanning_disjoint_views_stays_visible() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 20, 50, 20, 20, 0, W);
    ins(&mut a, 2, 50, 50, 80, 20, 0, M);
    a.clear_event();
    a.set_position(1, 80, 50);
    assert!(a.events().is_empty());
    a.set_layer(1, 1);
    assert_eq!(a.events(), &[AoiEvent::new(EVENT_LEAVE, 1, 2)]);
}

#[test]
fn self_visibility_is_updated_once_using_both_object_states() {
    let mut a = Aoi::new(0, 0, 100, 10);
    a.set_option(ENABLE_SELF_EVENT | ENABLE_LEAVE_EVENT);
    ins(&mut a, 1, 10, 10, 10, 10, 0, W | M);
    a.clear_event();
    a.set_position(1, 90, 90);
    assert_eq!(a.events(), &[AoiEvent::new(EVENT_POS, 1, 1)]);
    a.clear_event();
    a.set_view(1, 0, 0);
    assert_eq!(a.events(), &[AoiEvent::new(EVENT_LEAVE, 1, 1)]);
    a.clear_event();
    a.set_view(1, 10, 10);
    assert_eq!(a.events(), &[AoiEvent::new(EVENT_ENTER, 1, 1)]);
    a.clear_event();
    a.set_layer(1, 9);
    assert_eq!(a.events(), &[AoiEvent::new(EVENT_POS, 1, 1)]);
}

#[test]
fn fixed_footprint_outside_centered_area_is_raycastable() {
    for mode in [M | F, W | M | F] {
        let mut a = Aoi::new(0, 0, 100, 10);
        ins(&mut a, 1, 50, 50, 40, 40, 0, mode);
        assert_eq!(
            a.raycast(Vec2::new(85.0, 55.0), Vec2::new(89.0, 55.0), 1.0),
            Some(1)
        );
        // The extra broad-phase coverage must not expand the AOI shape.
        let mut found = Vec::new();
        a.query_rect(Rect::new(85, 55, 4, 4), |o| found.push(o.handle));
        assert!(found.is_empty());
    }
}

#[test]
fn fixed_move_invalidates_every_covered_tile() {
    let mut a = Aoi::new(0, 0, 100, 10);
    ins(&mut a, 1, 50, 50, 40, 40, 0, M | F);
    let before: Vec<_> = a.data.iter().map(|t| t.version).collect();
    a.set_position(1, 51, 50);
    let slot = a.objects.index[&1];
    for (i, tile) in a.data.iter().enumerate() {
        if tile.markers.contains(&slot) {
            assert!(tile.version > before[i], "tile {i} was not invalidated");
        }
    }
}

#[test]
fn named_apis_keep_objects_unique_and_drain_capacity() {
    let mut a = Aoi::new(0, 0, 100, 10);
    let desc = AoiObject {
        x: 50,
        y: 50,
        w: 60,
        h: 60,
        layer: 0,
        mode: W | M,
        handle: 1,
        user: 123,
    };
    assert!(a.insert_object(desc));
    assert!(!a.insert_object(desc));
    ins(&mut a, 2, 50, 50, 40, 40, 0, M);
    ins(&mut a, 3, 90, 90, 0, 0, 0, M);
    assert!(a.remove(3));
    assert_eq!(a.objects().count(), 2);
    let mut found = Vec::new();
    a.query_rect(Rect::new(40, 40, 20, 20), |o| found.push(o.handle));
    found.sort_unstable();
    assert_eq!(found, [1, 2]);
    let capacity = a.event_queue.capacity();
    assert!(a.drain_events().count() > 0);
    assert!(a.events().is_empty());
    assert_eq!(a.event_queue.capacity(), capacity);
    assert!(!a.set_layer(1, -1));
    a.fire_event(2, 99);
    a.clear();
    assert_eq!(a.objects().count(), 0);
    assert!(a.events().is_empty());
    assert_eq!(std::mem::size_of::<AoiObject>(), 40);
}

#[test]
fn invalid_configs_and_extreme_coordinates_are_checked() {
    for (config, error) in [
        (
            AoiConfig {
                origin: (0, 0),
                map_size: 0,
                tile_size: 10,
            },
            AoiError::InvalidMapSize,
        ),
        (
            AoiConfig {
                origin: (0, 0),
                map_size: 100,
                tile_size: 0,
            },
            AoiError::InvalidTileSize,
        ),
        (
            AoiConfig {
                origin: (0, 0),
                map_size: 100,
                tile_size: 3,
            },
            AoiError::UnalignedMapSize,
        ),
        (
            AoiConfig {
                origin: (i32::MAX, 0),
                map_size: 100,
                tile_size: 10,
            },
            AoiError::CoordinateOverflow,
        ),
        (
            AoiConfig {
                origin: (0, 0),
                map_size: 100000,
                tile_size: 1,
            },
            AoiError::TooManyTiles,
        ),
    ] {
        assert!(matches!(Aoi::try_new(config), Err(e) if e == error));
    }
    let mut a = Aoi::new(i32::MIN, i32::MAX - 100, 100, 10);
    ins(
        &mut a,
        1,
        i32::MIN,
        i32::MAX - 1,
        i32::MAX,
        i32::MAX,
        0,
        W | M,
    );
    assert!(a.set_position(1, i32::MAX, i32::MIN));
    assert_eq!(
        (a.find(1).unwrap().x, a.find(1).unwrap().y),
        (i32::MIN + 99, i32::MAX - 100)
    );
    assert_eq!(a.get_tile_x(i32::MAX), 9);
    assert_eq!(a.get_tile_y(i32::MIN), 0);
}
