use std::collections::BTreeSet;

use moon_game::common::{IdSet, rect_difference};
use moon_game::math::Rect;

#[test]
fn checked_id_insertion_preserves_uniqueness_across_index_promotion() {
    let mut ids = IdSet::default();
    for id in 0..260 {
        assert!(ids.insert(id));
        assert!(!ids.insert(id));
        assert!(ids.contains(&id));
    }
    assert!(ids.insert(u32::MAX));
    assert!(!ids.insert(u32::MAX));
    for id in (0..260).step_by(2) {
        assert!(ids.remove(&id));
        assert!(!ids.contains(&id));
    }
    let expected: BTreeSet<_> = (0..260)
        .filter(|id| id % 2 != 0)
        .chain([u32::MAX])
        .collect();
    assert_eq!(ids.len(), expected.len());
    assert_eq!(ids.iter().copied().collect::<BTreeSet<_>>(), expected);
    ids.clear();
    assert!(!ids.contains(&u32::MAX));
    assert!(ids.insert(u32::MAX));
    assert!(!ids.insert(u32::MAX));
    assert_eq!(&*ids, &[u32::MAX]);
}

fn cells(rect: Rect) -> impl Iterator<Item = (i32, i32)> {
    (rect.left()..rect.right()).flat_map(move |x| (rect.bottom()..rect.top()).map(move |y| (x, y)))
}

#[test]
fn rectangle_difference_matches_cell_subtraction_without_overlapping_pieces() {
    for (x, y) in [(-3, -2), (0, 0), (i32::MIN + 4, i32::MAX - 12)] {
        for w in [0, 1, 4] {
            for h in [0, 1, 4] {
                let outer = Rect::new(x, y, w, h);
                for dx in [-2, 0, 1, 3, 5] {
                    for dy in [-2, 0, 1, 3, 5] {
                        for iw in [0, 1, 3, 7] {
                            for ih in [0, 1, 3, 7] {
                                let interior = Rect::new(x + dx, y + dy, iw, ih);
                                let mut actual = BTreeSet::new();
                                for strip in rect_difference(outer, interior) {
                                    assert!(strip.width >= 0 && strip.height >= 0);
                                    for point in cells(strip) {
                                        assert!(actual.insert(point), "overlapping pieces");
                                    }
                                }
                                let expected: BTreeSet<_> = cells(outer)
                                    .filter(|&(px, py)| !interior.contains_point(px, py))
                                    .collect();
                                assert_eq!(actual, expected, "{outer:?} minus {interior:?}");
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn rectangle_difference_preserves_strip_order() {
    assert_eq!(
        rect_difference(Rect::new(0, 0, 10, 10), Rect::new(2, 3, 4, 5)),
        [
            Rect::new(0, 0, 2, 10),
            Rect::new(2, 0, 4, 3),
            Rect::new(2, 8, 4, 2),
            Rect::new(6, 0, 4, 10),
        ]
    );
}
