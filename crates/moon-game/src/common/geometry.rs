use crate::math::Rect;

/// Splits `outer` minus `interior` into at most four disjoint half-open rectangles.
/// Empty entries can be skipped. Overlapping inputs produce strips in left,
/// bottom, top, right order; disjoint inputs return outer followed by empty entries.
/// No allocation is performed.
///
/// As with Rect's boundary operations, right/top coordinates must fit in i32.
/// The outer rectangle must have non-negative dimensions.
#[inline]
pub fn rect_difference(outer: Rect, interior: Rect) -> [Rect; 4] {
    let left = interior.left().clamp(outer.left(), outer.right());
    let right = interior.right().clamp(outer.left(), outer.right());
    let bottom = interior.bottom().clamp(outer.bottom(), outer.top());
    let top = interior.top().clamp(outer.bottom(), outer.top());
    if left >= right || bottom >= top {
        return [outer, Rect::default(), Rect::default(), Rect::default()];
    }
    [
        Rect::new(
            outer.left(),
            outer.bottom(),
            left - outer.left(),
            outer.height,
        ),
        Rect::new(left, outer.bottom(), right - left, bottom - outer.bottom()),
        Rect::new(left, top, right - left, outer.top() - top),
        Rect::new(right, outer.bottom(), outer.right() - right, outer.height),
    ]
}
