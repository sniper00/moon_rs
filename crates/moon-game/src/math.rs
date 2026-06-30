//! Minimal geometry primitives used by the gameplay algorithms.
//!
//! Ported from the C++ `moon::rect` / `moon::vector2` in the original AOI
//! implementation, but trimmed to exactly what the algorithms here need.

use std::ops::{Add, Mul, Sub};

/// Axis-aligned integer rectangle: origin at `(x, y)` with `width`/`height`.
///
/// Mirrors the semantics of the C++ `rect<int>`: `right()`/`top()` are
/// exclusive, and `contains_point` uses a half-open interval `[x, right())`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rect {
    pub x: i32,
    pub y: i32,
    pub width: i32,
    pub height: i32,
}

impl Rect {
    #[inline]
    pub const fn new(x: i32, y: i32, width: i32, height: i32) -> Self {
        Self {
            x,
            y,
            width,
            height,
        }
    }

    #[inline]
    pub const fn left(&self) -> i32 {
        self.x
    }

    #[inline]
    pub const fn bottom(&self) -> i32 {
        self.y
    }

    #[inline]
    pub const fn right(&self) -> i32 {
        self.x + self.width
    }

    #[inline]
    pub const fn top(&self) -> i32 {
        self.y + self.height
    }

    /// Matches C++ `rect::empty()` for integers (epsilon collapses to 0).
    #[inline]
    pub const fn empty(&self) -> bool {
        self.width <= 0 && self.height <= 0
    }

    /// Half-open point containment: `x <= px < right() && y <= py < top()`.
    #[inline]
    pub const fn contains_point(&self, px: i32, py: i32) -> bool {
        px >= self.x && px < self.right() && py >= self.y && py < self.top()
    }

    /// Whether `rc` is fully contained within `self`.
    #[inline]
    pub const fn contains_rect(&self, rc: &Rect) -> bool {
        self.x <= rc.x && self.y <= rc.y && rc.right() <= self.right() && rc.top() <= self.top()
    }

    /// Whether two half-open rectangles overlap. Consistent with
    /// [`contains_point`](Self::contains_point): modeling a point `(px, py)` as a
    /// unit cell `[px, px+1) x [py, py+1)` makes overlap equivalent to point
    /// containment, so this unifies point and area visibility tests.
    #[inline]
    pub const fn intersects_halfopen(&self, rc: &Rect) -> bool {
        self.x < rc.right() && rc.x < self.right() && self.y < rc.top() && rc.y < self.top()
    }

    /// Intersection of two rectangles (C++ `rect::join`).
    ///
    /// Returns an empty rect if either operand is empty. The result may have a
    /// non-positive width/height when the rectangles are disjoint; this is
    /// intentional to stay faithful to the original algorithm.
    #[inline]
    pub fn join(&self, other: &Rect) -> Rect {
        if self.empty() || other.empty() {
            return Rect::default();
        }
        let minx = self.x.max(other.x);
        let miny = self.y.max(other.y);
        let maxx = (self.x + self.width).min(other.x + other.width);
        let maxy = (self.y + self.height).min(other.y + other.height);
        Rect::new(minx, miny, maxx - minx, maxy - miny)
    }
}

/// 2D float vector used by ray casting.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
}

impl Vec2 {
    #[inline]
    pub const fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }

    #[inline]
    pub fn distance(&self, other: &Vec2) -> f32 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        (dx * dx + dy * dy).sqrt()
    }

    /// Returns a normalized copy. Matches C++ `vector2::normalize` edge cases:
    /// already-unit vectors and near-zero vectors are returned unchanged.
    #[inline]
    pub fn normalized(self) -> Vec2 {
        let n = self.x * self.x + self.y * self.y;
        if n == 1.0 {
            return self;
        }
        let n = n.sqrt();
        if n < f32::MIN_POSITIVE {
            return self;
        }
        let inv = 1.0 / n;
        Vec2::new(self.x * inv, self.y * inv)
    }
}

impl Add for Vec2 {
    type Output = Vec2;
    #[inline]
    fn add(self, rhs: Vec2) -> Vec2 {
        Vec2::new(self.x + rhs.x, self.y + rhs.y)
    }
}

impl Sub for Vec2 {
    type Output = Vec2;
    #[inline]
    fn sub(self, rhs: Vec2) -> Vec2 {
        Vec2::new(self.x - rhs.x, self.y - rhs.y)
    }
}

impl Mul<f32> for Vec2 {
    type Output = Vec2;
    #[inline]
    fn mul(self, rhs: f32) -> Vec2 {
        Vec2::new(self.x * rhs, self.y * rhs)
    }
}
