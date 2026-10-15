use std::ops::Deref;

use super::{AoiConfig, AoiError, AoiObject, FIXED, Rect};

/// Distinct units prevent accidentally feeding tile indices into world geometry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct WorldRect(pub Rect);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct TileRect(pub Rect);

impl Deref for WorldRect {
    type Target = Rect;
    fn deref(&self) -> &Rect {
        &self.0
    }
}

impl Deref for TileRect {
    type Target = Rect;
    fn deref(&self) -> &Rect {
        &self.0
    }
}

impl TileRect {
    /// Visits cells in row-major order, preserving event and candidate order.
    #[inline]
    pub fn for_each(self, mut visit: impl FnMut(i32, i32)) {
        for y in self.bottom()..self.top() {
            for x in self.left()..self.right() {
                visit(x, y);
            }
        }
    }

    /// Watcher boundary scans use column-major order within each rectangle.
    #[inline]
    pub fn for_each_column_major(self, mut visit: impl FnMut(i32, i32)) {
        for x in self.left()..self.right() {
            for y in self.bottom()..self.top() {
                visit(x, y);
            }
        }
    }

    #[inline]
    pub fn outside(self, interior: Self) -> [Self; 4] {
        crate::common::rect_difference(self.0, interior.0).map(Self)
    }
}

impl WorldRect {
    #[inline]
    pub fn join(self, other: &Self) -> Self {
        Self(self.0.join(&other.0))
    }
}

#[derive(Clone, Copy)]
pub(super) struct Grid {
    pub bounds: Rect,
    pub tile_size: i32,
    pub count: i32,
}

impl Grid {
    pub fn new(config: AoiConfig) -> Result<Self, AoiError> {
        let AoiConfig {
            origin: (x, y),
            map_size,
            tile_size,
        } = config;
        if map_size <= 0 {
            return Err(AoiError::InvalidMapSize);
        }
        if tile_size <= 0 {
            return Err(AoiError::InvalidTileSize);
        }
        if map_size % tile_size != 0 {
            return Err(AoiError::UnalignedMapSize);
        }
        if x.checked_add(map_size).is_none() || y.checked_add(map_size).is_none() {
            return Err(AoiError::CoordinateOverflow);
        }
        let count = map_size / tile_size;
        if count.checked_mul(count).is_none() {
            return Err(AoiError::TooManyTiles);
        }
        Ok(Self {
            bounds: Rect::new(x, y, map_size, map_size),
            tile_size,
            count,
        })
    }

    pub fn len(self) -> usize {
        (self.count * self.count) as usize
    }

    #[inline]
    pub fn tile_x(self, x: i32) -> i32 {
        ((x.clamp(self.bounds.x, self.bounds.right()) - self.bounds.x) / self.tile_size)
            .min(self.count - 1)
    }

    #[inline]
    pub fn tile_y(self, y: i32) -> i32 {
        ((y.clamp(self.bounds.y, self.bounds.top()) - self.bounds.y) / self.tile_size)
            .min(self.count - 1)
    }

    #[inline]
    pub fn tile_world(self, x: i32, y: i32) -> WorldRect {
        WorldRect(Rect::new(
            self.bounds.x + x * self.tile_size,
            self.bounds.y + y * self.tile_size,
            self.tile_size,
            self.tile_size,
        ))
    }

    /// Tiles wholly contained in a clipped world rectangle and its tile cover.
    /// `cover` must come from `tiles(view)` or matching `view_tiles` arguments.
    /// Unlike `tiles`,
    /// partially covered boundary cells are excluded, so they still get exact
    /// visibility checks even when a watcher moves without crossing a tile.
    #[inline]
    pub fn interior_tiles(self, view: WorldRect, cover: TileRect) -> TileRect {
        if view.empty() {
            return TileRect::default();
        }
        // Reuse the already computed cover instead of dividing coordinates
        // again. The cover includes the right/top boundary tile, except when
        // that boundary coincides with the end of the map.
        let left =
            cover.left() + i32::from(view.left() != self.bounds.x + cover.left() * self.tile_size);
        let bottom = cover.bottom()
            + i32::from(view.bottom() != self.bounds.y + cover.bottom() * self.tile_size);
        let right = cover.right() - i32::from(view.right() != self.bounds.right());
        let top = cover.top() - i32::from(view.top() != self.bounds.top());
        TileRect(Rect::new(
            left,
            bottom,
            (right - left).max(0),
            (top - bottom).max(0),
        ))
    }

    /// Object centers are inside the validated map. Limit each half-extent
    /// before adding/subtracting it, avoiding both overflow and saturating ops
    /// in per-candidate visibility checks.
    #[cfg_attr(feature = "prof_noinline", inline(never))]
    #[cfg_attr(not(feature = "prof_noinline"), inline)]
    pub fn view(self, x: i32, y: i32, w: i32, h: i32) -> WorldRect {
        let left = (w / 2).min(x - self.bounds.x);
        let bottom = (h / 2).min(y - self.bounds.y);
        if w == 0 || h == 0 {
            return WorldRect(Rect::new(x - left, y - bottom, 0, 0));
        }
        let right = (w / 2).min(self.bounds.right() - x);
        let top = (h / 2).min(self.bounds.top() - y);
        WorldRect(Rect::new(x - left, y - bottom, left + right, bottom + top))
    }

    /// Compatibility geometry helpers accept arbitrary centers, unlike stored
    /// objects. Use wide arithmetic at this boundary rather than in hot scans.
    pub fn clipped_view(self, x: i32, y: i32, w: i32, h: i32) -> WorldRect {
        let x = i64::from(x);
        let y = i64::from(y);
        let left = (x - i64::from(w / 2))
            .clamp(i64::from(self.bounds.x), i64::from(self.bounds.right()))
            as i32;
        let bottom = (y - i64::from(h / 2))
            .clamp(i64::from(self.bounds.y), i64::from(self.bounds.top()))
            as i32;
        if w <= 0 || h <= 0 {
            return WorldRect(Rect::new(left, bottom, 0, 0));
        }
        let right = (x + i64::from(w / 2))
            .clamp(i64::from(self.bounds.x), i64::from(self.bounds.right()))
            as i32;
        let top = (y + i64::from(h / 2))
            .clamp(i64::from(self.bounds.y), i64::from(self.bounds.top())) as i32;
        WorldRect(Rect::new(left, bottom, right - left, top - bottom))
    }

    #[inline]
    pub fn view_tiles(self, x: i32, y: i32, w: i32, h: i32) -> TileRect {
        let left = self.tile_x(x.saturating_sub(w / 2));
        let bottom = self.tile_y(y.saturating_sub(h / 2));
        if w <= 0 || h <= 0 {
            return TileRect(Rect::new(left, bottom, 0, 0));
        }
        let right = self.tile_x(x.saturating_add(w / 2)) + 1;
        let top = self.tile_y(y.saturating_add(h / 2)) + 1;
        TileRect(Rect::new(left, bottom, right - left, top - bottom))
    }

    #[inline]
    pub fn tiles(self, rect: WorldRect) -> TileRect {
        if rect.empty() {
            return TileRect::default();
        }
        let left = self.tile_x(rect.left());
        let bottom = self.tile_y(rect.bottom());
        let right = self.tile_x(rect.right()) + 1;
        let top = self.tile_y(rect.top()) + 1;
        TileRect(Rect::new(left, bottom, right - left, top - bottom))
    }

    #[inline]
    pub fn marker_cover(self, obj: &AoiObject) -> TileRect {
        let mut cover = if obj.is_range_marker() {
            self.view_tiles(obj.x, obj.y, obj.w, obj.h)
        } else {
            TileRect(Rect::new(
                (obj.x - self.bounds.x) / self.tile_size,
                (obj.y - self.bounds.y) / self.tile_size,
                1,
                1,
            ))
        };
        if obj.mode & FIXED != 0 && obj.w > 0 && obj.h > 0 {
            // Ray footprints start at (x,y); the index contains both shapes.
            let right = self.tile_x(obj.x.saturating_add(obj.w)) + 1;
            let top = self.tile_y(obj.y.saturating_add(obj.h)) + 1;
            cover.0.width = cover.right().max(right) - cover.x;
            cover.0.height = cover.top().max(top) - cover.y;
        }
        cover
    }

    #[inline]
    pub fn marker_area(self, obj: &AoiObject) -> Option<WorldRect> {
        obj.is_range_marker()
            .then(|| self.view(obj.x, obj.y, obj.w, obj.h))
    }

    /// Stored point markers are inside the map, so clipping a watcher's view
    /// cannot change containment. Compare offsets without building a rectangle.
    #[cfg_attr(feature = "prof_noinline", inline(never))]
    #[cfg_attr(not(feature = "prof_noinline"), inline)]
    pub fn sees_marker(self, watcher: &AoiObject, x: i32, y: i32, area: Option<WorldRect>) -> bool {
        if let Some(area) = area {
            return self
                .view(watcher.x, watcher.y, watcher.w, watcher.h)
                .intersects_halfopen(&area);
        }
        let dx = i64::from(x) - i64::from(watcher.x);
        let dy = i64::from(y) - i64::from(watcher.y);
        let half_w = i64::from(watcher.w / 2);
        let half_h = i64::from(watcher.h / 2);
        dx >= -half_w && dx < half_w && dy >= -half_h && dy < half_h
    }
}
