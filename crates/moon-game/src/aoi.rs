//! Grid-based AOI (Area of Interest) system.
//!
//! Rust port of the C++ `moon::aoi<AoiObject>` template. The map is divided
//! into `count * count` square tiles; each tile tracks the set of `markers`
//! (observable objects) and `watchers` (observers with a view rectangle) that
//! currently overlap it. `insert` / `update` / `erase` / `fire_event` compare
//! old vs. new view rectangles and push `enter` / `leave` / `pos` events into an
//! event queue that callers drain via [`Aoi::events`] / [`Aoi::clear_event`].
//!
//! # Visibility model
//!
//! A watcher sees a marker when (a) the watcher's `layer <= marker.layer` and
//! (b) their shapes overlap. Point markers (`w == 0 || h == 0`, or watcher+marker
//! objects) are tested by half-open point containment of the marker center;
//! **range markers** (marker-only with positive `w`/`h`) occupy an area and are
//! tested by half-open rectangle intersection, so a watcher whose view clips the
//! edge of a range marker still sees it. Because a range marker spans several
//! tiles, every scan that crosses multiple of its tiles is de-duplicated so each
//! watcher/marker pair produces at most one event per operation.
//!
//! # Porting design (slab storage)
//!
//! The C++ version stores objects in a node-stable `std::unordered_map` and
//! keeps raw `object*` pointers inside each tile. Rust's `HashMap` does not
//! provide stable value addresses, so objects live in a dense slab
//! (`Vec<AoiObject>`) and each tile stores a [`Slot`] (a `u32` index into the
//! slab) instead of a pointer. A `HashMap<Handle, Slot>` backs the public,
//! handle-based API. The hot emit loops therefore resolve an object with a
//! single `slab[slot]` array access — the direct analogue of the C++ pointer
//! deref — rather than a per-element hash lookup. Hot paths still copy the small
//! `Copy` [`AoiObject`] out once and operate on disjoint struct fields, keeping
//! the whole implementation free of `unsafe`.

// Fx hashing (no random seed) instead of std's SipHash: the maps/sets are keyed
// by small integer handles/slots and are extremely hot (per-tile membership
// churn and per-op dedup), where SipHash dominates. FxHashMap/FxHashSet are
// drop-in.
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::math::{Rect, Vec2};

/// Object identifier. Matches the C++ `object_handle_type` (`int64_t`).
pub type Handle = i64;

// ── mode flags (object roles) ──────────────────────────────────────────────
/// Observer: has a view rectangle and receives enter/leave/pos events.
pub const WATCHER: i32 = 1;
/// Observable target that watchers can see.
pub const MARKER: i32 = 1 << 1;
/// Static object (obstacle/NPC). Drives tile versions and ray casting; skipped
/// by `update_watcher` enter/leave generation.
pub const FIXED: i32 = 1 << 2;
/// Temporarily invisible: produces no events while set.
pub const HIDE: i32 = 1 << 3;

// ── event ids ──────────────────────────────────────────────────────────────
pub const EVENT_ENTER: i32 = 1;
pub const EVENT_LEAVE: i32 = 2;
pub const EVENT_POS: i32 = 3;

// ── option flags ───────────────────────────────────────────────────────────
/// Emit leave events (off by default to save bandwidth in many games).
pub const ENABLE_LEAVE_EVENT: i32 = 1 << 0;
/// Allow an object that is both watcher and marker to observe itself.
pub const ENABLE_SELF_EVENT: i32 = 1 << 1;
/// Reserved to mirror the C++ debug flag; currently a no-op.
pub const ENABLE_DEBUG: i32 = 1 << 2;

/// How a watcher's view changed, controlling which events a tile scan emits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Vtm {
    ZoomIn,
    ZoomOut,
    Layer,
}

/// An object tracked by the AOI grid.
///
/// All fields are plain integers so the struct is `Copy`; this is what lets the
/// hot paths snapshot an object and then freely borrow the other containers.
/// `user` carries opaque caller data (the C++ template's extra constructor
/// args, e.g. an object "type").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AoiObject {
    pub x: i32,
    pub y: i32,
    pub w: i32,
    pub h: i32,
    pub layer: i32,
    pub mode: i32,
    pub handle: Handle,
    pub user: i64,
}

impl AoiObject {
    /// Footprint containment of a point (C++ `test_object::contains`), using the
    /// object's own `[x, x+w) x [y, y+h)` box. Used by ray casting.
    #[inline]
    fn contains(&self, px: f32, py: f32) -> bool {
        px >= self.x as f32
            && px < (self.x + self.w) as f32
            && py >= self.y as f32
            && py < (self.y + self.h) as f32
    }
}

/// A queued visibility event. `marker` is `0` for position-based events fired
/// via [`Aoi::fire_event_pos`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AoiEvent {
    pub eventid: i32,
    pub watcher: Handle,
    pub marker: Handle,
}

impl AoiEvent {
    #[inline]
    fn new(eventid: i32, watcher: Handle, marker: Handle) -> Self {
        Self {
            eventid,
            watcher,
            marker,
        }
    }
}

/// Dense slab index into [`Aoi::slab`]. Stored inside tiles (instead of a
/// [`Handle`]) so the hot emit loops resolve an object with a single array index
/// — the Rust equivalent of the C++ version's `object*` stored per tile.
type Slot = u32;

#[derive(Clone, Debug)]
struct Tile {
    version: i64,
    markers: HashSet<Slot>,
    watchers: HashSet<Slot>,
}

impl Default for Tile {
    fn default() -> Self {
        // C++ tiles start at version 1.
        Self {
            version: 1,
            markers: HashSet::default(),
            watchers: HashSet::default(),
        }
    }
}

/// Grid-based Area of Interest system. See the module docs for an overview.
pub struct Aoi {
    option: i32,
    rect: Rect,
    tile_size: i32,
    map_size: i32,
    count: i32, // map_size / tile_size
    data: Vec<Tile>,
    /// Dense object storage. Tiles reference objects by [`Slot`] (index here), so
    /// the hot emit loops do a direct `slab[slot]` array access instead of a
    /// per-element `HashMap` lookup. Slots are stable for an object's lifetime;
    /// freed slots are recycled via `free`.
    slab: Vec<AoiObject>,
    /// Recycled (vacant) slab slots, reused before growing `slab`.
    free: Vec<Slot>,
    /// Handle → slot mapping for the public, handle-based API.
    index: HashMap<Handle, Slot>,
    event_queue: Vec<AoiEvent>,
    /// Reusable dedup buffer for event emission. Every `insert`/`update`/`erase`/
    /// `fire_event` needs a per-op "already notified" set; allocating a fresh
    /// `HashSet` each call dominated profiles (hashbrown resize churn). We
    /// `mem::take` this out, use it, and put it back so its capacity is retained
    /// across calls (no more reallocation after warm-up). Uses are never nested.
    scratch: HashSet<Slot>,
}

impl Aoi {
    /// Creates a map at origin `(posx, posy)` of `map_size x map_size`, split
    /// into square tiles of `tile_size`.
    ///
    /// # Panics
    /// If `map_size` is not an exact multiple of `tile_size`, or `tile_size` is
    /// not positive.
    pub fn new(posx: i32, posy: i32, map_size: i32, tile_size: i32) -> Self {
        assert!(tile_size > 0, "tile_size must be positive");
        assert!(
            map_size % tile_size == 0,
            "map_size must be a multiple of tile_size"
        );
        let count = map_size / tile_size;
        let len = (count * count) as usize;
        let mut data = Vec::with_capacity(len);
        data.resize_with(len, Tile::default);
        Self {
            option: 0,
            rect: Rect::new(posx, posy, map_size, map_size),
            tile_size,
            map_size,
            count,
            data,
            slab: Vec::new(),
            free: Vec::new(),
            index: HashMap::default(),
            event_queue: Vec::new(),
            scratch: HashSet::default(),
        }
    }

    // ── slab management ────────────────────────────────────────────────────────

    /// Allocates a slab slot for `obj` (recycling a freed one when available),
    /// registers its handle, and returns the slot.
    #[inline]
    fn alloc_slot(&mut self, obj: AoiObject) -> Slot {
        let slot = if let Some(s) = self.free.pop() {
            self.slab[s as usize] = obj;
            s
        } else {
            let s = self.slab.len() as Slot;
            self.slab.push(obj);
            s
        };
        self.index.insert(obj.handle, slot);
        slot
    }

    // ── geometry (free-standing so hot loops avoid borrowing all of `self`) ──

    #[inline]
    fn calc_tile_x(map: &Rect, tile_size: i32, count: i32, v: i32) -> i32 {
        let res = (v - map.x) / tile_size;
        res.clamp(0, count - 1)
    }

    #[inline]
    fn calc_tile_y(map: &Rect, tile_size: i32, count: i32, v: i32) -> i32 {
        let res = (v - map.y) / tile_size;
        res.clamp(0, count - 1)
    }

    #[cfg_attr(feature = "prof_noinline", inline(never))]
    fn calc_make_rect(map: &Rect, x: i32, y: i32, w: i32, h: i32) -> Rect {
        let left = (x - w / 2).clamp(map.left(), map.right());
        let right = (x + w / 2).clamp(map.left(), map.right());
        let bottom = (y - h / 2).clamp(map.bottom(), map.top());
        let top = (y + h / 2).clamp(map.bottom(), map.top());
        if w == 0 || h == 0 {
            return Rect::new(left, bottom, 0, 0);
        }
        Rect::new(left, bottom, right - left, top - bottom)
    }

    fn calc_make_tile_rect(
        map: &Rect,
        tile_size: i32,
        count: i32,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
    ) -> Rect {
        let left = Self::calc_tile_x(map, tile_size, count, (x - w / 2).clamp(map.left(), map.right()));
        let right =
            Self::calc_tile_x(map, tile_size, count, (x + w / 2).clamp(map.left(), map.right())) + 1;
        let bottom =
            Self::calc_tile_y(map, tile_size, count, (y - h / 2).clamp(map.bottom(), map.top()));
        let top =
            Self::calc_tile_y(map, tile_size, count, (y + h / 2).clamp(map.bottom(), map.top())) + 1;
        if w == 0 || h == 0 {
            return Rect::new(left, bottom, 0, 0);
        }
        Rect::new(left, bottom, right - left, top - bottom)
    }

    fn calc_to_tile_rect(
        map: &Rect,
        tile_size: i32,
        count: i32,
        left_x: i32,
        left_y: i32,
        w: i32,
        h: i32,
    ) -> Rect {
        let left = Self::calc_tile_x(map, tile_size, count, left_x.clamp(map.left(), map.right()));
        let right =
            Self::calc_tile_x(map, tile_size, count, (left_x + w).clamp(map.left(), map.right())) + 1;
        let bottom = Self::calc_tile_y(map, tile_size, count, left_y.clamp(map.bottom(), map.top()));
        let top =
            Self::calc_tile_y(map, tile_size, count, (left_y + h).clamp(map.bottom(), map.top())) + 1;
        if w == 0 || h == 0 {
            return Rect::new(left, bottom, 0, 0);
        }
        Rect::new(left, bottom, right - left, top - bottom)
    }

    /// Tile column index of map coordinate `v`, clamped to `[0, count)`.
    #[inline]
    pub fn get_tile_x(&self, v: i32) -> i32 {
        Self::calc_tile_x(&self.rect, self.tile_size, self.count, v)
    }

    /// Tile row index of map coordinate `v`, clamped to `[0, count)`.
    #[inline]
    pub fn get_tile_y(&self, v: i32) -> i32 {
        Self::calc_tile_y(&self.rect, self.tile_size, self.count, v)
    }

    /// View rectangle in world coordinates, centered on `(x, y)` with size
    /// `w x h`, clamped to the map.
    pub fn make_rect(&self, x: i32, y: i32, w: i32, h: i32) -> Rect {
        Self::calc_make_rect(&self.rect, x, y, w, h)
    }

    /// View rectangle expressed in tile indices.
    pub fn make_tile_rect(&self, x: i32, y: i32, w: i32, h: i32) -> Rect {
        Self::calc_make_tile_rect(&self.rect, self.tile_size, self.count, x, y, w, h)
    }

    // ── insert ──────────────────────────────────────────────────────────────

    /// Inserts an object. Returns `false` if `(x, y)` is outside the map or the
    /// `handle` already exists.
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &mut self,
        handle: Handle,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
        layer: i32,
        mode: i32,
        user: i64,
    ) -> bool {
        if !self.rect.contains_point(x, y) {
            return false;
        }
        if self.index.contains_key(&handle) {
            return false;
        }
        let obj = AoiObject {
            x,
            y,
            w,
            h,
            layer,
            mode,
            handle,
            user,
        };
        let slot = self.alloc_slot(obj);

        let map = self.rect;
        let count = self.count;
        let tile_size = self.tile_size;

        let tile_x = Self::calc_tile_x(&map, tile_size, count, x);
        let tile_y = Self::calc_tile_y(&map, tile_size, count, y);

        if mode & MARKER != 0 {
            let is_range = Self::is_range_marker(mode, w, h);
            let cover = if is_range {
                Self::calc_make_tile_rect(&map, tile_size, count, x, y, w, h)
            } else {
                Rect::new(tile_x, tile_y, 1, 1)
            };
            self.add_marker_tiles(cover, slot);
            if mode & HIDE == 0 {
                let area = if is_range {
                    Some(Self::calc_make_rect(&map, x, y, w, h))
                } else {
                    None
                };
                self.emit_to_watchers(cover, x, y, area, layer, handle, EVENT_ENTER);
            }
        }

        if mode & WATCHER != 0 {
            let tr = Self::calc_make_tile_rect(&map, tile_size, count, x, y, w, h);
            let rc = Self::calc_make_rect(&map, x, y, w, h);
            let option = self.option;
            let mut seen = std::mem::take(&mut self.scratch);
            seen.clear();
            for i in tr.left()..tr.right() {
                for j in tr.bottom()..tr.top() {
                    let index = (j * count + i) as usize;
                    self.data[index].watchers.insert(slot);
                    Self::update_watcher(
                        &map,
                        &self.slab,
                        option,
                        &mut self.event_queue,
                        &self.data[index],
                        Rect::default(),
                        rc,
                        &obj,
                        Vtm::ZoomOut,
                        obj.layer,
                        &mut seen,
                    );
                }
            }
            self.scratch = seen;
        }
        true
    }

    // ── events triggered explicitly ──────────────────────────────────────────

    /// Re-fires `eventid` to every watcher that can currently see object
    /// `handle` (used e.g. to notify after attribute changes).
    pub fn fire_event(&mut self, handle: Handle, eventid: i32) {
        let obj = match self.index.get(&handle) {
            Some(&s) => self.slab[s as usize],
            None => return,
        };
        let map = self.rect;
        let count = self.count;
        let tile_size = self.tile_size;

        let is_range = Self::is_range_marker(obj.mode, obj.w, obj.h);
        let cover = if is_range {
            Self::calc_make_tile_rect(&map, tile_size, count, obj.x, obj.y, obj.w, obj.h)
        } else {
            let tx = Self::calc_tile_x(&map, tile_size, count, obj.x);
            let ty = Self::calc_tile_y(&map, tile_size, count, obj.y);
            Rect::new(tx, ty, 1, 1)
        };

        for i in cover.left()..cover.right() {
            for j in cover.bottom()..cover.top() {
                let index = (j * count + i) as usize;
                Self::update_tile_version(&obj, &mut self.data[index]);
            }
        }

        if obj.mode & HIDE != 0 {
            return;
        }
        let area = if is_range {
            Some(Self::calc_make_rect(&map, obj.x, obj.y, obj.w, obj.h))
        } else {
            None
        };
        self.emit_to_watchers(cover, obj.x, obj.y, area, obj.layer, handle, eventid);
    }

    /// Fires `eventid` to every watcher whose view contains the point `(x, y)`.
    /// The emitted events carry `marker = 0`.
    pub fn fire_event_pos(&mut self, x: i32, y: i32, eventid: i32) {
        let map = self.rect;
        let count = self.count;
        let tile_size = self.tile_size;

        let tile_x = Self::calc_tile_x(&map, tile_size, count, x);
        let tile_y = Self::calc_tile_y(&map, tile_size, count, y);
        let index = (tile_y * count + tile_x) as usize;

        let node = &self.data[index];
        for &wh in &node.watchers {
            let w = &self.slab[wh as usize];
            let rc = Self::calc_make_rect(&map, w.x, w.y, w.w, w.h);
            if !rc.contains_point(x, y) {
                continue;
            }
            self.event_queue.push(AoiEvent::new(eventid, w.handle, 0));
        }
    }

    /// Toggles the hide flag, emitting a leave (show→hide) or enter (hide→show)
    /// burst to the affected watchers.
    pub fn set_hide(&mut self, handle: Handle, v: bool) {
        let slot = match self.index.get(&handle) {
            Some(&s) => s,
            None => return,
        };
        let mode = self.slab[slot as usize].mode;
        if v && (mode & HIDE == 0) {
            self.fire_event(handle, EVENT_LEAVE);
            self.slab[slot as usize].mode |= HIDE;
        }
        if !v && (mode & HIDE != 0) {
            self.slab[slot as usize].mode &= !HIDE;
            self.fire_event(handle, EVENT_ENTER);
        }
    }

    // ── update (position / view size / layer) ─────────────────────────────────

    /// Updates an object's position, view size, and layer, emitting the
    /// resulting visibility events. Returns `false` for unknown handles or
    /// negative dimensions. Coordinates are clamped to the map.
    pub fn update(&mut self, handle: Handle, x: i32, y: i32, w: i32, h: i32, layer: i32) -> bool {
        if w < 0 || h < 0 {
            return false;
        }
        let slot = match self.index.get(&handle) {
            Some(&s) => s,
            None => return false,
        };

        let map = self.rect;
        let count = self.count;
        let tile_size = self.tile_size;
        let option = self.option;

        let x = x.clamp(map.x, map.x + self.map_size - 1);
        let y = y.clamp(map.y, map.y + self.map_size - 1);

        let prev = self.slab[slot as usize];
        let old_view = Self::calc_make_rect(&map, prev.x, prev.y, prev.w, prev.h);
        let old_tile_view =
            Self::calc_make_tile_rect(&map, tile_size, count, prev.x, prev.y, prev.w, prev.h);
        let old_x = prev.x;
        let old_y = prev.y;
        let old_layer = prev.layer;

        {
            let o = &mut self.slab[slot as usize];
            o.x = x;
            o.y = y;
            o.h = h;
            o.w = w;
            if layer >= 0 {
                o.layer = layer;
            }
        }
        let obj = self.slab[slot as usize];

        if obj.mode & MARKER != 0 {
            self.update_marker(slot, old_x, old_y, prev.w, prev.h, old_layer);
        }

        if obj.mode & WATCHER == 0 {
            return true;
        }

        // Reusable dedup buffer (see the `scratch` field): taken once here,
        // cleared before each tile-scan loop, and put back at every exit so its
        // capacity persists across calls instead of reallocating every update.
        let mut seen = std::mem::take(&mut self.scratch);

        let new_view = Self::calc_make_rect(&map, x, y, w, h);
        let new_tile_view = Self::calc_make_tile_rect(&map, tile_size, count, x, y, w, h);

        // watcher zoom in: new view fully inside old view
        if old_view.contains_rect(&new_view) {
            seen.clear();
            for i in old_tile_view.left()..old_tile_view.right() {
                for j in old_tile_view.bottom()..old_tile_view.top() {
                    let rc = Rect::new(i * tile_size, j * tile_size, tile_size, tile_size);
                    if new_view.contains_rect(&rc) {
                        continue;
                    }
                    let index = (j * count + i) as usize;
                    if !new_tile_view.contains_point(i, j) {
                        self.data[index].watchers.remove(&slot);
                    }
                    Self::update_watcher(
                        &map,
                        &self.slab,
                        option,
                        &mut self.event_queue,
                        &self.data[index],
                        old_view,
                        new_view,
                        &obj,
                        Vtm::ZoomIn,
                        old_layer,
                        &mut seen,
                    );
                }
            }
            if old_layer != obj.layer {
                seen.clear();
                for i in new_tile_view.left()..new_tile_view.right() {
                    for j in new_tile_view.bottom()..new_tile_view.top() {
                        let index = (j * count + i) as usize;
                        Self::update_watcher(
                            &map,
                            &self.slab,
                            option,
                            &mut self.event_queue,
                            &self.data[index],
                            Rect::default(),
                            new_view,
                            &obj,
                            Vtm::Layer,
                            old_layer,
                            &mut seen,
                        );
                    }
                }
            }
            self.scratch = seen;
            return true;
        }

        // watcher zoom out: old view fully inside new view
        if new_view.contains_rect(&old_view) {
            seen.clear();
            for i in new_tile_view.left()..new_tile_view.right() {
                for j in new_tile_view.bottom()..new_tile_view.top() {
                    let rc = Rect::new(i * tile_size, j * tile_size, tile_size, tile_size);
                    if old_view.contains_rect(&rc) {
                        continue;
                    }
                    let index = (j * count + i) as usize;
                    if !old_tile_view.contains_point(i, j) {
                        self.data[index].watchers.insert(slot);
                    }
                    Self::update_watcher(
                        &map,
                        &self.slab,
                        option,
                        &mut self.event_queue,
                        &self.data[index],
                        old_view,
                        new_view,
                        &obj,
                        Vtm::ZoomOut,
                        old_layer,
                        &mut seen,
                    );
                }
            }
            if old_layer != obj.layer {
                seen.clear();
                for i in old_tile_view.left()..old_tile_view.right() {
                    for j in old_tile_view.bottom()..old_tile_view.top() {
                        let index = (j * count + i) as usize;
                        Self::update_watcher(
                            &map,
                            &self.slab,
                            option,
                            &mut self.event_queue,
                            &self.data[index],
                            Rect::default(),
                            old_view,
                            &obj,
                            Vtm::Layer,
                            old_layer,
                            &mut seen,
                        );
                    }
                }
            }
            self.scratch = seen;
            return true;
        }

        // general case: partial overlap (or disjoint) views
        let join_area = new_view.join(&old_view);
        let join_tile_area = Self::calc_to_tile_rect(
            &map,
            tile_size,
            count,
            join_area.x,
            join_area.y,
            join_area.width,
            join_area.height,
        );

        seen.clear();
        for i in old_tile_view.left()..old_tile_view.right() {
            for j in old_tile_view.bottom()..old_tile_view.top() {
                let rc = Rect::new(i * tile_size, j * tile_size, tile_size, tile_size);
                if join_area.contains_rect(&rc) {
                    continue;
                }
                let index = (j * count + i) as usize;
                if join_tile_area.empty() || !join_tile_area.contains_point(i, j) {
                    self.data[index].watchers.remove(&slot);
                }
                Self::update_watcher(
                    &map,
                            &self.slab,
                    option,
                    &mut self.event_queue,
                    &self.data[index],
                    old_view,
                    join_area,
                    &obj,
                    Vtm::ZoomIn,
                    old_layer,
                    &mut seen,
                );
            }
        }

        seen.clear();
        for i in new_tile_view.left()..new_tile_view.right() {
            for j in new_tile_view.bottom()..new_tile_view.top() {
                let rc = Rect::new(i * tile_size, j * tile_size, tile_size, tile_size);
                if join_area.contains_rect(&rc) {
                    continue;
                }
                let index = (j * count + i) as usize;
                if join_tile_area.empty() || !join_tile_area.contains_point(i, j) {
                    self.data[index].watchers.insert(slot);
                }
                Self::update_watcher(
                    &map,
                            &self.slab,
                    option,
                    &mut self.event_queue,
                    &self.data[index],
                    join_area,
                    new_view,
                    &obj,
                    Vtm::ZoomOut,
                    old_layer,
                    &mut seen,
                );
            }
        }

        if old_layer != obj.layer {
            seen.clear();
            for i in old_tile_view.left()..old_tile_view.right() {
                for j in old_tile_view.bottom()..old_tile_view.top() {
                    let index = (j * count + i) as usize;
                    Self::update_watcher(
                        &map,
                        &self.slab,
                        option,
                        &mut self.event_queue,
                        &self.data[index],
                        Rect::default(),
                        join_area,
                        &obj,
                        Vtm::Layer,
                        old_layer,
                        &mut seen,
                    );
                }
            }
        }
        self.scratch = seen;
        true
    }

    // ── query / iteration ─────────────────────────────────────────────────────

    /// Visits all markers within the tile rectangle covering `(x, y, w, h)`.
    /// `handler` receives each marker handle and whether its tile is on the
    /// query's edge (so callers can do precise bounds re-checks on edges).
    pub fn query<F: FnMut(Handle, bool)>(&self, x: i32, y: i32, w: i32, h: i32, mut handler: F) {
        let tile_rc = Self::calc_make_tile_rect(&self.rect, self.tile_size, self.count, x, y, w, h);
        let start_x = tile_rc.x;
        let start_y = tile_rc.y;
        let end_x = tile_rc.right();
        let end_y = tile_rc.top();

        for i in start_x..end_x {
            let is_x_edge = i == start_x || i == end_x - 1;
            for j in start_y..end_y {
                let is_edge = is_x_edge || j == start_y || j == end_y - 1;
                let node = &self.data[(j * self.count + i) as usize];
                for &mh in &node.markers {
                    handler(self.slab[mh as usize].handle, is_edge);
                }
            }
        }
    }

    /// Visits every object whose `mode` matches `filter`, in tile order.
    /// `handler` receives `(handle, x, y, tile_x, tile_y)`.
    pub fn for_each_all<F: FnMut(Handle, i32, i32, i32, i32)>(&self, mut handler: F, filter: i32) {
        let count = self.count;
        for y in 0..count {
            for x in 0..count {
                let node = &self.data[(y * count + x) as usize];
                for &mh in &node.markers {
                    let m = &self.slab[mh as usize];
                    if m.mode & filter != 0 {
                        handler(m.handle, m.x, m.y, x, y);
                    }
                }
                for &wh in &node.watchers {
                    let w = &self.slab[wh as usize];
                    if w.mode & filter != 0 {
                        handler(w.handle, w.x, w.y, x, y);
                    }
                }
            }
        }
    }

    /// Casts a ray from `spos` to `epos` sampling every `step` units, returning
    /// the handle of the first `FIXED` marker whose footprint contains a sample.
    pub fn raycast(&self, spos: Vec2, epos: Vec2, step: f32) -> Option<Handle> {
        let map = self.rect;
        let count = self.count;
        let tile_size = self.tile_size;

        let d = spos.distance(&epos);
        let dir = (epos - spos).normalized();
        let mut f = 0.0_f32;
        let mut last = (-1_i32, -1_i32);
        while f < d {
            let pos = spos + dir * f;
            let tile_x = Self::calc_tile_x(&map, tile_size, count, pos.x as i32);
            let tile_y = Self::calc_tile_y(&map, tile_size, count, pos.y as i32);
            f += step;
            if (tile_x, tile_y) == last {
                continue;
            }
            last = (tile_x, tile_y);

            let node = &self.data[(tile_y * count + tile_x) as usize];
            for &mh in &node.markers {
                let m = &self.slab[mh as usize];
                if m.mode & FIXED == 0 {
                    continue;
                }
                if m.contains(pos.x, pos.y) {
                    return Some(m.handle);
                }
            }
        }
        None
    }

    // ── removal ───────────────────────────────────────────────────────────────

    /// Removes an object. When `remove` is `false`, the object is kept in the
    /// registry with its geometry zeroed (soft delete) but unlinked from all
    /// tiles.
    pub fn erase(&mut self, handle: Handle, remove: bool) {
        let slot = match self.index.get(&handle) {
            Some(&s) => s,
            None => return,
        };
        let obj = self.slab[slot as usize];
        let map = self.rect;
        let count = self.count;
        let tile_size = self.tile_size;

        if obj.mode & MARKER != 0 {
            let is_range = Self::is_range_marker(obj.mode, obj.w, obj.h);
            let cover = if is_range {
                Self::calc_make_tile_rect(&map, tile_size, count, obj.x, obj.y, obj.w, obj.h)
            } else {
                let tile_x = Self::calc_tile_x(&map, tile_size, count, obj.x);
                let tile_y = Self::calc_tile_y(&map, tile_size, count, obj.y);
                Rect::new(tile_x, tile_y, 1, 1)
            };
            self.remove_marker_tiles(cover, slot);
            if obj.mode & HIDE == 0 {
                let area = if is_range {
                    Some(Self::calc_make_rect(&map, obj.x, obj.y, obj.w, obj.h))
                } else {
                    None
                };
                self.emit_to_watchers(cover, obj.x, obj.y, area, obj.layer, handle, EVENT_LEAVE);
            }
        }

        if obj.mode & WATCHER != 0 {
            let tr = Self::calc_make_tile_rect(&map, tile_size, count, obj.x, obj.y, obj.w, obj.h);
            for i in tr.left()..tr.right() {
                for j in tr.bottom()..tr.top() {
                    let index = (j * count + i) as usize;
                    self.data[index].watchers.remove(&slot);
                }
            }
        }

        if remove {
            self.index.remove(&handle);
            self.free.push(slot);
        } else {
            let o = &mut self.slab[slot as usize];
            o.x = 0;
            o.y = 0;
            o.w = 0;
            o.h = 0;
            o.layer = 0;
        }
    }

    /// Removes all objects and clears every tile.
    pub fn clear(&mut self) {
        for n in &mut self.data {
            n.markers.clear();
            n.watchers.clear();
        }
        self.slab.clear();
        self.free.clear();
        self.index.clear();
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    #[inline]
    pub fn set_option(&mut self, option: i32) {
        self.option = option;
    }

    #[inline]
    pub fn has_object(&self, handle: Handle) -> bool {
        self.index.contains_key(&handle)
    }

    #[inline]
    pub fn find(&self, handle: Handle) -> Option<&AoiObject> {
        self.index.get(&handle).map(|&s| &self.slab[s as usize])
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.index.len()
    }

    #[inline]
    pub fn clear_event(&mut self) {
        self.event_queue.clear();
    }

    #[inline]
    pub fn events(&self) -> &[AoiEvent] {
        &self.event_queue
    }

    /// Linear tile index for `(tile_x, tile_y)`.
    #[inline]
    pub fn get_index(&self, x: i32, y: i32) -> usize {
        (y * self.count + x) as usize
    }

    /// Monotonic version counter of a tile (bumped by fixed-object changes).
    #[inline]
    pub fn get_version(&self, index: usize) -> i64 {
        self.data[index].version
    }

    /// The marker handles registered to a tile. Builds a fresh `Vec` by resolving
    /// each stored slot to its handle (tiles store dense slots internally), so
    /// this is intended for tests/diagnostics rather than hot paths.
    #[inline]
    pub fn markers(&self, index: usize) -> Vec<Handle> {
        self.data[index]
            .markers
            .iter()
            .map(|&s| self.slab[s as usize].handle)
            .collect()
    }

    // ── private helpers ───────────────────────────────────────────────────────

    #[inline]
    fn update_tile_version(obj: &AoiObject, node: &mut Tile) {
        if obj.mode & HIDE != 0 {
            return;
        }
        if obj.mode & FIXED != 0 {
            node.version += 1;
        }
    }

    /// A marker is a "range marker" (occupies an area / multiple tiles) when it
    /// is marker-only with positive extent. Watcher+marker objects are points.
    #[inline]
    fn is_range_marker(mode: i32, w: i32, h: i32) -> bool {
        (mode & WATCHER == 0) && w > 0 && h > 0
    }

    /// The marker's visibility shape: `Some(rect)` for range markers (area), or
    /// `None` for point markers (use the center point).
    #[inline]
    fn marker_area_of(map: &Rect, m: &AoiObject) -> Option<Rect> {
        if Self::is_range_marker(m.mode, m.w, m.h) {
            Some(Self::calc_make_rect(map, m.x, m.y, m.w, m.h))
        } else {
            None
        }
    }

    /// Whether a watcher `view` can see a marker: half-open area intersection for
    /// range markers, half-open point containment for point markers.
    #[cfg_attr(feature = "prof_noinline", inline(never))]
    #[cfg_attr(not(feature = "prof_noinline"), inline)]
    fn marker_seen(view: &Rect, mx: i32, my: i32, area: Option<Rect>) -> bool {
        match area {
            Some(a) => view.intersects_halfopen(&a),
            None => view.contains_point(mx, my),
        }
    }

    /// Registers a marker `slot` into every tile of `cover` (and bumps versions).
    fn add_marker_tiles(&mut self, cover: Rect, slot: Slot) {
        let obj = self.slab[slot as usize];
        let count = self.count;
        for i in cover.left()..cover.right() {
            for j in cover.bottom()..cover.top() {
                let index = (j * count + i) as usize;
                Self::update_tile_version(&obj, &mut self.data[index]);
                let inserted = self.data[index].markers.insert(slot);
                debug_assert!(inserted);
            }
        }
    }

    /// Unregisters a marker `slot` from every tile of `cover` (and bumps versions).
    fn remove_marker_tiles(&mut self, cover: Rect, slot: Slot) {
        let obj = self.slab[slot as usize];
        let count = self.count;
        for i in cover.left()..cover.right() {
            for j in cover.bottom()..cover.top() {
                let index = (j * count + i) as usize;
                Self::update_tile_version(&obj, &mut self.data[index]);
                let removed = self.data[index].markers.remove(&slot);
                debug_assert!(removed);
            }
        }
    }

    /// Emits `eventid` to every watcher (gathered uniquely across `cover` tiles)
    /// that can currently see the marker at `(mx, my)` with shape `area` and
    /// layer `mlayer`. Dedup ensures a watcher spanning several of the marker's
    /// tiles is notified at most once.
    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(feature = "prof_noinline", inline(never))]
    fn emit_to_watchers(
        &mut self,
        cover: Rect,
        mx: i32,
        my: i32,
        area: Option<Rect>,
        mlayer: i32,
        mhandle: Handle,
        eventid: i32,
    ) {
        let map = self.rect;
        let count = self.count;
        let option = self.option;
        // Dedup is only needed when the marker spans more than one tile (only
        // then can the same watcher be registered in several scanned tiles). For
        // the overwhelmingly common single-tile marker, skip the per-watcher
        // hash-set probe entirely — that hash was pure overhead vs C++.
        let need_dedup = cover.width > 1 || cover.height > 1;
        let mut seen = std::mem::take(&mut self.scratch);
        seen.clear();
        for i in cover.left()..cover.right() {
            for j in cover.bottom()..cover.top() {
                let index = (j * count + i) as usize;
                for &wh in &self.data[index].watchers {
                    if need_dedup && !seen.insert(wh) {
                        continue;
                    }
                    let w = &self.slab[wh as usize];
                    if option & ENABLE_SELF_EVENT == 0 && w.handle == mhandle {
                        continue;
                    }
                    let view = Self::calc_make_rect(&map, w.x, w.y, w.w, w.h);
                    if w.layer <= mlayer && Self::marker_seen(&view, mx, my, area) {
                        self.event_queue
                            .push(AoiEvent::new(eventid, w.handle, mhandle));
                    }
                }
            }
        }
        self.scratch = seen;
    }

    #[cfg_attr(feature = "prof_noinline", inline(never))]
    fn update_marker(
        &mut self,
        slot: Slot,
        old_x: i32,
        old_y: i32,
        old_w: i32,
        old_h: i32,
        old_layer: i32,
    ) {
        let map = self.rect;
        let count = self.count;
        let tile_size = self.tile_size;
        let option = self.option;

        let obj = self.slab[slot as usize];
        let old_cx = Self::calc_tile_x(&map, tile_size, count, old_x);
        let old_cy = Self::calc_tile_y(&map, tile_size, count, old_y);
        let new_cx = Self::calc_tile_x(&map, tile_size, count, obj.x);
        let new_cy = Self::calc_tile_y(&map, tile_size, count, obj.y);
        let new_index = (new_cy * count + new_cx) as usize;

        // A1 fix: diff the *tile coverage* of the marker. A range marker
        // (marker-only with positive extent) occupies every tile its box
        // overlaps; the old single-tile move corrupted membership when such a
        // marker moved. Point markers reduce to a 1x1 coverage rect, so this
        // path also reproduces the original point-marker behavior.
        let old_cover = if (obj.mode & WATCHER == 0) && old_w > 0 && old_h > 0 {
            Self::calc_make_tile_rect(&map, tile_size, count, old_x, old_y, old_w, old_h)
        } else {
            Rect::new(old_cx, old_cy, 1, 1)
        };
        let new_cover = if (obj.mode & WATCHER == 0) && obj.w > 0 && obj.h > 0 {
            Self::calc_make_tile_rect(&map, tile_size, count, obj.x, obj.y, obj.w, obj.h)
        } else {
            Rect::new(new_cx, new_cy, 1, 1)
        };

        // tiles no longer covered: drop membership
        for i in old_cover.left()..old_cover.right() {
            for j in old_cover.bottom()..old_cover.top() {
                if new_cover.contains_point(i, j) {
                    continue;
                }
                let index = (j * count + i) as usize;
                Self::update_tile_version(&obj, &mut self.data[index]);
                // Membership may already be absent when re-linking a
                // soft-erased marker (`erase(_, false)` unlinks but keeps the
                // object); tolerate it like the C++ reference's `[[maybe_unused]]`.
                self.data[index].markers.remove(&slot);
            }
        }
        // tiles newly covered: add membership
        for i in new_cover.left()..new_cover.right() {
            for j in new_cover.bottom()..new_cover.top() {
                if old_cover.contains_point(i, j) {
                    continue;
                }
                let index = (j * count + i) as usize;
                Self::update_tile_version(&obj, &mut self.data[index]);
                self.data[index].markers.insert(slot);
            }
        }
        // A position-only move (coverage unchanged) still bumps the current
        // tile's version so version-cache consumers notice fixed markers
        // shifting in place.
        Self::update_tile_version(&obj, &mut self.data[new_index]);

        if obj.mode & HIDE != 0 {
            return;
        }

        // Events use area-intersection visibility for range markers (point
        // containment for point markers). Visibility is evaluated against the
        // old state (old pos/extent/layer) vs the new state, so position, extent
        // and layer changes all map to the correct enter/leave/pos (A2). Candidate
        // watchers are gathered uniquely across the union of the old and new
        // coverage tiles, so a watcher spanning several of the marker's tiles is
        // notified at most once.
        let old_area = if Self::is_range_marker(obj.mode, old_w, old_h) {
            Some(Self::calc_make_rect(&map, old_x, old_y, old_w, old_h))
        } else {
            None
        };
        let new_area = if Self::is_range_marker(obj.mode, obj.w, obj.h) {
            Some(Self::calc_make_rect(&map, obj.x, obj.y, obj.w, obj.h))
        } else {
            None
        };

        // Scan the union of old/new coverage. When the marker stayed within the
        // same tile set, a single pass suffices; dedup is only required when more
        // than one tile is visited (a watcher may then sit in several of them).
        let both = [old_cover, new_cover];
        let covers: &[Rect] = if old_cover == new_cover {
            &both[..1]
        } else {
            &both[..]
        };
        let need_dedup = covers.len() > 1 || old_cover.width > 1 || old_cover.height > 1;
        let mut seen = std::mem::take(&mut self.scratch);
        seen.clear();
        for &cover in covers {
            for i in cover.left()..cover.right() {
                for j in cover.bottom()..cover.top() {
                    let index = (j * count + i) as usize;
                    for &wh in &self.data[index].watchers {
                        if need_dedup && !seen.insert(wh) {
                            continue;
                        }
                        let w = &self.slab[wh as usize];
                        if option & ENABLE_SELF_EVENT == 0 && w.handle == obj.handle {
                            continue;
                        }
                        let view = Self::calc_make_rect(&map, w.x, w.y, w.w, w.h);
                        let was =
                            w.layer <= old_layer && Self::marker_seen(&view, old_x, old_y, old_area);
                        let is =
                            w.layer <= obj.layer && Self::marker_seen(&view, obj.x, obj.y, new_area);
                        if was && !is {
                            if option & ENABLE_LEAVE_EVENT != 0 {
                                self.event_queue
                                    .push(AoiEvent::new(EVENT_LEAVE, w.handle, obj.handle));
                            }
                        } else if !was && is {
                            self.event_queue
                                .push(AoiEvent::new(EVENT_ENTER, w.handle, obj.handle));
                        } else if was && is {
                            self.event_queue
                                .push(AoiEvent::new(EVENT_POS, w.handle, obj.handle));
                        }
                    }
                }
            }
        }
        self.scratch = seen;
    }

    /// Scans a tile's markers and emits enter/leave for a watcher whose view
    /// transitioned per `vtm`. Takes disjoint borrows so callers can pass
    /// `&self.slab`, `&mut self.event_queue`, and `&self.data[i]` together.
    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(feature = "prof_noinline", inline(never))]
    fn update_watcher(
        map: &Rect,
        slab: &[AoiObject],
        option: i32,
        events: &mut Vec<AoiEvent>,
        tile: &Tile,
        old_view: Rect,
        new_view: Rect,
        obj: &AoiObject,
        vtm: Vtm,
        old_layer: i32,
        seen: &mut HashSet<Slot>,
    ) {
        for &mh in &tile.markers {
            // dedup within this scan pass: a range marker registered to several
            // of the watcher's tiles must be evaluated only once.
            if !seen.insert(mh) {
                continue;
            }
            let m = &slab[mh as usize];
            if (m.mode & (FIXED | HIDE) != 0)
                || (option & ENABLE_SELF_EVENT == 0 && obj.handle == m.handle)
            {
                continue;
            }
            let area = Self::marker_area_of(map, m);
            match vtm {
                Vtm::ZoomIn => {
                    if option & ENABLE_LEAVE_EVENT != 0
                        && m.layer >= old_layer
                        && m.layer >= obj.layer
                        && Self::marker_seen(&old_view, m.x, m.y, area)
                        && !Self::marker_seen(&new_view, m.x, m.y, area)
                    {
                        events.push(AoiEvent::new(EVENT_LEAVE, obj.handle, m.handle));
                    }
                }
                Vtm::ZoomOut => {
                    if m.layer >= obj.layer
                        && Self::marker_seen(&new_view, m.x, m.y, area)
                        && !Self::marker_seen(&old_view, m.x, m.y, area)
                    {
                        events.push(AoiEvent::new(EVENT_ENTER, obj.handle, m.handle));
                    }
                }
                Vtm::Layer => {
                    if Self::marker_seen(&new_view, m.x, m.y, area) {
                        if old_layer > obj.layer && m.layer < old_layer && m.layer >= obj.layer {
                            events.push(AoiEvent::new(EVENT_ENTER, obj.handle, m.handle));
                        }
                        if old_layer < obj.layer && m.layer >= old_layer && m.layer < obj.layer {
                            events.push(AoiEvent::new(EVENT_LEAVE, obj.handle, m.handle));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
