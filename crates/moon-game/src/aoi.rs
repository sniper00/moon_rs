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
//! `FIXED` objects also register the cells covered by their ray footprint
//! `[x, x+w) x [y, y+h)`. This extra broad-phase coverage does not change their
//! centered AOI shape. `query`/`query_objects`/`visit_candidates` expose tile candidates;
//! `query_rect` performs exact, unique AOI-shape intersection.
//!
//! # Lifecycle and events
//!
//! `detach` preserves an object's geometry; `attach` restores its membership.
//! Compatibility `erase(handle, false)` additionally zeroes the geometry.
//! Geometry updates reattach detached objects. `objects` visits registered
//! objects once, including detached ones; `is_attached` distinguishes them.
//!
//! Event and query order is unspecified. Position updates still notify markers
//! when the watcher state is unchanged. Leave suppression applies to geometric
//! moves; watcher layer changes and explicit hide/remove retain their historical
//! notifications. `clear` clears both objects and pending events.
//!
//! # Porting design (slab storage)
//!
//! The C++ version stores objects in a node-stable `std::unordered_map` and
//! keeps raw `object*` pointers inside each tile. Rust's `HashMap` does not
//! provide stable value addresses, so objects live in a dense slab
//! (`Vec<AoiObject>`) and each tile stores a `Slot` (a `u32` index into the
//! slab) instead of a pointer. A `HashMap<Handle, Slot>` backs the public,
//! handle-based API. The hot emit loops therefore resolve an object with a
//! single `slab[slot]` array access — the direct analogue of the C++ pointer
//! deref — rather than a per-element hash lookup. Hot paths still copy the small
//! `Copy` [`AoiObject`] out once and operate on disjoint struct fields, keeping
//! the whole implementation free of `unsafe`.

// Fx hashing (no random seed) instead of std's SipHash: the maps/sets are keyed
// by small integer handles/slots and are extremely hot (handle lookups and
// per-tile membership churn), where SipHash dominates. FxHashMap/FxHashSet are
// drop-in.
use rustc_hash::FxHashSet as HashSet;

use crate::common::{IdSet, VisitMarks};
use crate::math::{Rect, Vec2};

mod api;
mod grid;
// Role-specific transitions share the grid and object store; public lifecycle
// methods below determine their order (marker, watcher, then self visibility).
mod marker;
mod storage;
mod watcher;
use grid::{Grid, TileRect, WorldRect};
use storage::ObjectStore;

/// Square map geometry. Coordinates and tile indices are validated at construction.
#[derive(Clone, Copy, Debug)]
pub struct AoiConfig {
    pub origin: (i32, i32),
    pub map_size: i32,
    pub tile_size: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AoiError {
    InvalidMapSize,
    InvalidTileSize,
    UnalignedMapSize,
    CoordinateOverflow,
    TooManyTiles,
    AllocationFailed,
}

impl std::fmt::Display for AoiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidMapSize => "map size must be positive",
            Self::InvalidTileSize => "tile size must be positive",
            Self::UnalignedMapSize => "map size must be a multiple of tile size",
            Self::CoordinateOverflow => "map coordinates overflow i32",
            Self::TooManyTiles => "tile count exceeds the supported index range",
            Self::AllocationFailed => "cannot allocate AOI tiles",
        })
    }
}
impl std::error::Error for AoiError {}

/// Object identifier. Matches the C++ `object_handle_type` (`int64_t`).
pub type Handle = i64;

// ── mode flags (object roles) ──────────────────────────────────────────────
/// Observer: has a view rectangle and receives enter/leave/pos events.
pub const WATCHER: i32 = 1;
/// Observable target that watchers can see.
pub const MARKER: i32 = 1 << 1;
/// Static object (obstacle/NPC). Drives tile versions and ray casting; skipped
/// by watcher-driven enter/leave generation.
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
    /// Non-watchers with positive extents use an area as their AOI shape.
    /// This does not require MARKER: explicit events also accept other roles.
    #[inline]
    fn is_range_marker(&self) -> bool {
        self.mode & WATCHER == 0 && self.w > 0 && self.h > 0
    }

    /// Footprint containment of a point (C++ `test_object::contains`), using the
    /// object's own `[x, x+w) x [y, y+h)` box. Used by ray casting.
    #[inline]
    fn contains(&self, px: f32, py: f32) -> bool {
        px >= self.x as f32
            && px < (i64::from(self.x) + i64::from(self.w)) as f32
            && py >= self.y as f32
            && py < (i64::from(self.y) + i64::from(self.h)) as f32
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

/// Dense slab index into the object store. Stored inside tiles (instead of a
/// [`Handle`]) so the hot emit loops resolve an object with a single array index
/// — the Rust equivalent of the C++ version's `object*` stored per tile.
type Slot = u32;

#[derive(Clone, Debug)]
struct Tile {
    version: i64,
    markers: IdSet,
    watchers: HashSet<Slot>,
}

impl Default for Tile {
    fn default() -> Self {
        // C++ tiles start at version 1.
        Self {
            version: 1,
            markers: IdSet::default(),
            watchers: HashSet::default(),
        }
    }
}

/// Grid-based Area of Interest system. See the module docs for an overview.
pub struct Aoi {
    option: i32,
    grid: Grid,
    data: Vec<Tile>,
    objects: ObjectStore,
    event_queue: Vec<AoiEvent>,
    /// Per-slot generation marks shared by event emission and exact queries.
    /// A scan starts a new generation rather than clearing a hash set. Point
    /// paths which do not need dedup also avoid allocating the mark array.
    scratch: VisitMarks,
}

impl Aoi {
    /// Creates a map at origin `(posx, posy)` of `map_size x map_size`, split
    /// into square tiles of `tile_size`.
    ///
    /// # Panics
    /// If the configuration is invalid or allocation fails; see [`Self::try_new`].
    pub fn new(posx: i32, posy: i32, map_size: i32, tile_size: i32) -> Self {
        Self::try_new(AoiConfig {
            origin: (posx, posy),
            map_size,
            tile_size,
        })
        .expect("invalid AOI configuration")
    }

    /// Validates the grid and reports invalid dimensions or allocation failure.
    pub fn try_new(config: AoiConfig) -> Result<Self, AoiError> {
        let grid = Grid::new(config)?;
        let len = grid.len();
        let mut data = Vec::new();
        data.try_reserve_exact(len)
            .map_err(|_| AoiError::AllocationFailed)?;
        data.resize_with(len, Tile::default);
        Ok(Self {
            option: 0,
            grid,
            data,
            objects: ObjectStore::default(),
            event_queue: Vec::new(),
            scratch: VisitMarks::default(),
        })
    }

    /// Tile column index of map coordinate `v`, clamped to `[0, count)`.
    #[inline]
    pub fn get_tile_x(&self, v: i32) -> i32 {
        self.grid.tile_x(v)
    }

    /// Tile row index of map coordinate `v`, clamped to `[0, count)`.
    #[inline]
    pub fn get_tile_y(&self, v: i32) -> i32 {
        self.grid.tile_y(v)
    }

    /// View rectangle in world coordinates, centered on `(x, y)` with size
    /// `w x h`, clamped to the map.
    pub fn make_rect(&self, x: i32, y: i32, w: i32, h: i32) -> Rect {
        self.grid.clipped_view(x, y, w, h).0
    }

    /// View rectangle expressed in tile indices.
    pub fn make_tile_rect(&self, x: i32, y: i32, w: i32, h: i32) -> Rect {
        self.grid.view_tiles(x, y, w, h).0
    }

    // ── insert ──────────────────────────────────────────────────────────────

    /// Inserts an object. Returns `false` for negative dimensions, coordinates
    /// outside the map, or a duplicate handle. See also [`Self::insert_object`].
    #[allow(clippy::too_many_arguments)]
    #[inline]
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
        self.insert_object(AoiObject {
            x,
            y,
            w,
            h,
            layer,
            mode,
            handle,
            user,
        })
    }

    /// Inserts an object using named fields. Dimensions must be non-negative.
    #[inline]
    pub fn insert_object(&mut self, obj: AoiObject) -> bool {
        if obj.w < 0 || obj.h < 0 || !self.grid.bounds.contains_point(obj.x, obj.y) {
            return false;
        }
        let Some(slot) = self.objects.insert(obj) else {
            return false;
        };

        self.attach_slot(slot, obj);
        true
    }

    #[inline]
    fn attach_slot(&mut self, slot: Slot, obj: AoiObject) {
        self.objects.attached[slot as usize] = true;
        let AoiObject {
            x,
            y,
            w,
            h,
            layer,
            mode,
            handle,
            ..
        } = obj;
        let grid = self.grid;

        if mode & MARKER != 0 {
            let is_range = obj.is_range_marker();
            let cover = grid.marker_cover(&obj);
            self.add_marker_tiles(cover, slot);
            if mode & HIDE == 0 {
                let area = if is_range {
                    Some(grid.view(x, y, w, h))
                } else {
                    None
                };
                self.emit_to_watchers(cover, x, y, area, layer, handle, EVENT_ENTER);
            }
        }

        if mode & WATCHER != 0 {
            self.attach_watcher(slot, obj);
        }
    }

    // ── events triggered explicitly ──────────────────────────────────────────

    /// Re-fires `eventid` to every watcher that can currently see object
    /// `handle` (used e.g. to notify after attribute changes).
    pub fn fire_event(&mut self, handle: Handle, eventid: i32) {
        if let Some(&slot) = self.objects.index.get(&handle) {
            self.fire_event_slot(slot, eventid);
        }
    }

    #[inline]
    fn fire_event_slot(&mut self, slot: Slot, eventid: i32) {
        if !self.objects.attached[slot as usize] {
            return;
        }
        let obj = self.objects.values[slot as usize];
        let grid = self.grid;
        let count = self.grid.count;

        let is_range = obj.is_range_marker();
        let cover = grid.marker_cover(&obj);

        cover.for_each(|i, j| {
            let index = (j * count + i) as usize;
            Self::update_tile_version(&obj, &mut self.data[index]);
        });

        if obj.mode & HIDE != 0 {
            return;
        }
        let area = if is_range {
            Some(grid.view(obj.x, obj.y, obj.w, obj.h))
        } else {
            None
        };
        self.emit_to_watchers(cover, obj.x, obj.y, area, obj.layer, obj.handle, eventid);
    }

    /// Fires `eventid` to every watcher whose view contains the point `(x, y)`.
    /// The emitted events carry `marker = 0`.
    pub fn fire_event_pos(&mut self, x: i32, y: i32, eventid: i32) {
        let grid = self.grid;
        let count = self.grid.count;

        let tile_x = grid.tile_x(x);
        let tile_y = grid.tile_y(y);
        let index = (tile_y * count + tile_x) as usize;

        let node = &self.data[index];
        for &wh in &node.watchers {
            let w = &self.objects.values[wh as usize];
            let rc = grid.view(w.x, w.y, w.w, w.h);
            if !rc.contains_point(x, y) {
                continue;
            }
            self.event_queue.push(AoiEvent::new(eventid, w.handle, 0));
        }
    }

    /// Toggles the hide flag, emitting a leave (show→hide) or enter (hide→show)
    /// burst to the affected watchers.
    pub fn set_hide(&mut self, handle: Handle, v: bool) {
        let slot = match self.objects.index.get(&handle) {
            Some(&s) => s,
            None => return,
        };
        let mode = self.objects.values[slot as usize].mode;
        if v && (mode & HIDE == 0) {
            self.fire_event_slot(slot, EVENT_LEAVE);
            self.objects.values[slot as usize].mode |= HIDE;
        }
        if !v && (mode & HIDE != 0) {
            self.objects.values[slot as usize].mode &= !HIDE;
            self.fire_event_slot(slot, EVENT_ENTER);
        }
    }

    // ── update (position / view size / layer) ─────────────────────────────────

    /// Updates an object's position, view size, and layer, emitting the
    /// resulting visibility events. Returns `false` for unknown handles or
    /// negative dimensions. Coordinates are clamped to the map.
    pub fn update(&mut self, handle: Handle, x: i32, y: i32, w: i32, h: i32, layer: i32) -> bool {
        let Some(&slot) = self.objects.index.get(&handle) else {
            return false;
        };
        self.update_slot(slot, x, y, w, h, layer)
    }

    #[inline]
    fn update_slot(&mut self, slot: Slot, x: i32, y: i32, w: i32, h: i32, layer: i32) -> bool {
        if w < 0 || h < 0 {
            return false;
        }
        let grid = self.grid;

        let x = x.clamp(grid.bounds.x, grid.bounds.right() - 1);
        let y = y.clamp(grid.bounds.y, grid.bounds.top() - 1);

        let prev = self.objects.values[slot as usize];

        {
            let o = &mut self.objects.values[slot as usize];
            o.x = x;
            o.y = y;
            o.h = h;
            o.w = w;
            if layer >= 0 {
                o.layer = layer;
            }
        }
        let obj = self.objects.values[slot as usize];
        if !self.objects.attached[slot as usize] {
            self.attach_slot(slot, obj);
            return true;
        }

        if obj.mode & MARKER != 0 {
            self.update_marker(slot, prev);
        }

        if obj.mode & WATCHER == 0 {
            return true;
        }

        self.update_watcher_state(slot, prev);
        self.emit_self_transition(prev, obj);
        true
    }

    // ── query / iteration ─────────────────────────────────────────────────────

    /// Visits all markers within the tile rectangle covering `(x, y, w, h)`.
    /// `handler` receives each marker handle and whether its tile is on the
    /// query's edge. Objects spanning tiles may repeat; this is a broad-phase
    /// query. Use [`Self::query_objects`] to read candidates without a handle
    /// lookup, or [`Self::query_rect`] for exact, unique results.
    pub fn query<F: FnMut(Handle, bool)>(&self, x: i32, y: i32, w: i32, h: i32, mut handler: F) {
        self.query_objects(x, y, w, h, |obj, is_edge| handler(obj.handle, is_edge));
    }

    /// Visits borrowed marker objects in the tiles covering the centered query
    /// rectangle. Like [`Self::query`], this returns broad-phase candidates:
    /// objects spanning tiles may repeat, and no exact shape, layer, hidden or
    /// fixed filtering is applied. The boolean denotes a boundary tile, not
    /// whether the object intersects the query rectangle.
    ///
    /// References point directly into object storage, avoiding a subsequent
    /// [`Self::find`] lookup. The callback cannot mutate this AOI while it is
    /// borrowed. Use [`Self::query_rect`] for exact, unique results.
    pub fn query_objects<F: FnMut(&AoiObject, bool)>(
        &self,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
        mut handler: F,
    ) {
        let tile_rc = self.grid.view_tiles(x, y, w, h);
        let start_x = tile_rc.x;
        let start_y = tile_rc.y;
        let end_x = tile_rc.right();
        let end_y = tile_rc.top();

        for j in start_y..end_y {
            let is_y_edge = j == start_y || j == end_y - 1;
            for i in start_x..end_x {
                let is_edge = is_y_edge || i == start_x || i == end_x - 1;
                let node = &self.data[(j * self.grid.count + i) as usize];
                for &mh in &node.markers {
                    handler(&self.objects.values[mh as usize], is_edge);
                }
            }
        }
    }

    /// Visits tile memberships whose object `mode` matches `filter`, in tile
    /// order. Objects may repeat. `handler` receives `(handle, x, y, tile_x,
    /// tile_y)`; use [`Self::objects`] to visit registered objects once.
    pub fn for_each_all<F: FnMut(Handle, i32, i32, i32, i32)>(&self, mut handler: F, filter: i32) {
        let count = self.grid.count;
        for y in 0..count {
            for x in 0..count {
                let node = &self.data[(y * count + x) as usize];
                for &mh in &node.markers {
                    let m = &self.objects.values[mh as usize];
                    if m.mode & filter != 0 {
                        handler(m.handle, m.x, m.y, x, y);
                    }
                }
                for &wh in &node.watchers {
                    let w = &self.objects.values[wh as usize];
                    if w.mode & filter != 0 {
                        handler(w.handle, w.x, w.y, x, y);
                    }
                }
            }
        }
    }

    /// Casts a ray from `spos` to `epos` sampling every `step` units, returning
    /// the handle of the first `FIXED` marker whose footprint contains a sample.
    /// When several markers contain the first hit sample, their tie order is unspecified.
    /// Returns `None` for a non-positive/non-finite step or non-finite distance.
    pub fn raycast(&self, spos: Vec2, epos: Vec2, step: f32) -> Option<Handle> {
        if !step.is_finite() || step <= 0.0 {
            return None;
        }
        let grid = self.grid;
        let count = self.grid.count;

        let d = spos.distance(&epos);
        if !d.is_finite() {
            return None;
        }
        let dir = (epos - spos).normalized();
        let mut f = 0.0_f32;
        // Cache the tile between boundary crossings, but still test every
        // sample. Integer bounds preserve float-to-int truncation and the
        // clamping of samples outside the map (including negative origins).
        let (mut left, mut right, mut bottom, mut top) = (1, 0, 1, 0);
        let mut node = &self.data[0];
        while f < d {
            let pos = spos + dir * f;
            let x = pos.x as i32;
            let y = pos.y as i32;
            if x < left || x > right || y < bottom || y > top {
                let tile_x = grid.tile_x(x);
                let tile_y = grid.tile_y(y);
                let bounds = grid.tile_world(tile_x, tile_y);
                left = if tile_x == 0 { i32::MIN } else { bounds.x };
                bottom = if tile_y == 0 { i32::MIN } else { bounds.y };
                right = if tile_x == count - 1 {
                    i32::MAX
                } else {
                    bounds.right() - 1
                };
                top = if tile_y == count - 1 {
                    i32::MAX
                } else {
                    bounds.top() - 1
                };
                node = &self.data[(tile_y * count + tile_x) as usize];
            }
            for &mh in &node.markers {
                let m = &self.objects.values[mh as usize];
                if m.mode & FIXED == 0 {
                    continue;
                }
                if m.contains(pos.x, pos.y) {
                    return Some(m.handle);
                }
            }
            let next = f + step;
            // A small step can stop advancing once f loses float precision.
            if next <= f {
                break;
            }
            f = next;
        }
        None
    }

    // ── removal ───────────────────────────────────────────────────────────────

    /// Removes an object. When `remove` is `false`, the object is kept in the
    /// registry with its geometry zeroed (soft delete) but unlinked from all
    /// tiles.
    pub fn erase(&mut self, handle: Handle, remove: bool) {
        if remove {
            self.remove(handle);
            return;
        }
        let slot = match self.objects.index.get(&handle) {
            Some(&s) => s,
            None => return,
        };
        self.detach_slot(slot);
        let o = &mut self.objects.values[slot as usize];
        o.x = 0;
        o.y = 0;
        o.w = 0;
        o.h = 0;
        o.layer = 0;
    }

    #[inline]
    fn detach_slot(&mut self, slot: Slot) {
        if !self.objects.attached[slot as usize] {
            return;
        }
        self.objects.attached[slot as usize] = false;
        let obj = self.objects.values[slot as usize];
        let grid = self.grid;
        let count = self.grid.count;

        if obj.mode & MARKER != 0 {
            let is_range = obj.is_range_marker();
            let cover = grid.marker_cover(&obj);
            self.remove_marker_tiles(cover, slot);
            if obj.mode & HIDE == 0 {
                let area = if is_range {
                    Some(grid.view(obj.x, obj.y, obj.w, obj.h))
                } else {
                    None
                };
                self.emit_to_watchers(
                    cover,
                    obj.x,
                    obj.y,
                    area,
                    obj.layer,
                    obj.handle,
                    EVENT_LEAVE,
                );
            }
        }

        if obj.mode & WATCHER != 0 {
            let tr = grid.view_tiles(obj.x, obj.y, obj.w, obj.h);
            tr.for_each(|i, j| {
                let index = (j * count + i) as usize;
                self.data[index].watchers.remove(&slot);
            });
        }
    }

    /// Removes all objects and pending events, retaining allocated capacity.
    /// Occupied tile versions advance so external caches can invalidate them.
    pub fn clear(&mut self) {
        for n in &mut self.data {
            if !n.markers.is_empty() {
                n.version += 1;
            }
            n.markers.clear();
            n.watchers.clear();
        }
        self.objects.clear();
        self.event_queue.clear();
        self.scratch.reset();
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    #[inline]
    pub fn set_option(&mut self, option: i32) {
        self.option = option;
    }

    #[inline]
    pub fn has_object(&self, handle: Handle) -> bool {
        self.objects.index.contains_key(&handle)
    }

    #[inline]
    pub fn find(&self, handle: Handle) -> Option<&AoiObject> {
        self.objects
            .index
            .get(&handle)
            .map(|&s| &self.objects.values[s as usize])
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.objects.index.len()
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
        (y * self.grid.count + x) as usize
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
            .map(|&s| self.objects.values[s as usize].handle)
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

    /// Whether a watcher `view` can see a marker: half-open area intersection for
    /// range markers, half-open point containment for point markers.
    #[cfg_attr(feature = "prof_noinline", inline(never))]
    #[cfg_attr(not(feature = "prof_noinline"), inline)]
    fn marker_seen(view: &WorldRect, mx: i32, my: i32, area: Option<WorldRect>) -> bool {
        match area {
            Some(a) => view.intersects_halfopen(&a),
            None => view.contains_point(mx, my),
        }
    }

    #[inline]
    fn transition_event(was: bool, is: bool, leave: bool, position: bool) -> Option<i32> {
        match (was, is) {
            (false, true) => Some(EVENT_ENTER),
            (true, false) if leave => Some(EVENT_LEAVE),
            (true, true) if position => Some(EVENT_POS),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;
