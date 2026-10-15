//! Watcher updates: choose affected tiles, maintain membership, then emit
//! visibility transitions. A scan borrows one dedup generation for all phases.
use super::*;

/// How a watcher's view changed, controlling which events a tile scan emits.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ScanPhase {
    Leaving,
    Entering,
    Layer,
}

#[derive(Clone, Copy)]
struct WatcherChange {
    before: WorldRect,
    after: WorldRect,
    old_layer: i32,
    layer: i32,
    handle: Handle,
}

impl WatcherChange {
    /// Point phases partition old-only, new-only and common visibility.
    #[inline]
    fn point_event(&self, m: &AoiObject, phase: ScanPhase, leave: bool) -> Option<i32> {
        match phase {
            ScanPhase::Leaving
                if leave
                    && m.layer >= self.old_layer
                    && self.before.contains_point(m.x, m.y)
                    && !self.after.contains_point(m.x, m.y) =>
            {
                Some(EVENT_LEAVE)
            }
            ScanPhase::Entering
                if m.layer >= self.layer
                    && self.after.contains_point(m.x, m.y)
                    && !self.before.contains_point(m.x, m.y) =>
            {
                Some(EVENT_ENTER)
            }
            ScanPhase::Layer
                if m.layer >= self.old_layer.min(self.layer)
                    && m.layer < self.old_layer.max(self.layer)
                    && self.before.contains_point(m.x, m.y)
                    && self.after.contains_point(m.x, m.y) =>
            {
                Some(if self.old_layer > self.layer {
                    EVENT_ENTER
                } else {
                    EVENT_LEAVE
                })
            }
            _ => None,
        }
    }

    /// A range can intersect both disjoint views. Compare the real endpoints,
    /// preserving layer-change leaves even when movement leaves are disabled.
    #[inline]
    fn range_event(&self, area: WorldRect, layer: i32, leave: bool) -> Option<i32> {
        let old_geometry = self.before.intersects_halfopen(&area);
        let new_geometry = self.after.intersects_halfopen(&area);
        let was = layer >= self.old_layer && old_geometry;
        let is = layer >= self.layer && new_geometry;
        Aoi::transition_event(was, is, leave || new_geometry, false)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct WatcherView {
    world: WorldRect,
    tiles: TileRect,
}

impl WatcherView {
    fn new(grid: Grid, obj: AoiObject) -> Self {
        Self {
            world: grid.view(obj.x, obj.y, obj.w, obj.h),
            tiles: grid.view_tiles(obj.x, obj.y, obj.w, obj.h),
        }
    }
}

impl Aoi {
    #[inline]
    pub(super) fn attach_watcher(&mut self, slot: Slot, obj: AoiObject) {
        let view = WatcherView::new(self.grid, obj);
        let change = WatcherChange {
            before: WorldRect::default(),
            after: view.world,
            old_layer: obj.layer,
            layer: obj.layer,
            handle: obj.handle,
        };
        let mut scan = WatcherScan::new(
            self.grid,
            &self.objects.values,
            self.option,
            &mut self.event_queue,
            &mut self.scratch,
            change,
        );
        view.tiles.for_each(|x, y| {
            let tile = &mut self.data[(y * self.grid.count + x) as usize];
            tile.watchers.insert(slot);
            scan.tile(tile, ScanPhase::Entering);
        });
    }

    pub(super) fn update_watcher_state(&mut self, slot: Slot, prev: AoiObject) {
        let obj = self.objects.values[slot as usize];
        // Marker events/versions are processed independently, even on no-op moves.
        if (prev.x, prev.y, prev.w, prev.h, prev.layer) == (obj.x, obj.y, obj.w, obj.h, obj.layer) {
            return;
        }
        let before = WatcherView::new(self.grid, prev);
        let after = WatcherView::new(self.grid, obj);
        let change = WatcherChange {
            before: before.world,
            after: after.world,
            old_layer: prev.layer,
            layer: obj.layer,
            handle: obj.handle,
        };
        // All phases borrow the same marks directly; no take/restore or exit cleanup.
        let mut scan = WatcherScan::new(
            self.grid,
            &self.objects.values,
            self.option & !ENABLE_SELF_EVENT,
            &mut self.event_queue,
            &mut self.scratch,
            change,
        );
        let layer_tiles = if before == after {
            after.tiles
        } else if before.world.contains_rect(&after.world) {
            scan.zoom(
                &mut self.data,
                slot,
                before.tiles,
                after,
                ScanPhase::Leaving,
            );
            after.tiles
        } else if after.world.contains_rect(&before.world) {
            scan.zoom(
                &mut self.data,
                slot,
                after.tiles,
                before,
                ScanPhase::Entering,
            );
            before.tiles
        } else {
            scan.movement(&mut self.data, slot, before, after);
            before.tiles
        };
        scan.layers(&self.data, layer_tiles);
    }

    #[inline]
    pub(super) fn emit_self_transition(&mut self, prev: AoiObject, obj: AoiObject) {
        if self.option & ENABLE_SELF_EVENT == 0 || obj.mode & (MARKER | HIDE) != MARKER {
            return;
        }
        // Both roles changed together. Normal scans exclude self; evaluate it once.
        let was = self
            .grid
            .view(prev.x, prev.y, prev.w, prev.h)
            .contains_point(prev.x, prev.y);
        let is = self
            .grid
            .view(obj.x, obj.y, obj.w, obj.h)
            .contains_point(obj.x, obj.y);
        if let Some(eventid) =
            Self::transition_event(was, is, self.option & ENABLE_LEAVE_EVENT != 0, true)
        {
            self.event_queue
                .push(AoiEvent::new(eventid, obj.handle, obj.handle));
        }
    }
}

/// Shared immutable inputs and output buffers for one watcher transition.
/// Tile membership is borrowed separately so mutation stays at the scan boundary.
struct WatcherScan<'a> {
    grid: Grid,
    objects: &'a [AoiObject],
    option: i32,
    events: &'a mut Vec<AoiEvent>,
    seen: &'a mut VisitMarks,
    change: WatcherChange,
}

impl<'a> WatcherScan<'a> {
    fn new(
        grid: Grid,
        objects: &'a [AoiObject],
        option: i32,
        events: &'a mut Vec<AoiEvent>,
        seen: &'a mut VisitMarks,
        change: WatcherChange,
    ) -> Self {
        seen.begin();
        Self {
            grid,
            objects,
            option,
            events,
            seen,
            change,
        }
    }

    /// Zoom phases retain the full rectangle scan and skip wholly shared tiles.
    fn zoom(
        &mut self,
        tiles: &mut [Tile],
        slot: Slot,
        cover: TileRect,
        other: WatcherView,
        phase: ScanPhase,
    ) {
        cover.for_each_column_major(|x, y| {
            if other.world.contains_rect(&self.grid.tile_world(x, y)) {
                return;
            }
            let tile = &mut tiles[(y * self.grid.count + x) as usize];
            self.transition_tile(tile, slot, other.tiles.contains_point(x, y), phase);
        });
    }

    /// General moves scan the old/new boundary strips in the original order.
    fn movement(
        &mut self,
        tiles: &mut [Tile],
        slot: Slot,
        before: WatcherView,
        after: WatcherView,
    ) {
        let shared = after.world.join(&before.world);
        let interior = self.grid.interior_tiles(shared, self.grid.tiles(shared));
        self.strips(
            tiles,
            slot,
            before.tiles,
            after.tiles,
            interior,
            ScanPhase::Leaving,
        );
        self.strips(
            tiles,
            slot,
            after.tiles,
            before.tiles,
            interior,
            ScanPhase::Entering,
        );
    }

    fn strips(
        &mut self,
        tiles: &mut [Tile],
        slot: Slot,
        cover: TileRect,
        other: TileRect,
        interior: TileRect,
        phase: ScanPhase,
    ) {
        for strip in cover.outside(interior) {
            strip.for_each_column_major(|x, y| {
                let tile = &mut tiles[(y * self.grid.count + x) as usize];
                self.transition_tile(tile, slot, other.contains_point(x, y), phase);
            });
        }
    }

    #[inline]
    fn transition_tile(&mut self, tile: &mut Tile, slot: Slot, retained: bool, phase: ScanPhase) {
        if !retained {
            match phase {
                ScanPhase::Leaving => {
                    tile.watchers.remove(&slot);
                }
                ScanPhase::Entering => {
                    tile.watchers.insert(slot);
                }
                ScanPhase::Layer => unreachable!("layer changes do not change membership"),
            }
        }
        self.tile(tile, phase);
    }

    fn layers(&mut self, tiles: &[Tile], cover: TileRect) {
        if self.change.old_layer == self.change.layer {
            return;
        }
        cover.for_each(|x, y| {
            self.tile(&tiles[(y * self.grid.count + x) as usize], ScanPhase::Layer);
        });
    }

    #[cfg_attr(feature = "prof_noinline", inline(never))]
    #[cfg_attr(not(feature = "prof_noinline"), inline)]
    fn tile(&mut self, tile: &Tile, phase: ScanPhase) {
        let grid = &self.grid;
        let objects = self.objects;
        let option = self.option;
        let change = &self.change;
        let events = &mut *self.events;
        let seen = &mut *self.seen;
        let leave = option & ENABLE_LEAVE_EVENT != 0;
        if phase == ScanPhase::Leaving && !leave && change.old_layer == change.layer {
            return;
        }
        for &slot in &tile.markers {
            let m = &objects[slot as usize];
            if m.mode & (FIXED | HIDE) != 0
                || (option & ENABLE_SELF_EVENT == 0 && change.handle == m.handle)
            {
                continue;
            }
            if m.is_range_marker() {
                if m.layer < change.old_layer.min(change.layer) || !seen.insert(slot) {
                    continue;
                }
                if let Some(eventid) =
                    change.range_event(grid.view(m.x, m.y, m.w, m.h), m.layer, leave)
                {
                    events.push(AoiEvent::new(eventid, change.handle, m.handle));
                }
                continue;
            }
            let eventid = change.point_event(m, phase, leave);
            if let Some(eventid) = eventid {
                events.push(AoiEvent::new(eventid, change.handle, m.handle));
            }
        }
    }
}
