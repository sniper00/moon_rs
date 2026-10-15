//! Marker updates maintain tile coverage first, then notify affected watchers.
use super::*;

impl Aoi {
    /// Registers a marker `slot` into every tile of `cover` (and bumps versions).
    pub(super) fn add_marker_tiles(&mut self, cover: TileRect, slot: Slot) {
        let obj = self.objects.values[slot as usize];
        let count = self.grid.count;
        for j in cover.bottom()..cover.top() {
            for i in cover.left()..cover.right() {
                let index = (j * count + i) as usize;
                Self::update_tile_version(&obj, &mut self.data[index]);
                self.data[index].markers.insert_unique(slot);
            }
        }
    }

    /// Unregisters a marker `slot` from every tile of `cover` (and bumps versions).
    pub(super) fn remove_marker_tiles(&mut self, cover: TileRect, slot: Slot) {
        let obj = self.objects.values[slot as usize];
        let count = self.grid.count;
        for j in cover.bottom()..cover.top() {
            for i in cover.left()..cover.right() {
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
    pub(super) fn emit_to_watchers(
        &mut self,
        cover: TileRect,
        mx: i32,
        my: i32,
        area: Option<WorldRect>,
        mlayer: i32,
        mhandle: Handle,
        eventid: i32,
    ) {
        if cover.width == 1
            && cover.height == 1
            && self.data[(cover.y * self.grid.count + cover.x) as usize]
                .watchers
                .is_empty()
        {
            return;
        }
        let grid = self.grid;
        let count = self.grid.count;
        let option = self.option;
        // Dedup is only needed when the marker spans more than one tile (only
        // then can the same watcher be registered in several scanned tiles). For
        // the overwhelmingly common single-tile marker, skip the per-watcher
        // dedup mark entirely.
        let need_dedup = cover.width > 1 || cover.height > 1;
        let seen = &mut self.scratch;
        if need_dedup {
            seen.begin();
        }
        for j in cover.bottom()..cover.top() {
            for i in cover.left()..cover.right() {
                let index = (j * count + i) as usize;
                for &wh in &self.data[index].watchers {
                    if need_dedup && !seen.insert(wh) {
                        continue;
                    }
                    let w = &self.objects.values[wh as usize];
                    if w.layer > mlayer || (option & ENABLE_SELF_EVENT == 0 && w.handle == mhandle)
                    {
                        continue;
                    }
                    if grid.sees_marker(w, mx, my, area) {
                        self.event_queue
                            .push(AoiEvent::new(eventid, w.handle, mhandle));
                    }
                }
            }
        }
    }

    pub(super) fn update_marker(&mut self, slot: Slot, prev: AoiObject) {
        let obj = self.objects.values[slot as usize];
        let old_cover = self.grid.marker_cover(&prev);
        let new_cover = self.grid.marker_cover(&obj);
        self.move_marker_tiles(slot, obj, old_cover, new_cover);
        self.emit_marker_transition(prev, obj, old_cover, new_cover);
    }

    /// Membership and cache invalidation run even for hidden markers.
    #[inline]
    fn move_marker_tiles(
        &mut self,
        slot: Slot,
        obj: AoiObject,
        old_cover: TileRect,
        new_cover: TileRect,
    ) {
        let count = self.grid.count;
        let coverage_changed = old_cover != new_cover;
        if coverage_changed {
            // tiles no longer covered: drop membership
            old_cover.for_each(|i, j| {
                if new_cover.contains_point(i, j) {
                    return;
                }
                let index = (j * count + i) as usize;
                Self::update_tile_version(&obj, &mut self.data[index]);
                let removed = self.data[index].markers.remove(&slot);
                debug_assert!(removed);
            });
        }
        if coverage_changed || obj.mode & (FIXED | HIDE) == FIXED {
            // Every covered tile may cache geometry, even if membership is unchanged.
            new_cover.for_each(|i, j| {
                let index = (j * count + i) as usize;
                Self::update_tile_version(&obj, &mut self.data[index]);
                if !old_cover.contains_point(i, j) {
                    self.data[index].markers.insert_unique(slot);
                }
            });
        }
    }

    /// Compare the two complete object states for each watcher in the cover union.
    /// Self visibility is handled once after both marker and watcher updates.
    #[cfg_attr(feature = "prof_noinline", inline(never))]
    fn emit_marker_transition(
        &mut self,
        prev: AoiObject,
        obj: AoiObject,
        old_cover: TileRect,
        new_cover: TileRect,
    ) {
        let grid = self.grid;
        let count = grid.count;
        let option = self.option;
        let coverage_changed = old_cover != new_cover;
        if obj.mode & HIDE != 0 {
            return;
        }

        if !coverage_changed
            && new_cover.width == 1
            && new_cover.height == 1
            && self.data[(new_cover.y * count + new_cover.x) as usize]
                .watchers
                .is_empty()
        {
            return;
        }

        let old_area = grid.marker_area(&prev);
        let new_area = grid.marker_area(&obj);
        let AoiObject {
            x: old_x,
            y: old_y,
            layer: old_layer,
            ..
        } = prev;
        // Scan the union of old/new coverage. When the marker stayed within the
        // same tile set, a single pass suffices; dedup is only required when more
        // than one tile is visited (a watcher may then sit in several of them).
        let both = [old_cover, new_cover];
        let covers: &[TileRect] = if old_cover == new_cover {
            &both[..1]
        } else {
            &both[..]
        };
        let need_dedup = covers.len() > 1 || old_cover.width > 1 || old_cover.height > 1;
        let seen = &mut self.scratch;
        seen.begin();
        for (pass, &cover) in covers.iter().enumerate() {
            cover.for_each(|i, j| {
                if pass != 0 && old_cover.contains_point(i, j) {
                    return;
                }
                let index = (j * count + i) as usize;
                for &wh in &self.data[index].watchers {
                    if need_dedup && !seen.insert(wh) {
                        continue;
                    }
                    let w = &self.objects.values[wh as usize];
                    if w.layer > old_layer.max(obj.layer) || w.handle == obj.handle {
                        continue;
                    }
                    let view = grid.view(w.x, w.y, w.w, w.h);
                    let was =
                        w.layer <= old_layer && Self::marker_seen(&view, old_x, old_y, old_area);
                    let is =
                        w.layer <= obj.layer && Self::marker_seen(&view, obj.x, obj.y, new_area);
                    if let Some(eventid) =
                        Self::transition_event(was, is, option & ENABLE_LEAVE_EVENT != 0, true)
                    {
                        self.event_queue
                            .push(AoiEvent::new(eventid, w.handle, obj.handle));
                    }
                }
            });
        }
    }
}
