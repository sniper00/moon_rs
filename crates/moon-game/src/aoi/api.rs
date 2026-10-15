use super::{Aoi, AoiObject, Handle, Rect, WorldRect};

impl Aoi {
    /// Updates position using the existing dimensions/layer. Reattaches a
    /// detached object, matching the compatibility `update` method.
    pub fn set_position(&mut self, handle: Handle, x: i32, y: i32) -> bool {
        let Some(&slot) = self.objects.index.get(&handle) else {
            return false;
        };
        let obj = self.objects.values[slot as usize];
        self.update_slot(slot, x, y, obj.w, obj.h, obj.layer)
    }

    /// Updates dimensions without changing position or layer.
    pub fn set_view(&mut self, handle: Handle, w: i32, h: i32) -> bool {
        let Some(&slot) = self.objects.index.get(&handle) else {
            return false;
        };
        let obj = self.objects.values[slot as usize];
        self.update_slot(slot, obj.x, obj.y, w, h, obj.layer)
    }

    /// Sets a non-negative layer. Unlike `update`, there is no sentinel value.
    pub fn set_layer(&mut self, handle: Handle, layer: i32) -> bool {
        if layer < 0 {
            return false;
        }
        let Some(&slot) = self.objects.index.get(&handle) else {
            return false;
        };
        let obj = self.objects.values[slot as usize];
        self.update_slot(slot, obj.x, obj.y, obj.w, obj.h, layer)
    }

    /// Unlinks an object but preserves its geometry for a later `attach`.
    /// Returns false if the object is missing or already detached.
    pub fn detach(&mut self, handle: Handle) -> bool {
        let Some(&slot) = self.objects.index.get(&handle) else {
            return false;
        };
        if !self.objects.attached[slot as usize] {
            return false;
        }
        self.detach_slot(slot);
        true
    }

    /// Restores all grid memberships and emits the corresponding enter events.
    pub fn attach(&mut self, handle: Handle) -> bool {
        let Some(&slot) = self.objects.index.get(&handle) else {
            return false;
        };
        if self.objects.attached[slot as usize] {
            return false;
        }
        // Compatibility soft deletion zeroes coordinates, which may be outside
        // a map with a nonzero origin. Clamp before restoring membership.
        let obj = &mut self.objects.values[slot as usize];
        obj.x = obj
            .x
            .clamp(self.grid.bounds.x, self.grid.bounds.right() - 1);
        obj.y = obj.y.clamp(self.grid.bounds.y, self.grid.bounds.top() - 1);
        let obj = *obj;
        self.attach_slot(slot, obj);
        true
    }

    pub fn is_attached(&self, handle: Handle) -> bool {
        self.objects
            .index
            .get(&handle)
            .is_some_and(|&slot| self.objects.attached[slot as usize])
    }

    /// Removes an attached or detached object and releases its slot.
    #[inline]
    pub fn remove(&mut self, handle: Handle) -> bool {
        let Some(slot) = self.objects.index.remove(&handle) else {
            return false;
        };
        self.detach_slot(slot);
        self.objects.free.push(slot);
        true
    }

    /// Visits each registered object once, including detached objects. Order is
    /// unspecified; vacant slab slots and duplicate tile memberships are absent.
    pub fn objects(&self) -> impl Iterator<Item = &AoiObject> {
        self.objects
            .index
            .values()
            .map(|&slot| &self.objects.values[slot as usize])
    }

    /// Drains pending events while retaining queue capacity. Dropping the
    /// iterator also discards unread events. The iterator borrows this AOI.
    pub fn drain_events(&mut self) -> impl Iterator<Item = super::AoiEvent> + '_ {
        self.event_queue.drain(..)
    }

    /// Low-level tile candidates for a centered rectangle. Range markers can
    /// appear more than once; the boolean denotes a boundary tile, not an exact
    /// shape test. No layer/hidden/fixed filtering is applied.
    /// Use [`Self::query_objects`] when the callback needs object fields.
    pub fn visit_candidates<F: FnMut(Handle, bool)>(
        &self,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
        handler: F,
    ) {
        self.query(x, y, w, h, handler);
    }

    /// Exact AOI-shape intersection with a half-open world rectangle, clipped
    /// to the map. Each attached marker is returned once. This is a spatial
    /// query: hidden/fixed/layer filtering is left to the caller.
    pub fn query_rect<F: FnMut(&AoiObject)>(&mut self, rect: Rect, mut handler: F) {
        if rect.empty() {
            return;
        }
        let map = self.grid.bounds;
        let left = rect.x.clamp(map.x, map.right());
        let bottom = rect.y.clamp(map.y, map.top());
        let right = rect.x.saturating_add(rect.width).clamp(map.x, map.right());
        let top = rect.y.saturating_add(rect.height).clamp(map.y, map.top());
        let view = WorldRect(Rect::new(left, bottom, right - left, top - bottom));
        if view.empty() {
            return;
        }
        let tiles = self.grid.tiles(view);
        let seen = &mut self.scratch;
        seen.begin();
        tiles.for_each(|x, y| {
            for &slot in &self.data[(y * self.grid.count + x) as usize].markers {
                // Fixed footprints may also span tiles for ray casting.
                if !seen.insert(slot) {
                    continue;
                }
                let obj = &self.objects.values[slot as usize];
                if Self::marker_seen(&view, obj.x, obj.y, self.grid.marker_area(obj)) {
                    handler(obj);
                }
            }
        });
    }
}
