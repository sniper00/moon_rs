/// Deduplicates compact `u32` IDs across sequential scans using generation marks.
///
/// Call [`Self::begin`] before each scan, then [`Self::insert`] for each ID.
/// IDs must retain their meaning during a scan. Storage grows lazily to the
/// highest visited ID, so this is intended for dense, bounded ID spaces.
#[derive(Default)]
pub struct VisitMarks {
    marks: Vec<u32>,
    epoch: u32,
}

impl VisitMarks {
    /// Starts a fresh scan, retaining allocated storage.
    #[inline]
    pub fn begin(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.restart_epoch();
        }
    }

    /// Returns true only on the first visit in the current operation.
    #[inline]
    pub fn insert(&mut self, slot: u32) -> bool {
        debug_assert_ne!(self.epoch, 0, "begin a scan before marking slots");
        let slot = slot as usize;
        if slot >= self.marks.len() {
            self.grow(slot + 1);
        }
        let mark = &mut self.marks[slot];
        if *mark == self.epoch {
            return false;
        }
        *mark = self.epoch;
        true
    }

    // Keep allocation and epoch wraparound out of the inlined candidate loops.
    #[cold]
    #[inline(never)]
    fn grow(&mut self, len: usize) {
        self.marks.resize(len, 0);
    }

    #[cold]
    #[inline(never)]
    fn restart_epoch(&mut self) {
        // Zero means unvisited, including newly allocated slots. Clear all
        // old generations before reusing epoch 1 after wraparound.
        self.marks.fill(0);
        self.epoch = 1;
    }

    /// Discards all ID history, retaining capacity. Call begin before reuse.
    pub fn reset(&mut self) {
        self.marks.clear();
        self.epoch = 0;
    }
}

#[cfg(test)]
#[path = "../../tests/unit/common/visit_marks.rs"]
mod tests;
