use rustc_hash::FxHashMap;

/// A set of `u32` IDs stored contiguously for iteration.
///
/// Small sets use linear lookup/removal; dense sets additionally index positions.
/// Iteration order is unspecified: removal moves the last ID into the hole.
/// The index is retained across shrinking and clearing to avoid rebuild churn.
#[derive(Clone, Debug, Default)]
pub struct IdSet {
    ids: Vec<u32>,
    positions: Option<Box<FxHashMap<u32, u32>>>,
}

impl IdSet {
    // Bound linear removal to 128 ids (512 bytes). Once built, retain the
    // index across shrinking/clear to avoid rebuilding around the threshold.
    const INDEX_THRESHOLD: usize = 128;

    /// Returns whether the ID is present, using the position index when available.
    #[inline]
    pub fn contains(&self, id: &u32) -> bool {
        match &self.positions {
            Some(positions) => positions.contains_key(id),
            None => self.ids.contains(id),
        }
    }

    /// Inserts an ID once. Returns false if it was already present.
    #[inline]
    pub fn insert(&mut self, id: u32) -> bool {
        if self.contains(&id) {
            return false;
        }
        self.insert_unique(id);
        true
    }

    /// Inserts an ID that the caller guarantees is absent.
    ///
    /// Use [`Self::insert`] when uniqueness is not already known. Duplicates
    /// violate the container's invariants; a debug assertion detects them.
    #[inline]
    pub fn insert_unique(&mut self, id: u32) {
        debug_assert!(!self.ids.contains(&id), "duplicate ID");
        if let Some(positions) = &mut self.positions {
            positions.insert(id, self.ids.len() as u32);
        } else if self.ids.len() == Self::INDEX_THRESHOLD {
            self.build_index();
            self.positions
                .as_mut()
                .unwrap()
                .insert(id, self.ids.len() as u32);
        }
        self.ids.push(id);
    }

    #[cold]
    #[inline(never)]
    fn build_index(&mut self) {
        // Unique u32 IDs bound the number of positions to the same address space.
        self.positions = Some(Box::new(
            self.ids
                .iter()
                .enumerate()
                .map(|(i, &s)| (s, i as u32))
                .collect(),
        ));
    }

    #[inline]
    pub fn remove(&mut self, id: &u32) -> bool {
        if let Some(positions) = &mut self.positions {
            let Some(index) = positions.remove(id) else {
                return false;
            };
            let index = index as usize;
            self.ids.swap_remove(index);
            if let Some(moved) = self.ids.get(index) {
                *positions.get_mut(moved).unwrap() = index as u32;
            }
            true
        } else if let Some(index) = self.ids.iter().position(|s| s == id) {
            self.ids.swap_remove(index);
            true
        } else {
            false
        }
    }

    pub fn clear(&mut self) {
        self.ids.clear();
        // Keep allocated storage for the next rebuild.
        if let Some(positions) = &mut self.positions {
            positions.clear();
        }
    }
}

impl std::ops::Deref for IdSet {
    type Target = [u32];

    fn deref(&self) -> &Self::Target {
        &self.ids
    }
}

impl<'a> IntoIterator for &'a IdSet {
    type Item = &'a u32;
    type IntoIter = std::slice::Iter<'a, u32>;

    fn into_iter(self) -> Self::IntoIter {
        self.ids.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::IdSet;

    #[test]
    fn id_set_matches_set_through_churn_and_clear() {
        use std::collections::BTreeSet;

        let mut markers = IdSet::default();
        let mut expected = BTreeSet::new();
        let mut rng = 123_u32;
        for cycle in 0..3 {
            for slot in 0..1024 {
                markers.insert_unique(slot);
                expected.insert(slot);
            }
            for step in 0..10000 {
                rng = rng.wrapping_mul(1664525).wrapping_add(1013904223);
                let slot = (rng >> 16) % 1536;
                if rng & 1 == 0 {
                    if expected.insert(slot) {
                        markers.insert_unique(slot);
                    }
                } else {
                    assert_eq!(markers.remove(&slot), expected.remove(&slot));
                }
                if step % 127 == 0 {
                    let mut actual: Vec<_> = markers.iter().copied().collect();
                    actual.sort_unstable();
                    assert_eq!(actual, expected.iter().copied().collect::<Vec<_>>());
                }
            }
            // Remove in sorted order, independent of the container's order.
            for slot in &expected {
                assert!(markers.remove(slot), "cycle {cycle}, slot {slot}");
            }
            assert!(markers.is_empty());
            markers.insert_unique(42);
            markers.clear();
            expected.clear();
            assert!(markers.is_empty());
        }
    }

    #[test]
    fn id_set_clear_and_clone_at_index_threshold() {
        for count in [0, 1, 128, 129, 1024] {
            let mut markers = IdSet::default();
            for slot in 0..count {
                markers.insert_unique(slot);
            }
            let mut cloned = markers.clone();
            markers.clear();
            assert!(markers.is_empty());
            for slot in (0..count).rev() {
                // Reuse the same slots after clear, including indexed tiles.
                markers.insert_unique(slot);
                assert!(cloned.remove(&slot));
            }
            assert!(cloned.is_empty());
            assert_eq!(markers.len(), count as usize);
            for slot in 0..count {
                assert!(markers.remove(&slot));
                assert!(!markers.remove(&slot));
            }
            assert!(markers.is_empty());
        }
    }
}
