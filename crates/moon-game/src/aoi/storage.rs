use rustc_hash::FxHashMap;
use std::collections::hash_map::Entry;

use super::{AoiObject, Handle, Slot};

/// Stable slots keep candidate iteration independent of handle hashing.
/// Attachment uses a separate byte per slot, without enlarging AoiObject.
#[derive(Default)]
pub(super) struct ObjectStore {
    pub values: Vec<AoiObject>,
    pub attached: Vec<bool>,
    pub free: Vec<Slot>,
    pub index: FxHashMap<Handle, Slot>,
}

impl ObjectStore {
    #[inline]
    pub fn insert(&mut self, obj: AoiObject) -> Option<Slot> {
        let Entry::Vacant(entry) = self.index.entry(obj.handle) else {
            return None;
        };
        let slot = if let Some(slot) = self.free.pop() {
            self.values[slot as usize] = obj;
            self.attached[slot as usize] = false;
            slot
        } else {
            let slot = Slot::try_from(self.values.len()).expect("too many AOI objects");
            self.values.push(obj);
            self.attached.push(false);
            slot
        };
        entry.insert(slot);
        Some(slot)
    }

    pub fn clear(&mut self) {
        self.values.clear();
        self.attached.clear();
        self.free.clear();
        self.index.clear();
    }
}
