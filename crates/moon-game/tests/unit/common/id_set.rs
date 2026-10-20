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
