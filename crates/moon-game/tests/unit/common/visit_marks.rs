use super::VisitMarks;

#[test]
fn visit_marks_reuse_grow_reset_and_wrap() {
    let mut marks = VisitMarks::default();
    marks.begin();
    assert!(marks.insert(0));
    assert!(!marks.insert(0));
    assert!(marks.insert(100));
    assert!(marks.insert(50));
    assert!(!marks.insert(100));
    marks.begin();
    assert!(marks.insert(100));
    assert!(!marks.insert(100));

    // Slot 0 still carries epoch 1. Wrapping must not mistake it for a
    // visit in the new epoch 1, nor newly allocated slots for visited ones.
    marks.epoch = u32::MAX;
    assert!(marks.insert(50));
    marks.begin();
    assert_eq!(marks.epoch, 1);
    assert!(marks.insert(0));
    assert!(marks.insert(50));
    assert!(marks.insert(101));
    assert!(!marks.insert(101));

    let capacity = marks.marks.capacity();
    marks.reset();
    assert_eq!(marks.marks.capacity(), capacity);
    marks.begin();
    assert!(marks.insert(0));
    assert!(marks.insert(100));
    assert!(!marks.insert(0));
}
