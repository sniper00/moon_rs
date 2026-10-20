use super::*;
use futures_util::stream;

#[tokio::test]
async fn batches_follow_continuations_and_return_final_partial_batch() {
    let mut received = Vec::new();
    stream_batches(
        stream::iter((1..=5).map(Ok::<_, &str>)),
        2,
        1,
        10,
        |owner, session, rows, next| {
            received.push((owner, session, rows));
            if let Some(tx) = next {
                assert!(tx.send(CursorSignal::Next(owner + 1, session + 1)).is_ok());
            }
            true
        },
    )
    .await
    .unwrap();
    assert_eq!(
        received,
        vec![(1, 10, vec![1, 2]), (2, 11, vec![3, 4]), (3, 12, vec![5])]
    );
}

#[tokio::test]
async fn exact_multiple_ends_with_an_empty_batch() {
    let mut sizes = Vec::new();
    stream_batches(
        stream::iter([Ok::<_, &str>(1), Ok(2)]),
        2,
        1,
        1,
        |owner, session, rows, next| {
            sizes.push(rows.len());
            if let Some(tx) = next {
                assert!(tx.send(CursorSignal::Next(owner, session)).is_ok());
            }
            true
        },
    )
    .await
    .unwrap();
    assert_eq!(sizes, [2, 0]);
}

#[tokio::test]
async fn error_discards_partial_batch_and_targets_current_waiter() {
    let mut batches = 0;
    let err = stream_batches(
        stream::iter([Ok(1), Ok(2), Ok(3), Err("broken")]),
        2,
        1,
        1,
        |_, _, rows, next| {
            batches += 1;
            assert_eq!(rows, [1, 2]);
            assert!(next.unwrap().send(CursorSignal::Next(7, 9)).is_ok());
            true
        },
    )
    .await
    .unwrap_err();
    assert_eq!(batches, 1);
    assert_eq!(err, (7, 9, "broken"));
}

#[tokio::test]
async fn close_drop_and_failed_delivery_stop_without_reading_ahead() {
    for mode in 0..4 {
        let mut reads = 0;
        let stream = stream::iter([Ok::<_, &str>(1), Ok(2)]).map_ok(|row| {
            reads += 1;
            row
        });
        stream_batches(stream, 1, 1, 1, |_, _, rows, next| {
            assert_eq!(rows, [1]);
            match mode {
                0 => CursorHandle(next).close(),
                1 => drop(CursorHandle(next)),
                2 => drop(next),
                _ => {
                    drop(next);
                    return false;
                }
            }
            true
        })
        .await
        .unwrap();
        assert_eq!(reads, 1);
    }
}
