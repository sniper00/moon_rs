use super::*;
use tokio::io::{AsyncReadExt, duplex};

#[tokio::test]
async fn read_spare_capacity_across_partial_reads() {
    let (mut tx, mut rx) = duplex(2);
    let mut buffer = Buffer::from("prefix");
    let (sent, read) = tokio::join!(
        tx.write_all(b"payload"),
        read_buffer_exact(&mut rx, &mut buffer, 7)
    );
    sent.unwrap();
    read.unwrap();
    assert_eq!(buffer.as_slice(), b"prefixpayload");
}

#[tokio::test]
async fn incomplete_reads_do_not_publish_partial_data() {
    let mut rx = &b"short"[..];
    let mut buffer = Buffer::from("prefix");
    let err = read_buffer_exact(&mut rx, &mut buffer, 10)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(buffer.as_slice(), b"prefix");
    read_buffer_exact(&mut rx, &mut buffer, 0).await.unwrap();
    assert_eq!(buffer.as_slice(), b"prefix");
}

#[tokio::test]
async fn cancelled_read_keeps_only_committed_bytes() {
    let (mut tx, mut rx) = duplex(2);
    tx.write_all(b"ab").await.unwrap();
    let mut buffer = Buffer::from("prefix");
    {
        let read = read_buffer_exact(&mut rx, &mut buffer, 4);
        tokio::pin!(read);
        tokio::select! {
            biased;
            _ = &mut read => panic!("read should wait for the remaining bytes"),
            _ = tokio::task::yield_now() => {}
        }
    }
    assert_eq!(buffer.as_slice(), b"prefix");
}

#[tokio::test]
async fn vectored_write_preserves_empty_parts_and_partial_writes() {
    let (mut tx, mut rx) = duplex(2);
    let write = async {
        let mut parts = [
            IoSlice::new(b""),
            IoSlice::new(b"abc"),
            IoSlice::new(b""),
            IoSlice::new(b"defgh"),
            IoSlice::new(b""),
        ];
        write_all_vectored(&mut tx, &mut parts, "zero write")
            .await
            .unwrap();
        tx.shutdown().await.unwrap();
    };
    let mut output = Vec::new();
    let (_, read) = tokio::join!(write, rx.read_to_end(&mut output));
    read.unwrap();
    assert_eq!(output, b"abcdefgh");
}

#[tokio::test]
async fn zero_write_reports_the_callers_error_context() {
    let mut output = std::io::Cursor::new(&mut [][..]);
    let mut parts = [IoSlice::new(b"payload")];
    let err = write_all_vectored(&mut output, &mut parts, "cluster frame")
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    assert_eq!(err.to_string(), "cluster frame");
}
