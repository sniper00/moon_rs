use super::*;
use log::Log as _;

#[test]
fn failed_record_formatting_does_not_enqueue_partial_lines() {
    struct FailingDisplay;
    impl fmt::Display for FailingDisplay {
        fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
            Err(fmt::Error)
        }
    }
    let (sender, receiver) = mpsc::channel();
    let logger = Logger {
        sender,
        state: Arc::new(LoggerConext {
            enable_stdout: AtomicBool::new(false),
            state: Arc::new(AtomicU8::new(STATE_RUNNING)),
            level: AtomicU8::new(Logger::level_to_u8(Level::Debug)),
        }),
        pending: Arc::new(AtomicUsize::new(0)),
    };
    logger.log(
        &Record::builder()
            .level(Level::Info)
            .args(format_args!("{FailingDisplay}"))
            .build(),
    );
    assert!(receiver.try_recv().is_err());
    assert_eq!(logger.pending_count(), 0);
    logger.log(
        &Record::builder()
            .level(Level::Info)
            .args(format_args!("valid record"))
            .build(),
    );
    let LogMessage::Line(line) = receiver.try_recv().unwrap() else {
        panic!("expected line")
    };
    assert!(line.as_str().unwrap().contains("valid record"));
}

#[test]
fn failed_file_write_keeps_logger_alive_and_allows_shutdown() {
    let logger = Logger::new();
    logger.state.enable_stdout.store(false, Ordering::Release);
    // A read-only file handle deterministically rejects writes on every platform.
    let file = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).unwrap();
    logger.sender.send(LogMessage::File(file)).unwrap();
    logger.write(logger.make_line(false, Level::Info, 0).unwrap());
    logger.write(logger.make_line(false, Level::Info, 0).unwrap());
    logger.stop();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !logger.stopped() {
        assert!(
            std::time::Instant::now() < deadline,
            "logger did not finish after a write failure"
        );
        thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(logger.pending_count(), 0);
}
