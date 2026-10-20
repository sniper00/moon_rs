use super::*;

#[test]
#[cfg(target_pointer_width = "64")]
fn message_layout_stays_compact() {
    assert_eq!(std::mem::size_of::<BoxedValue>(), 8);
    assert_eq!(std::mem::size_of::<MessageBody>(), 16);
    assert_eq!(std::mem::size_of::<Message>(), 32);
}

#[repr(align(64))]
struct TrackedPayload {
    drops: Arc<AtomicUsize>,
    text: String,
}

impl Drop for TrackedPayload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn boxed_payload_drop_and_decode_preserve_ownership() {
    let drops = Arc::new(AtomicUsize::new(0));
    let payload = || TrackedPayload {
        drops: drops.clone(),
        text: "aligned payload".to_string(),
    };

    // Dropping a failed delivery must destroy its otherwise undecoded value.
    let failed = CONTEXT.send_value(PTYPE_PG, 0, 1, payload());
    assert!(failed.is_some());
    drop(failed);
    assert_eq!(drops.load(Ordering::Relaxed), 1);

    // The erased owner can cross threads, even for an over-aligned T.
    let boxed = BoxedValue::new(payload());
    assert_eq!(boxed.ptr.as_ptr() as usize % 64, 0);
    std::thread::spawn(move || drop(boxed)).join().unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 2);

    let mut message = Message {
        from: 0,
        to: 0,
        session: 1,
        data: MessageBody::Boxed(PTYPE_PG, BoxedValue::new(payload())),
    };
    let value =
        unsafe { crate::message_decode::take_boxed::<TrackedPayload>(&mut message).unwrap() };
    assert_eq!(value.text, "aligned payload");
    assert!(matches!(message.data, MessageBody::None(0)));
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    assert!(unsafe { crate::message_decode::take_boxed::<TrackedPayload>(&mut message) }.is_err());
    drop(message);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(value);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
}

#[test]
fn boxed_zero_sized_payload_is_dropped_once() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct ZeroSized;
    impl Drop for ZeroSized {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
    assert_eq!(std::mem::size_of::<ZeroSized>(), 0);
    drop(BoxedValue::new(ZeroSized));
    assert_eq!(DROPS.load(Ordering::Relaxed), 1);
    let value = unsafe { BoxedValue::new(ZeroSized).into_inner::<ZeroSized>() };
    assert_eq!(DROPS.load(Ordering::Relaxed), 1);
    drop(value);
    assert_eq!(DROPS.load(Ordering::Relaxed), 2);
}

fn actor_param(id: ActorId, name: &str, unique: bool) -> LuaActorParam {
    LuaActorParam {
        id,
        unique,
        creator: 0,
        session: 0,
        memlimit: 0,
        name: name.to_string(),
        source: String::new(),
        params: String::new(),
    }
}

#[test]
fn service_names_are_exclusive_regardless_of_unique() {
    for (i, (first_unique, second_unique)) in
        [(false, false), (false, true), (true, false), (true, true)]
            .into_iter()
            .enumerate()
    {
        let name = format!("exclusive-name-{i}");
        let first_id = 0x7200_0010 + i as ActorId * 2;
        let second_id = first_id + 1;
        let mut first = LuaActor::new(&actor_param(first_id, &name, first_unique));
        let mut second = LuaActor::new(&actor_param(second_id, &name, second_unique));
        let (tx, _rx) = mpsc::unbounded_channel();
        CONTEXT.add_actor(&mut first, tx).unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        assert!(CONTEXT.add_actor(&mut second, tx).is_err());
        CONTEXT.remove_actor(second_id, &name);
        assert_eq!(CONTEXT.actor_names.get(&name).map(|v| *v), Some(first_id));
        assert_eq!(
            CONTEXT.query(&name).map(|v| *v),
            first_unique.then_some(first_id)
        );
        CONTEXT.remove_actor(first_id, &name);
        let (tx, _rx) = mpsc::unbounded_channel();
        CONTEXT.add_actor(&mut second, tx).unwrap();
        CONTEXT.remove_actor(second_id, &name);
        assert!(!CONTEXT.actor_names.contains_key(&name));
    }
}

#[test]
fn concurrent_service_name_registration_has_one_winner() {
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|i| {
            let barrier = barrier.clone();
            thread::spawn(move || {
                let id = 0x7200_0020 + i;
                let mut actor = LuaActor::new(&actor_param(id, "concurrent-name", i % 2 == 0));
                let (tx, _rx) = mpsc::unbounded_channel();
                barrier.wait();
                CONTEXT.add_actor(&mut actor, tx).ok().map(|_| id)
            })
        })
        .collect();
    let winners: Vec<_> = threads
        .into_iter()
        .filter_map(|t| t.join().unwrap())
        .collect();
    assert_eq!(winners.len(), 1);
    CONTEXT.remove_actor(winners[0], "concurrent-name");
}

#[test]
fn remove_actor_sends_service_exit_only_to_unique_actors() {
    let target_id = 0x7100_0001;
    let unique_watcher_id = 0x7100_0002;
    let normal_watcher_id = 0x7100_0003;
    let pseudo_id = 0x7100_0004;

    let (target_tx, _target_rx) = mpsc::unbounded_channel();
    let (unique_watcher_tx, mut unique_watcher_rx) = mpsc::unbounded_channel();
    let (normal_watcher_tx, mut normal_watcher_rx) = mpsc::unbounded_channel();
    let (pseudo_tx, mut pseudo_rx) = mpsc::unbounded_channel();
    CONTEXT.register_pseudo_actor(pseudo_id, pseudo_tx);

    let mut target = LuaActor::new(&actor_param(target_id, "exit-target", false));
    let mut unique_watcher =
        LuaActor::new(&actor_param(unique_watcher_id, "exit-unique-watcher", true));
    let mut normal_watcher = LuaActor::new(&actor_param(
        normal_watcher_id,
        "exit-normal-watcher",
        false,
    ));

    CONTEXT.add_actor(&mut target, target_tx).unwrap();
    CONTEXT
        .add_actor(&mut unique_watcher, unique_watcher_tx)
        .unwrap();
    CONTEXT
        .add_actor(&mut normal_watcher, normal_watcher_tx)
        .unwrap();

    CONTEXT.remove_actor(target_id, &target.name);

    // `CONTEXT` is a process-wide singleton shared by every test in this
    // crate. Other tests running in parallel (e.g. cluster's
    // `broadcast_system(CLUSTER_ACTOR_ID, ...)`) also fan PTYPE_SYSTEM
    // messages out to *all* unique actors, so foreign messages can land in
    // this watcher's mailbox. Scan for our own service-exit (identified by
    // `from == target_id`) instead of assuming it is the first message.
    let mut service_exit = None;
    while let Ok(msg) = unique_watcher_rx.try_recv() {
        if msg.from == target_id {
            service_exit = Some(msg);
            break;
        }
    }
    let msg = service_exit.expect("unique watcher should receive service-exit notice");
    assert_eq!(msg.to, unique_watcher_id);
    assert_eq!(msg.session, 0);
    assert_eq!(msg.ptype(), PTYPE_SYSTEM);

    match msg.data {
        MessageBody::Buffer(_, data) => {
            let text = std::str::from_utf8(data.as_slice()).unwrap();
            assert!(text.starts_with("_service_exit,"));
            assert!(text.contains(&format!("Actor id:{} quited", target_id)));
        }
        other => panic!("unexpected service-exit payload: {}", other),
    }

    while let Ok(msg) = normal_watcher_rx.try_recv() {
        assert_ne!(
            msg.from, target_id,
            "ordinary services must not receive service-exit notices"
        );
    }

    assert!(pseudo_rx.try_recv().is_err());
    CONTEXT.actors.remove(&pseudo_id);
    CONTEXT.remove_actor(unique_watcher_id, &unique_watcher.name);
    CONTEXT.remove_actor(normal_watcher_id, &normal_watcher.name);
}
