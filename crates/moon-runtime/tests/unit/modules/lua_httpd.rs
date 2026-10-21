use super::*;

fn fill_cache(cache: &FileMetaCache, cached_at: Instant) {
    for i in 0..MAX_CACHE_ENTRIES {
        cache.entries.insert(
            PathBuf::from(i.to_string()),
            CacheEntry {
                meta: None,
                cached_at,
            },
        );
    }
}

#[test]
fn full_cache_skips_new_keys_but_refreshes_existing_keys() {
    let cache = FileMetaCache::new();
    let old = Instant::now() - CACHE_TTL;
    fill_cache(&cache, old);
    // Expired entries are retained until the cleanup interval elapses.
    cache.update(Path::new("overflow"), None);
    assert_eq!(cache.entries.len(), MAX_CACHE_ENTRIES);
    assert!(!cache.entries.contains_key(Path::new("overflow")));
    cache.update(Path::new("0"), None);
    assert!(cache.entries.get(Path::new("0")).unwrap().cached_at > old);
}

#[test]
fn full_cache_prunes_expired_entries_after_cleanup_interval() {
    let cache = FileMetaCache::new();
    let old = Instant::now() - CACHE_TTL;
    fill_cache(&cache, old);
    *cache.last_cleanup.lock().unwrap() = old;
    cache.update(Path::new("fresh"), None);
    assert_eq!(cache.entries.len(), 1);
    assert!(cache.entries.contains_key(Path::new("fresh")));
}

#[test]
fn cleanup_keeps_live_entries_and_does_not_repeat_on_the_next_miss() {
    let cache = FileMetaCache::new();
    fill_cache(&cache, Instant::now());
    let old = Instant::now() - CACHE_TTL;
    *cache.last_cleanup.lock().unwrap() = old;
    cache.update(Path::new("overflow"), None);
    assert_eq!(cache.entries.len(), MAX_CACHE_ENTRIES);
    assert!(!cache.entries.contains_key(Path::new("overflow")));
    let cleaned_at = *cache.last_cleanup.lock().unwrap();
    assert!(cleaned_at > old);
    cache.update(Path::new("another miss"), None);
    assert_eq!(*cache.last_cleanup.lock().unwrap(), cleaned_at);
}

#[test]
fn concurrent_cache_misses_respect_capacity() {
    let cache = Arc::new(FileMetaCache::new());
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let cache = cache.clone();
            scope.spawn(move || {
                for i in 0..MAX_CACHE_ENTRIES / 2 {
                    cache.update(Path::new(&format!("{worker}/{i}")), None);
                }
            });
        }
    });
    assert_eq!(cache.entries.len(), MAX_CACHE_ENTRIES);
}

#[test]
fn request_parts_decode_to_existing_lua_fields_without_string_staging() {
    use moon_lua_sys::laux::LuaGlobalState;
    use std::ptr::NonNull;
    let request = Request::builder()
        .method("POST")
        .uri("/hello%20world?key=a%26b&empty=")
        .header("x-example", "first")
        .header("x-example", "second")
        .header(
            "x-binary",
            hyper::header::HeaderValue::from_bytes(b"\xff").unwrap(),
        )
        .body(Bytes::from_static(b"\x00\xffbody"))
        .unwrap();
    let (parts, body) = request.into_parts();
    let (tx, mut rx) = oneshot::channel();
    let state = NonNull::new(unsafe { ffi::luaL_newstate() }).unwrap();
    let _guard = LuaGlobalState::new(state);
    let last_header = parts
        .headers
        .get_all("x-example")
        .iter()
        .next_back()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        push_httpd_request(
            state,
            HttpSrvRequest {
                method: parts.method,
                uri: parts.uri,
                headers: parts.headers,
                body,
                response_tx: tx,
            }
        ),
        2
    );
    let mut lua = unsafe { LuaStack::from_raw(state) };
    assert_eq!(lua.opt_field::<String>(1, "method").unwrap(), "POST");
    assert_eq!(
        lua.opt_field::<String>(1, "path").unwrap(),
        "/hello%20world"
    );
    assert_eq!(
        lua.opt_field::<String>(1, "query_string").unwrap(),
        "key=a%26b&empty="
    );
    unsafe {
        ffi::lua_getfield(state.as_ptr(), 1, cstr!("body"));
    }
    assert_eq!(lua.value(-1).as_bytes().unwrap(), b"\x00\xffbody");
    unsafe {
        ffi::lua_getfield(state.as_ptr(), 1, cstr!("headers"));
    }
    assert_eq!(
        lua.opt_field::<String>(-1, "x-example").unwrap(),
        last_header
    );
    assert_eq!(lua.opt_field::<String>(-1, "x-binary").unwrap(), "");
    drop(_guard);
    assert!(matches!(
        rx.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
}
