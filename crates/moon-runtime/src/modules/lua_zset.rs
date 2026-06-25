//! Ordered set (sorted set) for leaderboards and rankings.
//!
//! Rust port of the C++ skiplist-based ordered set in
//! `moon/src/common/zset.hpp` plus its Lua binding `lua_zset.cpp`.
//!
//! The skiplist uses a single flat `Vec<u64>` arena that emulates the C++
//! `level[1]` flexible-array node: each node occupies one contiguous block of
//! words (`key, score, timestamp, level_len|backward`, then one packed word per
//! level), so a node hop touches a single cache line instead of chasing a
//! pointer into a separate level buffer. Nodes are addressed by their `u32` word
//! offset with a `NIL` sentinel for null links (no raw pointers between nodes).
//! `get_unchecked` is used on the hot paths since all offsets are produced by
//! the structure itself. The span-based O(log N) rank logic mirrors the original
//! (and Redis) skiplist.

use moon_base::laux::LuaState;
use moon_base::{cstr, ffi, laux, lreg, lreg_null, luaL_newlib};
use rand::RngExt;
use std::collections::HashMap;
use std::ffi::{c_char, c_int};
use std::hash::{BuildHasherDefault, Hasher};

const MAXLEVEL: usize = 32;
const NIL: u32 = u32::MAX;

const ZSET_META: *const c_char = cstr!("lzet");

/// Sort key: ordered by score (descending by default), then timestamp
/// ascending, then key ascending. Identity is by `key` alone.
#[derive(Clone, Copy, Default)]
struct Context {
    key: i64,
    score: i64,
    timestamp: i64,
}

impl Context {
    /// Mirrors C++ `operator<`: the strict ordering used while traversing the
    /// skiplist for insert/update/erase. A higher score sorts earlier.
    #[inline]
    fn lt(&self, other: &Context) -> bool {
        if self.score == other.score {
            if self.timestamp == other.timestamp {
                self.key < other.key
            } else {
                self.timestamp < other.timestamp
            }
        } else {
            self.score > other.score
        }
    }

    /// Mirrors C++ `operator<=`: same key is treated as "not after", otherwise
    /// fall back to `lt`. Used by `get_rank`.
    #[inline]
    fn le(&self, other: &Context) -> bool {
        self.key == other.key || self.lt(other)
    }
}

#[derive(Clone, Copy)]
struct Level {
    forward: u32,
    span: u32,
}

impl Level {
    const EMPTY: Level = Level {
        forward: NIL,
        span: 0,
    };
}

/// `u64` header words preceding a node's packed level array in the flat arena:
/// `[key, score, timestamp, (level_len << 32 | backward)]`.
const NODE_HEADER_WORDS: usize = 4;

const LOW32: u64 = 0xFFFF_FFFF;

/// Arena-backed skiplist with a C++-style flexible-array node layout emulated
/// inside a single `Vec<u64>`: each node occupies one contiguous block holding
/// its fixed fields followed by its packed level array, so a node hop touches a
/// single cache line (one shared allocation, no pointer chasing). A node is
/// addressed by its `u32` word offset into `buf`; `NIL` marks a null link. Freed
/// blocks are recycled per level-count, so steady-state insert/erase allocate
/// nothing. Each level entry is one word: `forward << 32 | span`.
struct SkipList {
    buf: Vec<u64>,
    /// `free_blocks[k]` holds word offsets of freed node blocks of level count k.
    free_blocks: Vec<Vec<u32>>,
    header: u32,
    tail: u32,
    level: usize,
    length: usize,
    /// Cached PRNG state for level generation. A per-instance xorshift avoids
    /// the thread-local fetch + range reduction of `rand::rng()` on every
    /// insert (matching the C++ original's cached `std::mt19937`).
    rng_state: u64,
}

impl SkipList {
    fn new(capacity_hint: usize) -> Self {
        let node_cap = capacity_hint.saturating_add(1).min(1 << 22);
        // Average ~1.33 levels/node => ~5.3 words/node; reserve generously.
        let word_cap = node_cap.saturating_mul(NODE_HEADER_WORDS + 2);
        let mut list = SkipList {
            buf: Vec::with_capacity(word_cap),
            free_blocks: (0..=MAXLEVEL).map(|_| Vec::new()).collect(),
            header: NIL,
            tail: NIL,
            level: 1,
            length: 0,
            // xorshift state must never be zero.
            rng_state: rand::rng().random_range(1..=u64::MAX),
        };
        list.init_header();
        list
    }

    #[inline]
    fn next_rand(&mut self) -> u32 {
        // xorshift64* — cheap and well-distributed enough for level selection.
        let mut x = self.rng_state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng_state = x;
        (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 32) as u32
    }

    fn init_header(&mut self) {
        let header = self.alloc_node(MAXLEVEL, Context::default());
        self.set_backward(header, NIL);
        self.header = header;
        self.tail = NIL;
    }

    fn clear(&mut self) {
        self.buf.clear();
        for slots in self.free_blocks.iter_mut() {
            slots.clear();
        }
        self.level = 1;
        self.length = 0;
        self.init_header();
    }

    fn alloc_node(&mut self, level: usize, score: Context) -> u32 {
        let off = if let Some(o) = self.free_blocks[level].pop() {
            o
        } else {
            let o = self.buf.len();
            self.buf.resize(o + NODE_HEADER_WORDS + level, 0);
            u32::try_from(o).expect("zset arena overflowed u32")
        };
        self.set_score(off, score);
        // Header word: level_len in the high 32 bits, backward (NIL) in the low.
        self.set_word(off, 3, ((level as u64) << 32) | (NIL as u64));
        for i in 0..level {
            self.set_level(off, i, Level::EMPTY);
        }
        off
    }

    fn free_node(&mut self, off: u32) {
        let len = self.level_len(off) as usize;
        self.free_blocks[len].push(off);
    }

    // Packed accessors over the flat `buf`. Every node offset handled here is
    // produced by the structure itself (header, allocated blocks, or `NIL`
    // checked before use), so word indices are always in bounds; `get_unchecked`
    // drops the bounds-check branches that dominate the tight traversal loops,
    // and `debug_assert` keeps the invariant verifiable in debug/test builds.
    #[inline]
    fn word(&self, off: u32, w: usize) -> u64 {
        debug_assert!(off as usize + w < self.buf.len());
        unsafe { *self.buf.get_unchecked(off as usize + w) }
    }

    #[inline]
    fn set_word(&mut self, off: u32, w: usize, value: u64) {
        debug_assert!(off as usize + w < self.buf.len());
        unsafe {
            *self.buf.get_unchecked_mut(off as usize + w) = value;
        }
    }

    #[inline]
    fn lvl(&self, node: u32, i: usize) -> Level {
        let w = self.word(node, NODE_HEADER_WORDS + i);
        Level {
            forward: (w >> 32) as u32,
            span: (w & LOW32) as u32,
        }
    }

    #[inline]
    fn set_level(&mut self, node: u32, i: usize, lv: Level) {
        let packed = ((lv.forward as u64) << 32) | (lv.span as u64);
        self.set_word(node, NODE_HEADER_WORDS + i, packed);
    }

    #[inline]
    fn forward(&self, node: u32, i: usize) -> u32 {
        (self.word(node, NODE_HEADER_WORDS + i) >> 32) as u32
    }

    #[inline]
    fn score_of(&self, node: u32) -> Context {
        Context {
            key: self.word(node, 0) as i64,
            score: self.word(node, 1) as i64,
            timestamp: self.word(node, 2) as i64,
        }
    }

    #[inline]
    fn set_score(&mut self, node: u32, value: Context) {
        self.set_word(node, 0, value.key as u64);
        self.set_word(node, 1, value.score as u64);
        self.set_word(node, 2, value.timestamp as u64);
    }

    #[inline]
    fn key_of(&self, node: u32) -> i64 {
        self.word(node, 0) as i64
    }

    #[inline]
    fn level_len(&self, node: u32) -> u32 {
        (self.word(node, 3) >> 32) as u32
    }

    #[inline]
    fn backward(&self, node: u32) -> u32 {
        (self.word(node, 3) & LOW32) as u32
    }

    #[inline]
    fn set_backward(&mut self, node: u32, value: u32) {
        let w = self.word(node, 3);
        self.set_word(node, 3, (w & (LOW32 << 32)) | (value as u64));
    }

    fn rand_level(&mut self) -> usize {
        let mut level = 1;
        while level < MAXLEVEL && (self.next_rand() & 3) == 0 {
            level += 1;
        }
        level
    }

    fn insert(&mut self, score: Context) -> u32 {
        let mut update = [NIL; MAXLEVEL];
        let mut rank = [0u32; MAXLEVEL];

        let mut x = self.header;
        for idx in (0..self.level).rev() {
            rank[idx] = if idx == self.level - 1 { 0 } else { rank[idx + 1] };
            loop {
                let lv = self.lvl(x, idx);
                if lv.forward != NIL && self.score_of(lv.forward).lt(&score) {
                    rank[idx] += lv.span;
                    x = lv.forward;
                } else {
                    break;
                }
            }
            update[idx] = x;
        }

        let level = self.rand_level();
        if level > self.level {
            let header = self.header;
            for i in self.level..level {
                rank[i] = 0;
                update[i] = header;
                let mut lv = self.lvl(header, i);
                lv.span = self.length as u32;
                self.set_level(header, i, lv);
            }
            self.level = level;
        }

        let x = self.alloc_node(level, score);
        for i in 0..level {
            let upd = update[i];
            let upd_lv = self.lvl(upd, i);
            let crossed = rank[0] - rank[i];
            // x takes update[i]'s old forward; update[i] now points to x.
            self.set_level(x, i, Level {
                forward: upd_lv.forward,
                span: upd_lv.span - crossed,
            });
            self.set_level(upd, i, Level {
                forward: x,
                span: crossed + 1,
            });
        }

        for (i, &upd) in update.iter().enumerate().take(self.level).skip(level) {
            let mut lv = self.lvl(upd, i);
            lv.span += 1;
            self.set_level(upd, i, lv);
        }

        let backward = if update[0] == self.header {
            NIL
        } else {
            update[0]
        };
        self.set_backward(x, backward);
        let x_forward = self.forward(x, 0);
        if x_forward != NIL {
            self.set_backward(x_forward, x);
        } else {
            self.tail = x;
        }
        self.length += 1;
        x
    }

    fn remove_node(&mut self, x: u32, update: &[u32; MAXLEVEL]) {
        for (i, &upd) in update.iter().enumerate().take(self.level) {
            let mut u_lv = self.lvl(upd, i);
            if u_lv.forward == x {
                let x_lv = self.lvl(x, i);
                // When x is the last node at this level, `x_lv.span` is 0; the
                // C++/Redis original relies on unsigned wraparound so the net
                // effect is `span -= 1`. The final value is always in range.
                u_lv.span = u_lv.span.wrapping_add(x_lv.span.wrapping_sub(1));
                u_lv.forward = x_lv.forward;
            } else {
                u_lv.span -= 1;
            }
            self.set_level(upd, i, u_lv);
        }

        let x_forward = self.forward(x, 0);
        let x_backward = self.backward(x);
        if x_forward != NIL {
            self.set_backward(x_forward, x_backward);
        } else {
            self.tail = x_backward;
        }

        while self.level > 1 && self.forward(self.header, self.level - 1) == NIL {
            self.level -= 1;
        }
        self.length -= 1;
    }

    fn update(&mut self, curscore: Context, newscore: Context) -> u32 {
        let mut update = [NIL; MAXLEVEL];

        let mut x = self.header;
        for idx in (0..self.level).rev() {
            loop {
                let fwd = self.forward(x, idx);
                if fwd != NIL && self.score_of(fwd).lt(&curscore) {
                    x = fwd;
                } else {
                    break;
                }
            }
            update[idx] = x;
        }

        let x = self.forward(x, 0);
        debug_assert!(x != NIL && self.key_of(x) == curscore.key);

        let backward = self.backward(x);
        let forward0 = self.forward(x, 0);
        let backward_ok = backward == NIL || self.score_of(backward).lt(&newscore);
        let forward_ok = forward0 == NIL || newscore.lt(&self.score_of(forward0));
        if backward_ok && forward_ok {
            self.set_score(x, newscore);
            return x;
        }

        self.remove_node(x, &update);
        let new_node = self.insert(newscore);
        self.free_node(x);
        new_node
    }

    fn get_rank(&self, score: &Context) -> usize {
        let mut x = self.header;
        let mut rank: usize = 0;
        for idx in (0..self.level).rev() {
            loop {
                let lv = self.lvl(x, idx);
                if lv.forward != NIL && self.score_of(lv.forward).le(score) {
                    rank += lv.span as usize;
                    x = lv.forward;
                } else {
                    break;
                }
            }
            if x != self.header && self.key_of(x) == score.key {
                return rank;
            }
        }
        0
    }

    fn find_by_rank(&self, rank: usize) -> u32 {
        // Ranks are 1-based; rank 0 would match the header node (traversed == 0).
        if rank == 0 {
            return NIL;
        }
        let mut x = self.header;
        let mut traversed: usize = 0;
        for idx in (0..self.level).rev() {
            loop {
                let lv = self.lvl(x, idx);
                if lv.forward != NIL && traversed + lv.span as usize <= rank {
                    traversed += lv.span as usize;
                    x = lv.forward;
                } else {
                    break;
                }
            }
            if traversed == rank {
                return x;
            }
        }
        NIL
    }

    fn erase(&mut self, score: &Context) -> usize {
        let mut update = [NIL; MAXLEVEL];

        let mut x = self.header;
        for idx in (0..self.level).rev() {
            loop {
                let fwd = self.forward(x, idx);
                if fwd != NIL && self.score_of(fwd).lt(score) {
                    x = fwd;
                } else {
                    break;
                }
            }
            update[idx] = x;
        }

        let x = self.forward(x, 0);
        if x != NIL && self.key_of(x) == score.key {
            self.remove_node(x, &update);
            self.free_node(x);
            return 1;
        }
        0
    }

    #[inline]
    fn begin(&self) -> u32 {
        self.forward(self.header, 0)
    }
}

/// Fast hasher for the `i64` keys in the dict. `std`'s default `HashMap` uses
/// SipHash (DoS-resistant but slow); these keys are trusted internal player ids,
/// so a single multiply + xorshift mix (FxHash/splitmix style) is plenty and
/// roughly matches the C++ `unordered_map`'s near-identity integer hash.
#[derive(Default)]
struct IntHasher(u64);

impl Hasher for IntHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Generic fallback; the dict only ever hashes `i64` keys (write_i64).
        for &b in bytes {
            self.write_u64(b as u64);
        }
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 32;
        self.0 = x;
    }

    #[inline]
    fn write_i64(&mut self, i: i64) {
        self.write_u64(i as u64);
    }
}

type IntBuildHasher = BuildHasherDefault<IntHasher>;

/// The ordered set: a skiplist for ordering plus a dictionary for O(1) lookup.
struct ZSet {
    reverse: bool,
    max_count: usize,
    zsl: SkipList,
    dict: HashMap<i64, u32, IntBuildHasher>,
}

impl ZSet {
    fn new(max_count: usize, reverse: bool) -> Self {
        let hint = if max_count == usize::MAX {
            1024
        } else {
            max_count
        };
        ZSet {
            reverse,
            max_count,
            zsl: SkipList::new(hint),
            dict: HashMap::with_capacity_and_hasher(hint.min(1 << 20), IntBuildHasher::default()),
        }
    }

    fn update(&mut self, key: i64, score: i64, timestamp: i64) {
        if self.max_count == 0 || key == 0 {
            return;
        }
        // checked_neg: -i64::MIN would overflow; clamp to i64::MAX to preserve reverse-sort order
        let score = if self.reverse {
            score.checked_neg().unwrap_or(i64::MAX)
        } else {
            score
        };
        let ctx = Context {
            key,
            score,
            timestamp,
        };

        let existing = self.dict.get(&key).copied();

        if self.dict.len() == self.max_count && existing.is_none() {
            let tail = self.zsl.tail;
            if tail != NIL && self.zsl.score_of(tail).lt(&ctx) {
                return;
            }
        }

        match existing {
            None => {
                let idx = self.zsl.insert(ctx);
                self.dict.insert(key, idx);
            }
            Some(old_idx) => {
                let cur = self.zsl.score_of(old_idx);
                let new_idx = self.zsl.update(cur, ctx);
                self.dict.insert(key, new_idx);
            }
        }

        if self.dict.len() > self.max_count {
            let tail = self.zsl.tail;
            if tail != NIL {
                let tail_key = self.zsl.key_of(tail);
                self.erase(tail_key);
            }
        }
    }

    fn rank(&self, key: i64) -> usize {
        match self.dict.get(&key) {
            Some(&idx) => {
                let ctx = self.zsl.score_of(idx);
                self.zsl.get_rank(&ctx)
            }
            None => 0,
        }
    }

    fn score(&self, key: i64) -> i64 {
        match self.dict.get(&key) {
            Some(&idx) => {
                let raw = self.zsl.score_of(idx).score;
                if self.reverse {
                    raw.checked_neg().unwrap_or(i64::MAX)
                } else {
                    raw
                }
            }
            None => 0,
        }
    }

    fn has(&self, key: i64) -> bool {
        self.dict.contains_key(&key)
    }

    fn clear(&mut self) {
        self.dict.clear();
        self.zsl.clear();
    }

    fn erase(&mut self, key: i64) -> usize {
        if let Some(&idx) = self.dict.get(&key) {
            let ctx = self.zsl.score_of(idx);
            self.zsl.erase(&ctx);
            self.dict.remove(&key);
            1
        } else {
            0
        }
    }

    fn size(&self) -> usize {
        self.dict.len()
    }

    fn key_by_rank(&self, rank: usize) -> Option<i64> {
        if self.size() == 0 || self.size() < rank {
            return None;
        }
        let node = if rank == 1 {
            self.zsl.begin()
        } else {
            self.zsl.find_by_rank(rank)
        };
        if node != NIL {
            Some(self.zsl.key_of(node))
        } else {
            None
        }
    }

    /// Mirrors the index normalization in `lua_zset.cpp`'s `lrange`.
    ///
    /// `start`/`stop` are the 1-based, inclusive arguments passed from Lua
    /// (negative counts from the end). Returns:
    /// - `Ok(None)` for an empty/out-of-range request (caller pushes nothing),
    /// - `Ok(Some(keys))` for a valid range,
    /// - `Err(rangelen)` when the range is too large to materialize.
    fn range(&self, start: i64, stop: i64, reverse: bool) -> Result<Option<Vec<i64>>, i64> {
        let llen = self.size() as i64;
        let mut start = start - 1;
        let mut end = stop - 1;

        if start < 0 {
            start += llen;
        }
        if end < 0 {
            end += llen;
        }
        if start < 0 {
            start = 0;
        }

        if start > end || start >= llen {
            return Ok(None);
        }
        if end >= llen {
            end = llen - 1;
        }

        let rangelen = end - start + 1;
        // Cap range length to prevent accidental or malicious OOM from a
        // single range query (1 000 000 × 8 bytes = 8 MB per call).
        const MAX_RANGE_LEN: i64 = 1_000_000;
        if rangelen > MAX_RANGE_LEN {
            return Err(rangelen);
        }

        let mut node = if reverse {
            if start > 0 {
                self.zsl.find_by_rank((llen - start) as usize)
            } else {
                self.zsl.tail
            }
        } else if start > 0 {
            self.zsl.find_by_rank((start + 1) as usize)
        } else {
            self.zsl.begin()
        };

        let mut result = Vec::with_capacity(rangelen as usize);
        let mut remaining = rangelen;
        while remaining > 0 && node != NIL {
            result.push(self.zsl.key_of(node));
            node = if reverse {
                self.zsl.backward(node)
            } else {
                self.zsl.forward(node, 0)
            };
            remaining -= 1;
        }

        Ok(Some(result))
    }
}

/// Fetch the `ZSet` userdata bound as the method receiver.
///
/// These methods are only reachable through the zset's own metatable
/// `__index`, so argument 1 is always a zset userdata. We therefore skip the
/// per-call `luaL_checkudata` metatable string-compare and read the pointer
/// directly (same approach as `lua_redis.rs`), erroring only on a null pointer.
fn get_zset(state: LuaState, index: i32) -> &'static mut ZSet {
    laux::lua_touserdata::<ZSet>(state, index)
        .unwrap_or_else(|| laux::lua_error(state, "zset: expected zset userdata".to_string()))
}

extern "C-unwind" fn update(state: LuaState) -> c_int {
    let zset = get_zset(state, 1);
    let key = laux::lua_get::<i64>(state, 2);
    let score = laux::lua_get::<i64>(state, 3);
    let timestamp = laux::lua_get::<i64>(state, 4);
    if score == i64::MIN {
        laux::lua_error(
            state,
            "zset: score cannot be i64::MIN (negation overflow in reverse mode)".to_string(),
        );
    }
    zset.update(key, score, timestamp);
    0
}

extern "C-unwind" fn rank(state: LuaState) -> c_int {
    let zset = get_zset(state, 1);
    let key = laux::lua_get::<i64>(state, 2);
    let v = zset.rank(key);
    if v > 0 {
        laux::lua_push(state, v as ffi::lua_Integer);
        1
    } else {
        0
    }
}

extern "C-unwind" fn key_by_rank(state: LuaState) -> c_int {
    let zset = get_zset(state, 1);
    let rank = laux::lua_get::<i64>(state, 2);
    if rank <= 0 {
        return 0;
    }
    match zset.key_by_rank(rank as usize) {
        Some(key) => {
            laux::lua_push(state, key as ffi::lua_Integer);
            1
        }
        None => 0,
    }
}

extern "C-unwind" fn score(state: LuaState) -> c_int {
    let zset = get_zset(state, 1);
    let key = laux::lua_get::<i64>(state, 2);
    laux::lua_push(state, zset.score(key) as ffi::lua_Integer);
    1
}

extern "C-unwind" fn has(state: LuaState) -> c_int {
    let zset = get_zset(state, 1);
    let key = laux::lua_get::<i64>(state, 2);
    laux::lua_push(state, zset.has(key));
    1
}

extern "C-unwind" fn size(state: LuaState) -> c_int {
    let zset = get_zset(state, 1);
    laux::lua_push(state, zset.size() as ffi::lua_Integer);
    1
}

extern "C-unwind" fn clear(state: LuaState) -> c_int {
    let zset = get_zset(state, 1);
    zset.clear();
    0
}

extern "C-unwind" fn erase(state: LuaState) -> c_int {
    let zset = get_zset(state, 1);
    let key = laux::lua_get::<i64>(state, 2);
    laux::lua_push(state, zset.erase(key) as ffi::lua_Integer);
    1
}

extern "C-unwind" fn range(state: LuaState) -> c_int {
    let zset = get_zset(state, 1);
    let start = laux::lua_get::<i64>(state, 2);
    let stop = laux::lua_get::<i64>(state, 3);
    let reverse = laux::lua_opt::<bool>(state, 4).unwrap_or(false);

    match zset.range(start, stop, reverse) {
        Ok(None) => 0,
        Ok(Some(keys)) => {
            let table = laux::LuaTable::new(state, keys.len(), 0);
            for (i, key) in keys.into_iter().enumerate() {
                laux::lua_push(state, key as ffi::lua_Integer);
                table.rawseti(i + 1);
            }
            1
        }
        Err(rangelen) => laux::lua_error(
            state,
            format!(
                "zset.range: range length exceeds maximum supported size (requested={}, max={})",
                rangelen,
                i32::MAX - 1
            ),
        ),
    }
}

extern "C-unwind" fn create(state: LuaState) -> c_int {
    let max_count = laux::lua_get::<i64>(state, 1);
    let max_count = if max_count < 0 { 0 } else { max_count as usize };
    let reverse = laux::lua_opt::<bool>(state, 2).unwrap_or(false);

    let methods = [
        lreg!("update", update),
        lreg!("has", has),
        lreg!("rank", rank),
        lreg!("key_by_rank", key_by_rank),
        lreg!("score", score),
        lreg!("range", range),
        lreg!("clear", clear),
        lreg!("size", size),
        lreg!("erase", erase),
        lreg_null!(),
    ];

    if laux::lua_newuserdata(state, ZSet::new(max_count, reverse), ZSET_META, methods.as_ref())
        .is_none()
    {
        laux::lua_error(state, "zset: failed to allocate userdata".to_string());
    }
    1
}

pub extern "C-unwind" fn luaopen_zset(state: LuaState) -> c_int {
    let l = [lreg!("new", create), lreg_null!()];
    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys_in_order(zset: &ZSet) -> Vec<i64> {
        let mut out = Vec::new();
        let mut node = zset.zsl.begin();
        while node != NIL {
            out.push(zset.zsl.key_of(node));
            node = zset.zsl.forward(node, 0);
        }
        out
    }

    #[test]
    fn descending_score_then_timestamp_then_key() {
        let mut z = ZSet::new(usize::MAX, false);
        z.update(1, 100, 10);
        z.update(2, 200, 5);
        z.update(3, 100, 8); // same score as key 1, earlier timestamp
        z.update(4, 100, 8); // same score & timestamp as key 3, larger key

        // 200 first; then score 100 group ordered by timestamp asc then key asc.
        assert_eq!(keys_in_order(&z), vec![2, 3, 4, 1]);
        assert_eq!(z.size(), 4);
    }

    #[test]
    fn rank_and_score_and_has() {
        let mut z = ZSet::new(usize::MAX, false);
        z.update(1, 100, 1);
        z.update(2, 200, 1);
        z.update(3, 50, 1);

        assert_eq!(z.rank(2), 1);
        assert_eq!(z.rank(1), 2);
        assert_eq!(z.rank(3), 3);
        assert_eq!(z.rank(999), 0); // missing

        assert_eq!(z.score(2), 200);
        assert_eq!(z.score(999), 0);
        assert!(z.has(1));
        assert!(!z.has(999));
    }

    #[test]
    fn update_existing_repositions() {
        let mut z = ZSet::new(usize::MAX, false);
        z.update(1, 100, 1);
        z.update(2, 200, 1);
        z.update(3, 50, 1);
        assert_eq!(z.rank(1), 2);

        // Bump key 1 to the top.
        z.update(1, 300, 1);
        assert_eq!(z.rank(1), 1);
        assert_eq!(z.score(1), 300);
        assert_eq!(z.size(), 3);
        assert_eq!(keys_in_order(&z), vec![1, 2, 3]);

        // Drop key 1 to the bottom.
        z.update(1, 10, 1);
        assert_eq!(z.rank(1), 3);
        assert_eq!(keys_in_order(&z), vec![2, 3, 1]);
    }

    #[test]
    fn reverse_orders_ascending() {
        let mut z = ZSet::new(usize::MAX, true);
        z.update(1, 100, 1);
        z.update(2, 200, 1);
        z.update(3, 50, 1);

        // Lower score ranks first in reverse mode.
        assert_eq!(z.rank(3), 1);
        assert_eq!(z.rank(1), 2);
        assert_eq!(z.rank(2), 3);
        // Score is reported un-negated.
        assert_eq!(z.score(2), 200);
        assert_eq!(keys_in_order(&z), vec![3, 1, 2]);
    }

    #[test]
    fn eviction_at_max_count() {
        let mut z = ZSet::new(3, false);
        z.update(1, 100, 1);
        z.update(2, 200, 1);
        z.update(3, 50, 1);
        assert_eq!(z.size(), 3);

        // 300 is better than the worst (key 3, score 50) -> inserted, key 3 evicted.
        z.update(4, 300, 1);
        assert_eq!(z.size(), 3);
        assert!(!z.has(3));
        assert_eq!(keys_in_order(&z), vec![4, 2, 1]);

        // 10 is worse than the current worst -> rejected.
        z.update(5, 10, 1);
        assert_eq!(z.size(), 3);
        assert!(!z.has(5));
    }

    #[test]
    fn erase_removes() {
        let mut z = ZSet::new(usize::MAX, false);
        z.update(1, 100, 1);
        z.update(2, 200, 1);
        assert_eq!(z.erase(1), 1);
        assert_eq!(z.erase(1), 0);
        assert_eq!(z.size(), 1);
        assert_eq!(z.rank(2), 1);
    }

    #[test]
    fn key_by_rank_bounds() {
        let mut z = ZSet::new(usize::MAX, false);
        z.update(1, 100, 1);
        z.update(2, 200, 1);
        z.update(3, 50, 1);

        assert_eq!(z.key_by_rank(1), Some(2));
        assert_eq!(z.key_by_rank(2), Some(1));
        assert_eq!(z.key_by_rank(3), Some(3));
        assert_eq!(z.key_by_rank(4), None);
    }

    #[test]
    fn range_positive_negative_and_reverse() {
        let mut z = ZSet::new(usize::MAX, false);
        for (k, s) in [(1, 100), (2, 200), (3, 50), (4, 300), (5, 10)] {
            z.update(k, s, 1);
        }
        // Order: 4(300), 2(200), 1(100), 3(50), 5(10)
        assert_eq!(keys_in_order(&z), vec![4, 2, 1, 3, 5]);

        assert_eq!(z.range(1, 2, false).unwrap(), Some(vec![4, 2]));
        assert_eq!(z.range(1, 3, true).unwrap(), Some(vec![5, 3, 1]));
        // Negative indices follow the original C++ semantics: the argument is
        // first decremented (1-based -> 0-based) and then offset by llen, so
        // range(-2, -1) maps to 0-based [2, 3] => ranks 3 and 4 => keys 1, 3.
        assert_eq!(z.range(-2, -1, false).unwrap(), Some(vec![1, 3]));
        // out of range -> None
        assert_eq!(z.range(10, 20, false).unwrap(), None);
        // clamped end: start=4 (0-based 3) through end -> keys at ranks 4,5
        assert_eq!(z.range(4, 100, false).unwrap(), Some(vec![3, 5]));
    }

    #[test]
    fn ignores_zero_key_and_zero_capacity() {
        let mut z = ZSet::new(usize::MAX, false);
        z.update(0, 100, 1);
        assert_eq!(z.size(), 0);

        let mut z0 = ZSet::new(0, false);
        z0.update(1, 100, 1);
        assert_eq!(z0.size(), 0);
    }

    #[test]
    fn large_set_rank_consistency() {
        let mut z = ZSet::new(usize::MAX, false);
        let n = 2000i64;
        for k in 1..=n {
            z.update(k, k, 1); // score == key
        }
        assert_eq!(z.size(), n as usize);
        // Highest score (== n) is rank 1.
        assert_eq!(z.rank(n), 1);
        assert_eq!(z.rank(1), n as usize);
        assert_eq!(z.key_by_rank(1), Some(n));
        assert_eq!(z.key_by_rank(n as usize), Some(1));
    }

    /// Pure-Rust CPU flamegraph of the skiplist, with zero Lua/FFI in the
    /// stacks. Ignored by default; run explicitly with an optimized profile:
    ///
    /// ```text
    /// cargo test -p moon-runtime --profile profiling \
    ///     lua_zset::tests::profile_flamegraph -- --ignored --nocapture
    /// ```
    ///
    /// Writes one SVG per operation under `target/profile/`. Open them in a
    /// browser (zoomable / searchable). Tune size via env `ZSET_N` / `ZSET_OPS`.
    #[test]
    #[ignore = "manual profiling; emits flamegraph SVGs"]
    fn profile_flamegraph() {
        use std::io::Write;

        let n: i64 = std::env::var("ZSET_N").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
        let ops: usize = std::env::var("ZSET_OPS").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);

        // Local xorshift so key selection cost stays out of the skiplist frames.
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            seed ^= seed >> 12;
            seed ^= seed << 25;
            seed ^= seed >> 27;
            seed.wrapping_mul(0x2545_F491_4F6C_DD1D)
        };

        let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/profile");
        std::fs::create_dir_all(&out_dir).unwrap();

        // Run `body` under a fresh process-wide sampling profiler and write an SVG.
        let profile = |name: &str, body: &mut dyn FnMut()| {
            let guard = pprof::ProfilerGuardBuilder::default()
                .frequency(1000)
                .blocklist(&["libc", "libgcc", "pthread", "vdso", "libdyld"])
                .build()
                .expect("start profiler");
            body();
            let report = guard.report().build().expect("build report");
            let path = out_dir.join(format!("zset_{name}.svg"));
            let file = std::fs::File::create(&path).unwrap();
            report.flamegraph(file).expect("write flamegraph");
            println!("[flamegraph] {name:14} -> {}", path.canonicalize().unwrap().display());
        };

        let mut z = ZSet::new(usize::MAX, false);

        profile("build_insert", &mut || {
            for k in 1..=n {
                let s = (next() % 1_000_000) as i64;
                z.update(k, s, 1);
            }
        });

        profile("rank", &mut || {
            let mut acc = 0usize;
            for _ in 0..ops {
                let k = (next() % n as u64) as i64 + 1;
                acc = acc.wrapping_add(z.rank(k));
            }
            std::hint::black_box(acc);
        });

        profile("key_by_rank", &mut || {
            let mut acc = 0i64;
            for _ in 0..ops {
                let r = (next() % n as u64) as usize + 1;
                acc = acc.wrapping_add(z.key_by_rank(r).unwrap_or(0));
            }
            std::hint::black_box(acc);
        });

        profile("update_reposition", &mut || {
            for _ in 0..ops {
                let k = (next() % n as u64) as i64 + 1;
                let s = (next() % 1_000_000) as i64;
                z.update(k, s, 1);
            }
        });

        // erase + reinsert keeps the set size stable across the loop.
        profile("erase_reinsert", &mut || {
            for _ in 0..ops {
                let k = (next() % n as u64) as i64 + 1;
                let s = z.score(k);
                if z.erase(k) == 1 {
                    z.update(k, s, 1);
                }
            }
        });

        std::io::stdout().flush().ok();
        assert_eq!(z.size(), n as usize);
    }
}
