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

use crate::hash::FxBuildHasher;
use moon_base::laux::{LuaStack, LuaState};
use moon_base::{cstr, ffi, laux, lreg_null, lreg_try, luaL_newlib};
use rand::RngExt;
use std::collections::HashMap;
use std::ffi::{c_char, c_int};

const MAXLEVEL: usize = 32;
const NIL: u32 = u32::MAX;

/// Cap range length to prevent accidental or malicious OOM from a single range
/// query (8 bytes/element). Sourced from the crate-wide [`crate::LIMITS`] catalog
/// so it stays discoverable alongside the DB result-set caps.
const MAX_RANGE_LEN: i64 = crate::LIMITS.zset_range_len as i64;

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
            rank[idx] = if idx == self.level - 1 {
                0
            } else {
                rank[idx + 1]
            };
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
            self.set_level(
                x,
                i,
                Level {
                    forward: upd_lv.forward,
                    span: upd_lv.span - crossed,
                },
            );
            self.set_level(
                upd,
                i,
                Level {
                    forward: x,
                    span: crossed + 1,
                },
            );
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

/// The ordered set: a skiplist for ordering plus a dictionary for O(1) lookup.
///
/// The dict is keyed by trusted internal `i64` player ids, so it uses the
/// crate-wide FxHash (`crate::hash`) instead of std's DoS-resistant-but-slow
/// SipHash, roughly matching the C++ `unordered_map`'s near-identity integer
/// hash.
struct ZSet {
    reverse: bool,
    max_count: usize,
    zsl: SkipList,
    dict: HashMap<i64, u32, FxBuildHasher>,
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
            dict: HashMap::with_capacity_and_hasher(hint.min(1 << 20), FxBuildHasher::default()),
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
    fn range(&self, start: i64, stop: i64, reverse: bool) -> Result<Option<ZSetRange<'_>>, i64> {
        let llen = self.size() as i64;
        let mut start = start.saturating_sub(1);
        let mut end = stop.saturating_sub(1);

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
        if rangelen > MAX_RANGE_LEN {
            return Err(rangelen);
        }

        let node = if reverse {
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

        Ok(Some(ZSetRange {
            list: &self.zsl,
            node,
            remaining: rangelen as usize,
            reverse,
        }))
    }
}

struct ZSetRange<'a> {
    list: &'a SkipList,
    node: u32,
    remaining: usize,
    reverse: bool,
}

impl Iterator for ZSetRange<'_> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let key = self.list.key_of(self.node);
        self.node = if self.reverse {
            self.list.backward(self.node)
        } else {
            self.list.forward(self.node, 0)
        };
        self.remaining -= 1;
        Some(key)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for ZSetRange<'_> {}

/// Fetch the `ZSet` userdata bound as the method receiver.
///
/// These methods are only reachable through the zset's own metatable
/// `__index`, so argument 1 is always a zset userdata. We therefore skip the
/// per-call type/metatable checks and read the pointer directly (same approach
/// as the original FFI implementation and `lua_redis.rs`).
///
/// # Safety
/// `index` must be the zset userdata receiver installed by `create`.
#[inline(always)]
unsafe fn get_zset(lua: &LuaStack<'_>, index: i32) -> std::ptr::NonNull<ZSet> {
    unsafe { lua.userdata_ptr_unchecked(index) }
}

fn update(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let mut zset_ptr = unsafe { get_zset(lua, 1) };
    let zset = unsafe { zset_ptr.as_mut() };
    let key = lua.get::<i64>(2)?;
    let score = lua.get::<i64>(3)?;
    let timestamp = lua.get::<i64>(4)?;
    if score == i64::MIN {
        return Err(
            "zset: score cannot be i64::MIN (negation overflow in reverse mode)".to_string(),
        );
    }
    zset.update(key, score, timestamp);
    Ok(0)
}

fn rank(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut zset_ptr = unsafe { get_zset(lua, 1) };
    let zset = unsafe { zset_ptr.as_mut() };
    let key = lua.get::<i64>(2)?;
    let v = zset.rank(key);
    if v > 0 {
        laux::lua_push(state, v as ffi::lua_Integer);
        Ok(1)
    } else {
        Ok(0)
    }
}

fn key_by_rank(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut zset_ptr = unsafe { get_zset(lua, 1) };
    let zset = unsafe { zset_ptr.as_mut() };
    let rank = lua.get::<i64>(2)?;
    if rank <= 0 {
        return Ok(0);
    }
    match zset.key_by_rank(rank as usize) {
        Some(key) => {
            laux::lua_push(state, key as ffi::lua_Integer);
            Ok(1)
        }
        None => Ok(0),
    }
}

fn score(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut zset_ptr = unsafe { get_zset(lua, 1) };
    let zset = unsafe { zset_ptr.as_mut() };
    let key = lua.get::<i64>(2)?;
    laux::lua_push(state, zset.score(key) as ffi::lua_Integer);
    Ok(1)
}

fn has(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut zset_ptr = unsafe { get_zset(lua, 1) };
    let zset = unsafe { zset_ptr.as_mut() };
    let key = lua.get::<i64>(2)?;
    laux::lua_push(state, zset.has(key));
    Ok(1)
}

fn size(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut zset_ptr = unsafe { get_zset(lua, 1) };
    let zset = unsafe { zset_ptr.as_mut() };
    laux::lua_push(state, zset.size() as ffi::lua_Integer);
    Ok(1)
}

fn clear(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let mut zset_ptr = unsafe { get_zset(lua, 1) };
    let zset = unsafe { zset_ptr.as_mut() };
    zset.clear();
    Ok(0)
}

fn erase(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut zset_ptr = unsafe { get_zset(lua, 1) };
    let zset = unsafe { zset_ptr.as_mut() };
    let key = lua.get::<i64>(2)?;
    laux::lua_push(state, zset.erase(key) as ffi::lua_Integer);
    Ok(1)
}

fn range(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut zset_ptr = unsafe { get_zset(lua, 1) };
    let zset = unsafe { zset_ptr.as_mut() };
    let start = lua.get::<i64>(2)?;
    let stop = lua.get::<i64>(3)?;
    let reverse = lua.opt_truthy(4).unwrap_or(false);

    match zset.range(start, stop, reverse) {
        Ok(None) => Ok(0),
        Ok(Some(keys)) => {
            let table = laux::LuaTable::new(state, keys.len(), 0);
            for (i, key) in keys.enumerate() {
                laux::lua_push(state, key as ffi::lua_Integer);
                table.rawseti(i + 1);
            }
            Ok(1)
        }
        Err(rangelen) => Err(format!(
            "zset.range: range length exceeds maximum supported size (requested={}, max={})",
            rangelen, MAX_RANGE_LEN
        )),
    }
}

fn create(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let max_count = lua.get::<i64>(1)?;
    let max_count = if max_count < 0 { 0 } else { max_count as usize };
    let reverse = lua.opt_truthy(2).unwrap_or(false);

    let methods = [
        lreg_try!("update", update),
        lreg_try!("has", has),
        lreg_try!("rank", rank),
        lreg_try!("key_by_rank", key_by_rank),
        lreg_try!("score", score),
        lreg_try!("range", range),
        lreg_try!("clear", clear),
        lreg_try!("size", size),
        lreg_try!("erase", erase),
        lreg_null!(),
    ];

    if laux::lua_newuserdata(
        state,
        ZSet::new(max_count, reverse),
        ZSET_META,
        methods.as_ref(),
    )
    .is_none()
    {
        return Err("zset: failed to allocate userdata".to_string());
    }
    Ok(1)
}

pub extern "C-unwind" fn luaopen_zset(state: LuaState) -> c_int {
    let l = [lreg_try!("new", create), lreg_null!()];
    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
#[path = "../../tests/unit/modules/lua_zset.rs"]
mod tests;
