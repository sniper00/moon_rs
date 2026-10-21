//! UUID / player-UID generator.
//!
//! Produces two kinds of non-negative 64-bit ids that share one per-type atomic
//! counter. `type == 0` produces a player UID; `type` in `1..=TYPE_MAX` produces
//! a UUID.
//!
//! ## Bit layout
//!
//! `type` lives in the high bits, so `type == 0` (i.e.
//! `(uuid >> TYPE_LEFT_SHIFT) == 0`) identifies a player UID with no dedicated
//! flag bit. `TYPE_LEFT_SHIFT = 53` keeps `type` below the sign bit (bit 63), so
//! every id stays non-negative.
//!
//! ```text
//! UUID (type in 1..=1023):
//!   bit 63       bits 53..62   bits 37..52    bits 0..36
//!   +----------+-------------+--------------+---------------+
//!   | 0 (sign) | type (10)   | serverid(16) | sequence (37) |
//!   +----------+-------------+--------------+---------------+
//!
//! Player UID (type == 0):
//!   bits 48..63    bits 40..47   bits 24..39    bits 0..23
//!   +------------+-------------+--------------+---------------+
//!   | 0 (16 bits)| channel (8) | serverid(16) | sequence (24) |
//!   +------------+-------------+--------------+---------------+
//! ```
//!
//! A player UID tops out at bit 47, so bits 48..63 (including the `type` field at
//! 53..62) are always zero — that is what tells the two kinds apart.
//!
//! ## Persistence strategy
//!
//! The counter for each type is the next value to be assigned (pre-increment:
//! `issue_next` returns the value handed out and stops at the field cap so a
//! saturated counter never grows unbounded). To survive crashes the counters
//! are periodically dumped to a database, but with a safety margin: a *periodic*
//! dump reports `counter + margin` so a crash never reissues ids that were
//! handed out after the last save. A *clean-shutdown* dump reports the actual
//! counter. On startup `init` receives the previously-stored counter and adds a
//! random jitter (1000..=10000) so externally observed ids do not reveal a fixed
//! stride.

use moon_lua_sys::{cstr, ffi, laux, lreg, lreg_null, lreg_try, luaL_newlib};
use rand::RngExt;
use std::ffi::c_int;
use std::sync::atomic::{AtomicI64, Ordering};

use moon_lua_sys::laux::{LuaStack, LuaState};

// ---- UUID (type != 0): type(10) | serverid(16) | sequence(37) ----
// type lives in the high bits so that `type == 0` (=> player UID) is detectable
// without a dedicated flag bit. TYPE_LEFT_SHIFT = 53 keeps type below the sign
// bit (bit 63); UUIDs stay non-negative for type up to TYPE_MAX.
const TYPE_BITS: i64 = 10;
const SERVERID_BITS: i64 = 16;
const SEQUENCE_BITS: i64 = 37;

const TYPE_MAX: i64 = (1 << TYPE_BITS) - 1;
const SERVERID_MAX: i64 = (1 << SERVERID_BITS) - 1;
const SEQUENCE_MAX: i64 = (1 << SEQUENCE_BITS) - 1;

const SEQUENCE_LEFT_SHIFT: i64 = 0;
const SERVERID_LEFT_SHIFT: i64 = SEQUENCE_LEFT_SHIFT + SEQUENCE_BITS; // 37
const TYPE_LEFT_SHIFT: i64 = SERVERID_LEFT_SHIFT + SERVERID_BITS; // 53

// ---- Player UID (type == 0): channel(8) | serverid(16) | sequence(24) ----
// Total width is 48 bits, so bits 48..63 (including the type field at
// 53..62) are zero — that is what distinguishes a player UID from a UUID.
const UID_CHANNEL_BITS: i64 = 8;
const UID_SERVERID_BITS: i64 = 16;
const UID_SEQUENCE_BITS: i64 = 24;

const UID_CHANNEL_MAX: i64 = (1 << UID_CHANNEL_BITS) - 1;
const UID_SERVERID_MAX: i64 = (1 << UID_SERVERID_BITS) - 1;
const UID_SEQUENCE_MAX: i64 = (1 << UID_SEQUENCE_BITS) - 1;

const UID_SEQUENCE_LEFT_SHIFT: i64 = 0;
const UID_SERVERID_LEFT_SHIFT: i64 = UID_SEQUENCE_LEFT_SHIFT + UID_SEQUENCE_BITS; // 24
const UID_CHANNEL_LEFT_SHIFT: i64 = UID_SERVERID_LEFT_SHIFT + UID_SERVERID_BITS; // 40

/// Margin added to the real counter when dumping for a *periodic* save. A crash
/// can only lose ids handed out after the last periodic dump; advancing the
/// persisted value by this much makes that window safe without reissuing ids.
/// (Used only on the periodic path; clean-shutdown dumps the real counter.)
const SEQUENCE_SAVE_MARGIN: i64 = 10_000_000;

/// Periodic-save margin for the player-UID counter (type 0). The player-UID
/// sequence space is only 24 bits (`UID_SEQUENCE_MAX`), so the 37-bit UUID
/// margin would be far too large — a periodic dump could report a value past
/// the 24-bit cap and brick player-UID generation on the next `init`. Player
/// UIDs are also issued far more slowly than general UUIDs, so a smaller margin
/// still comfortably covers the between-save window.
const UID_SEQUENCE_SAVE_MARGIN: i64 = 100_000;

/// On `init`, a previously-stored counter is jittered by a random amount in
/// `[SEQUENCE_INIT_JITTER_MIN, SEQUENCE_INIT_JITTER_MAX]` so the first ids
/// issued after restart do not follow a predictable stride.
const SEQUENCE_INIT_JITTER_MIN: i64 = 1_000;
const SEQUENCE_INIT_JITTER_MAX: i64 = 10_000;

/// Per-type counter state. `sequence[type]` is the *next* value to be assigned.
struct UuidState {
    serverid: AtomicI64,
    channel: AtomicI64,
    /// `TYPE_MAX + 1` slots (index 0 is the player-UID counter).
    sequence: [AtomicI64; (TYPE_MAX + 1) as usize],
}

// `AtomicI64::new` is `const`, so the array can be statically initialized.
// Until `init` runs, `serverid == 0` gates `next`.
static UUID_STATE: UuidState = UuidState {
    serverid: AtomicI64::new(0),
    channel: AtomicI64::new(0),
    sequence: [const { AtomicI64::new(0) }; (TYPE_MAX + 1) as usize],
};

fn linit(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let channel: i64 = lua.get(1).map_err(|err| format!("uuid.init: {err}"))?;
    let serverid: i64 = lua.get(2).map_err(|err| format!("uuid.init: {err}"))?;
    // arg 3: persisted sequence table (may be empty / nil). Each entry is the
    // previously-dumped counter for the corresponding type (index 1 = type 0).
    let has_table = unsafe { ffi::lua_type(state.as_ptr(), 3) } == ffi::LUA_TTABLE;

    if !(channel > 0 && channel <= UID_CHANNEL_MAX) {
        return Err(format!(
            "uuid.init: channel out of limit (1..={})",
            UID_CHANNEL_MAX
        ));
    }
    if !(serverid > 0 && serverid <= SERVERID_MAX) {
        return Err(format!(
            "uuid.init: serverid out of limit (1..={})",
            SERVERID_MAX
        ));
    }

    // Seed the counters first, then publish `serverid` last (Release, below) so
    // `next` never observes an initialized serverid alongside stale counters.
    if has_table {
        let len = unsafe { ffi::lua_rawlen(state.as_ptr(), 3) } as i64;
        if len != 0 && len != TYPE_MAX + 1 {
            return Err(format!(
                "uuid.init: sequence table size error, expected 0 or {}, got {}",
                TYPE_MAX + 1,
                len
            ));
        }

        if len > 0 {
            // Persisted values come from a previous dump; jitter each so a
            // restart does not expose a fixed id stride.
            let mut rng = rand::rng();
            for i in 0..len {
                unsafe {
                    ffi::lua_rawgeti(state.as_ptr(), 3, (i + 1) as ffi::lua_Integer);
                    let stored: i64 = lua
                        .get(-1)
                        .map_err(|err| format!("uuid.init: sequence value {i}: {err}"))?;
                    ffi::lua_pop(state.as_ptr(), 1);
                    let jitter =
                        rng.random_range(SEQUENCE_INIT_JITTER_MIN..=SEQUENCE_INIT_JITTER_MAX);
                    // Clamp to the valid range for this type. Index 0 is the
                    // player-UID counter, which is capped at UID_SEQUENCE_MAX
                    // (24 bits) rather than the 37-bit UUID cap.
                    let cap = if i == 0 {
                        UID_SEQUENCE_MAX
                    } else {
                        SEQUENCE_MAX
                    };
                    let start = stored.saturating_add(jitter).min(cap);
                    UUID_STATE.sequence[i as usize].store(start, Ordering::Relaxed);
                }
            }
        } else {
            // Empty table: first run, seed every counter at 1.
            for slot in UUID_STATE.sequence.iter() {
                slot.store(1, Ordering::Relaxed);
            }
        }
    } else {
        // No persisted table: first run, seed every counter at 1.
        for slot in UUID_STATE.sequence.iter() {
            slot.store(1, Ordering::Relaxed);
        }
    }

    // Publish last: `channel` before `serverid`, and `serverid` with Release so
    // it pairs with the Acquire load in `next` (the init-gate). Once `next` sees
    // a non-zero serverid, all counter/channel stores above are visible.
    UUID_STATE.channel.store(channel, Ordering::Relaxed);
    UUID_STATE.serverid.store(serverid, Ordering::Release);

    Ok(0)
}

/// Atomically hand out the next value from `slot`, refusing once `max` has
/// already been issued. Unlike a bare `fetch_add`, the counter never advances
/// past `max + 1`, so a saturated type cannot grow the counter unbounded (and
/// cannot eventually wrap the `i64`). Returns `None` when exhausted.
fn issue_next(slot: &AtomicI64, max: i64) -> Option<i64> {
    let mut current = slot.load(Ordering::Relaxed);
    loop {
        if current > max {
            return None;
        }
        match slot.compare_exchange_weak(current, current + 1, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return Some(current),
            Err(actual) => current = actual,
        }
    }
}

fn lnext(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    // `serverid` is published last (Release) by `init`; loading it Acquire here
    // means that once we observe a non-zero serverid, every counter/channel
    // store from `init` is guaranteed visible even across threads. Callers must
    // still ensure `init` runs before any actor calls `next`.
    let serverid = UUID_STATE.serverid.load(Ordering::Acquire);
    let channel = UUID_STATE.channel.load(Ordering::Relaxed);
    if serverid == 0 || channel == 0 {
        return Err("uuid.next: not init".to_string());
    }

    let typ = unsafe { ffi::luaL_optinteger(state.as_ptr(), 1, 0) } as i64;
    if !(0..=TYPE_MAX).contains(&typ) {
        return Err(format!("uuid.next: type out of limit (0..={})", TYPE_MAX));
    }

    if typ == 0 {
        // Player UID: channel(8) | serverid(16) | sequence(24).
        let Some(sequence) = issue_next(&UUID_STATE.sequence[0], UID_SEQUENCE_MAX) else {
            return Err("uuid.next: player uid sequence out of limit".to_string());
        };
        let v =
            (channel << UID_CHANNEL_LEFT_SHIFT) | (serverid << UID_SERVERID_LEFT_SHIFT) | sequence;
        laux::lua_push(state, v as ffi::lua_Integer);
        Ok(1)
    } else {
        // UUID: type(10) | serverid(16) | sequence(37).
        // An explicit sequence may be supplied as arg #2; otherwise advance the
        // counter. (Explicit ids bypass the counter, but still range-checked.)
        let sequence = if unsafe { ffi::lua_type(state.as_ptr(), 2) } == ffi::LUA_TNUMBER {
            let explicit = (unsafe { ffi::luaL_optinteger(state.as_ptr(), 2, 0) }) as i64;
            // Explicit sequences may be negative or oversized; a negative value
            // would OR high bits into the result and corrupt the type/serverid
            // fields (or set the sign bit), so range-check both ends.
            if !(0..=SEQUENCE_MAX).contains(&explicit) {
                return Err("uuid.next: sequence out of limit".to_string());
            }
            explicit
        } else {
            let Some(seq) = issue_next(&UUID_STATE.sequence[typ as usize], SEQUENCE_MAX) else {
                return Err("uuid.next: sequence out of limit".to_string());
            };
            seq
        };

        let v = (typ << TYPE_LEFT_SHIFT) | (serverid << SERVERID_LEFT_SHIFT) | sequence;
        laux::lua_push(state, v as ffi::lua_Integer);
        Ok(1)
    }
}

fn ltype(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let uuid: i64 = lua.get(1).map_err(|err| format!("uuid.type: {err}"))?;
    // `(uuid >> TYPE_LEFT_SHIFT) == 0` means it is a player UID, not a UUID.
    if (uuid >> TYPE_LEFT_SHIFT) == 0 {
        return Err("uuid.type: attempt to get type from a non-uuid value".to_string());
    }
    laux::lua_push(
        state,
        ((uuid >> TYPE_LEFT_SHIFT) & TYPE_MAX) as ffi::lua_Integer,
    );
    Ok(1)
}

fn lis_uid(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let uuid: i64 = lua.get(1).map_err(|err| format!("uuid.is_uid: {err}"))?;
    // A player UID has type == 0 (high bits clear) and every field populated.
    let is_uid = (uuid >> TYPE_LEFT_SHIFT) == 0
        && (uuid & UID_SEQUENCE_MAX) != 0
        && ((uuid >> UID_SERVERID_LEFT_SHIFT) & UID_SERVERID_MAX) != 0
        && ((uuid >> UID_CHANNEL_LEFT_SHIFT) & UID_CHANNEL_MAX) != 0;
    laux::lua_push(state, is_uid);
    Ok(1)
}

fn lserverid(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let uuid: i64 = lua.get(1).map_err(|err| format!("uuid.serverid: {err}"))?;
    let serverid = if (uuid >> TYPE_LEFT_SHIFT) == 0 {
        // Player UID.
        (uuid >> UID_SERVERID_LEFT_SHIFT) & UID_SERVERID_MAX
    } else {
        // UUID.
        (uuid >> SERVERID_LEFT_SHIFT) & SERVERID_MAX
    };
    laux::lua_push(state, serverid as ffi::lua_Integer);
    Ok(1)
}

/// `split(value)` → `type, serverid, sequence, channel`.
///
/// Decodes an id into its component fields, dispatching on the kind. The first
/// three returns are always meaningful; `channel` is the player-UID channel for
/// a player UID (`type == 0`) and `0` for a UUID (which has no channel field).
fn lsplit(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let value: i64 = lua.get(1).map_err(|err| format!("uuid.split: {err}"))?;
    let (typ, serverid, sequence, channel) = if (value >> TYPE_LEFT_SHIFT) == 0 {
        // Player UID: channel(8) | serverid(16) | sequence(24).
        (
            0,
            (value >> UID_SERVERID_LEFT_SHIFT) & UID_SERVERID_MAX,
            value & UID_SEQUENCE_MAX,
            (value >> UID_CHANNEL_LEFT_SHIFT) & UID_CHANNEL_MAX,
        )
    } else {
        // UUID: type(10) | serverid(16) | sequence(37).
        (
            (value >> TYPE_LEFT_SHIFT) & TYPE_MAX,
            (value >> SERVERID_LEFT_SHIFT) & SERVERID_MAX,
            value & SEQUENCE_MAX,
            0,
        )
    };
    laux::lua_push(state, typ as ffi::lua_Integer);
    laux::lua_push(state, serverid as ffi::lua_Integer);
    laux::lua_push(state, sequence as ffi::lua_Integer);
    laux::lua_push(state, channel as ffi::lua_Integer);
    Ok(4)
}

/// `dump(periodic)`.
///
/// Returns the per-type sequence table (indices 1..=TYPE_MAX+1, i.e. type 0
/// first), the maximum fill percentage across all types, and the current
/// `serverid`.
///
/// When `periodic` is truthy, each reported sequence is `counter +
/// SEQUENCE_SAVE_MARGIN` (clamped) so a crash after the save cannot reissue ids.
/// When falsy, the real counter is reported (use on clean shutdown).
fn ldump(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    let periodic = unsafe { ffi::lua_toboolean(state.as_ptr(), 1) } != 0;

    let table = laux::LuaTable::new(state, (TYPE_MAX + 1) as usize, 0);
    let mut max_percent = 0.0f64;
    for i in 0..=(TYPE_MAX as usize) {
        let counter = UUID_STATE.sequence[i].load(Ordering::Relaxed);
        // type 0 is the player-UID counter (24-bit space, smaller margin); all
        // other types are UUIDs (37-bit space). Both the margin and the clamp
        // must respect the per-type cap so a periodic save can never report a
        // value past the field width.
        let (cap, margin) = if i == 0 {
            (UID_SEQUENCE_MAX, UID_SEQUENCE_SAVE_MARGIN)
        } else {
            (SEQUENCE_MAX, SEQUENCE_SAVE_MARGIN)
        };
        let reported = if periodic {
            counter.saturating_add(margin).min(cap)
        } else {
            counter
        };
        let percent = reported as f64 / cap as f64;
        if percent > max_percent {
            max_percent = percent;
        }
        laux::lua_push(state, reported as ffi::lua_Integer);
        table.rawseti(i + 1);
    }

    laux::lua_push(state, max_percent);
    laux::lua_push(
        state,
        UUID_STATE.serverid.load(Ordering::Relaxed) as ffi::lua_Integer,
    );
    3
}

pub unsafe extern "C-unwind" fn luaopen_uuid(state: LuaState) -> c_int {
    let l = [
        lreg_try!("init", linit),
        lreg_try!("next", lnext),
        lreg_try!("type", ltype),
        lreg_try!("is_uid", lis_uid),
        lreg_try!("serverid", lserverid),
        lreg_try!("split", lsplit),
        lreg!("dump", ldump),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}
