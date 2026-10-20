//! Protobuf encode/decode native module.
//!
//! A Rust port of moon's `lua_protobuf.cpp`. It loads a serialized
//! `FileDescriptorSet` (the binary produced by `protoc --descriptor_set_out`)
//! into a process-wide descriptor table, then encodes Lua tables to protobuf
//! wire bytes and decodes wire bytes back into Lua tables.
//!
//! Registered as the `protobuf` module (`require("protobuf")`), exposing
//! `load`, `encode`, `decode`, `messages`, `fields`, and `enums`.
//!
//! Like the C++ original this module is synchronous and does not touch the IO
//! runtime, so it follows the simpler `lua_thrift2.rs` shape rather than the
//! async session/pool pattern.

use std::collections::HashMap;
use std::ffi::CString;
use std::ffi::c_int;
use std::sync::atomic::{AtomicPtr, Ordering};

use moon_base::laux::{LuaStack, LuaType};
use moon_base::{cstr, ffi, laux, laux::LuaState, lreg_null, lreg_try, luaL_newlib};
use moon_runtime::buffer::Buffer;

const MAX_RECURSION_DEPTH: usize = 128;
/// One byte is reserved up-front for the length prefix of a nested message /
/// packed block; if the real length needs more than one byte the body is
/// shifted to make room (see [`write_len_prefixed`]).
const MIN_VARINT_LENGTH: usize = 1;

/// A base-128 varint encodes at most 64 bits, i.e. ceil(64 / 7) = 10 bytes.
/// Used both to size the spare capacity reserved before writing a varint and to
/// gate the bounds-check-free fast path when decoding.
const MAX_VARINT_LENGTH: usize = 10;

// =========================================================================
// Fast hashing for internal descriptor tables
// =========================================================================
//
// The descriptor lookup tables (`fields_by_name`, `fields_by_number`,
// `messages`, `enums`) are keyed by short, trusted strings/integers and are
// hit on the encode/decode hot path (e.g. `find_field_by_name` per Lua table
// key). std's default SipHash is overkill there, so we use the crate-wide
// FxHash (`crate::hash`) — the same fast, non-cryptographic hash rustc uses
// internally. These keys are never attacker-controlled in a way that matters
// (the descriptor is loaded from a trusted `.proto` build artifact), so
// collision-DoS resistance is unneeded.
use crate::hash::{FxBuildHasher, FxHashMap, FxHashSet};

// =========================================================================
// Wire / field type enums (values match descriptor.proto)
// =========================================================================

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WireType {
    Varint = 0,
    Fixed64 = 1,
    LengthDelimited = 2,
    Fixed32 = 5,
    Unknown = 99,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(dead_code)]
enum FieldType {
    None = 0,
    Double = 1,
    Float = 2,
    Int64 = 3,
    Uint64 = 4,
    Int32 = 5,
    Fixed64 = 6,
    Fixed32 = 7,
    Bool = 8,
    String = 9,
    Group = 10,
    Message = 11,
    Bytes = 12,
    Uint32 = 13,
    Enum = 14,
    Sfixed32 = 15,
    Sfixed64 = 16,
    Sint32 = 17,
    Sint64 = 18,
}

impl FieldType {
    fn from_i32(v: i32) -> FieldType {
        match v {
            1 => FieldType::Double,
            2 => FieldType::Float,
            3 => FieldType::Int64,
            4 => FieldType::Uint64,
            5 => FieldType::Int32,
            6 => FieldType::Fixed64,
            7 => FieldType::Fixed32,
            8 => FieldType::Bool,
            9 => FieldType::String,
            10 => FieldType::Group,
            11 => FieldType::Message,
            12 => FieldType::Bytes,
            13 => FieldType::Uint32,
            14 => FieldType::Enum,
            15 => FieldType::Sfixed32,
            16 => FieldType::Sfixed64,
            17 => FieldType::Sint32,
            18 => FieldType::Sint64,
            _ => FieldType::None,
        }
    }

    fn wire_type(self) -> WireType {
        match self {
            FieldType::Double | FieldType::Fixed64 | FieldType::Sfixed64 => WireType::Fixed64,
            FieldType::Float | FieldType::Fixed32 | FieldType::Sfixed32 => WireType::Fixed32,
            FieldType::Int64
            | FieldType::Uint64
            | FieldType::Int32
            | FieldType::Bool
            | FieldType::Uint32
            | FieldType::Enum
            | FieldType::Sint32
            | FieldType::Sint64 => WireType::Varint,
            FieldType::String | FieldType::Group | FieldType::Message | FieldType::Bytes => {
                WireType::LengthDelimited
            }
            FieldType::None => WireType::Unknown,
        }
    }

    /// Packable means the wire type is not length-delimited (numeric/enum/bool).
    fn is_packable(self) -> bool {
        !matches!(
            self,
            FieldType::None
                | FieldType::String
                | FieldType::Group
                | FieldType::Message
                | FieldType::Bytes
        )
    }

    fn is_allowed_map_key(self) -> bool {
        matches!(
            self,
            FieldType::String
                | FieldType::Int32
                | FieldType::Int64
                | FieldType::Uint32
                | FieldType::Uint64
                | FieldType::Sint32
                | FieldType::Sint64
                | FieldType::Fixed32
                | FieldType::Fixed64
                | FieldType::Sfixed32
                | FieldType::Sfixed64
        )
    }
}

fn wire_type_from_u8(v: u8) -> WireType {
    match v & 0x07 {
        0 => WireType::Varint,
        1 => WireType::Fixed64,
        2 => WireType::LengthDelimited,
        5 => WireType::Fixed32,
        _ => WireType::Unknown,
    }
}

const fn pb_tag(field_number: u32, wire_type: u32) -> u32 {
    (field_number << 3) | (wire_type & 7)
}

const fn pb_tag_ft(field_number: u32, wire_type: WireType) -> u32 {
    pb_tag(field_number, wire_type as u32)
}

// =========================================================================
// Descriptor types
// =========================================================================

struct PbField {
    packed: bool,
    /// Whether the `packed` field option was present in the descriptor. When
    /// set, the explicit value wins; otherwise proto3 repeated scalars default
    /// to packed.
    packed_set: bool,
    type_: FieldType,
    wtype: WireType,
    oneof_index: i32,
    number: i32,
    label: i32,
    /// Resolved index into `PbDescriptor::all_messages` for message-typed
    /// fields (also used for synthetic map-entry messages). `None` for scalars
    /// and enums.
    message: Option<usize>,
    /// Cached `true` when this field's resolved message is a synthetic
    /// map-entry message. Computed once in [`Loader::finalize`] so the hot
    /// path avoids re-resolving through `all_messages`.
    is_map: bool,
    name: String,
    name_c: CString,
    type_name: String,
}

impl PbField {
    fn is_repeated(&self) -> bool {
        self.label == 3
    }
}

struct PbMessage {
    is_map: bool,
    name: String,
    full_name: String,
    all_fields: Vec<PbField>,
    oneof_decl: Vec<CString>,
    fast_fields: [Option<usize>; 32],
    fields_by_number: FxHashMap<i32, usize>,
    fields_by_name: FxHashMap<Vec<u8>, usize>,
}

impl PbMessage {
    fn init(&mut self) {
        for (idx, field) in self.all_fields.iter().enumerate() {
            self.fields_by_number.entry(field.number).or_insert(idx);
            self.fields_by_name
                .entry(field.name.as_bytes().to_vec())
                .or_insert(idx);
            if field.number >= 0 && (field.number as usize) < self.fast_fields.len() {
                self.fast_fields[field.number as usize] = Some(idx);
            }
        }
    }

    fn find_field_by_number(&self, num: i32) -> Option<&PbField> {
        let idx = if num >= 0 && (num as usize) < self.fast_fields.len() {
            self.fast_fields[num as usize]
        } else {
            self.fields_by_number.get(&num).copied()
        };
        idx.map(|i| &self.all_fields[i])
    }

    fn find_field_by_tag(&self, tag: u32) -> Option<&PbField> {
        self.find_field_by_number((tag >> 3) as i32)
    }

    fn find_field_by_name(&self, name: &[u8]) -> Option<&PbField> {
        self.fields_by_name.get(name).map(|&i| &self.all_fields[i])
    }
}

struct PbDescriptor {
    ignore_empty: bool,
    all_messages: Vec<PbMessage>,
    enums: FxHashSet<String>,
    messages: FxHashMap<String, usize>,
}

impl PbDescriptor {
    fn find_message(&self, name: &str) -> Option<&PbMessage> {
        self.messages.get(name).map(|&i| &self.all_messages[i])
    }

    fn message_at(&self, idx: usize) -> &PbMessage {
        &self.all_messages[idx]
    }
}

static GLOBAL_DESCRIPTOR: AtomicPtr<PbDescriptor> = AtomicPtr::new(std::ptr::null_mut());

fn get_global_descriptor() -> Option<&'static PbDescriptor> {
    let ptr = GLOBAL_DESCRIPTOR.load(Ordering::Acquire);
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { &*ptr })
    }
}

fn set_global_descriptor(desc: Box<PbDescriptor>) {
    let ptr = Box::into_raw(desc);
    // Intentionally leak any previous descriptor instead of freeing it.
    //
    // `get_global_descriptor` hands out `&'static PbDescriptor` borrowed from
    // this pointer, and `pb_encode` / `pb_decode` running on other actor
    // threads may still be holding such a reference when a concurrent
    // `pb.load` swaps in a new descriptor. Dropping the old box here would
    // turn those live references into dangling pointers (use-after-free), and
    // would make the `'static` lifetime we vend a lie. Leaking keeps every
    // descriptor ever published alive for the rest of the process, so readers
    // that loaded either the old or the new pointer always observe valid
    // memory. `pb.load` is effectively a one-time startup operation, so the
    // leak is bounded in practice. (This mirrors the upstream C++ binding,
    // which calls `release()` on the previous descriptor for the same reason.)
    let _leaked_old = GLOBAL_DESCRIPTOR.swap(ptr, Ordering::AcqRel);
}

// =========================================================================
// Encoding helpers
// =========================================================================

fn encode_sint(val: i64) -> u64 {
    ((val as u64) << 1) ^ ((val >> 63) as u64)
}

fn decode_sint(val: u64) -> i64 {
    ((val >> 1) as i64) ^ -((val & 1) as i64)
}

fn write_varint(buf: &mut Buffer, mut val: u64) {
    // Reserve the maximum varint footprint once, then write each byte straight
    // into the spare capacity. `unsafe_write` skips the per-byte capacity check
    // that `Buffer::write` (a `Vec::push`) performs, which matters on this hot
    // path. A varint is at most `MAX_VARINT_LENGTH` bytes, so the reservation
    // covers every iteration.
    buf.prepare(MAX_VARINT_LENGTH);
    // SAFETY: the reservation above covers the maximum encoded length.
    unsafe {
        while val >= 0x80 {
            buf.unsafe_write(0x80 | (val & 0x7f) as u8);
            val >>= 7;
        }
        buf.unsafe_write(val as u8);
    }
}

fn write_varint_to_slice(out: &mut [u8], mut val: u64) -> usize {
    let mut i = 0;
    while val >= 0x80 {
        out[i] = 0x80 | (val & 0x7f) as u8;
        val >>= 7;
        i += 1;
    }
    out[i] = val as u8;
    i + 1
}

fn write_wire_type(buf: &mut Buffer, field_num: i32, wtype: WireType) {
    write_varint(buf, pb_tag(field_num as u32, wtype as u32) as u64);
}

fn write_string(buf: &mut Buffer, data: &[u8]) {
    write_varint(buf, data.len() as u64);
    buf.write_slice(data);
}

/// Reserve [`MIN_VARINT_LENGTH`] spare bytes for a length prefix and return the
/// write position where the (still unwritten) body begins. The body is written
/// after this call; [`buffer_revert_varint_space`] then rewinds the logical
/// length so [`write_len_prefixed`] can backfill the real prefix.
fn buffer_reserve_varint_space(buf: &mut Buffer) -> usize {
    buf.prepare(64);
    buf.write(0);
    buf.write_pos()
}

/// Returns the byte length of the body written since `origin_write_pos`, and
/// rewinds the buffer past both the body and the reserved prefix byte. The body
/// bytes remain in spare capacity for [`write_len_prefixed`] to re-commit.
fn buffer_revert_varint_space(buf: &mut Buffer, origin_write_pos: usize) -> usize {
    let data_len = buf.write_pos() - origin_write_pos;
    buf.revert(data_len + MIN_VARINT_LENGTH);
    data_len
}

/// Backfill a length-delimited prefix in front of a body produced by
/// [`buffer_reserve_varint_space`] / [`buffer_revert_varint_space`].
///
/// Precondition: `buf.write_pos()` sits at the reserved prefix byte and the
/// body of `data_len` bytes lives in spare capacity at `write_pos +
/// MIN_VARINT_LENGTH`.
fn write_len_prefixed(buf: &mut Buffer, data_len: usize) {
    if data_len < 0x80 {
        // Single-byte prefix fits the reserved space exactly.
        buf.write(data_len as u8);
        // SAFETY: this body was initialized before rewinding its length.
        let _ = unsafe { buf.commit(data_len) };
    } else {
        let mut tmp = [0u8; 16];
        let header_size = write_varint_to_slice(&mut tmp, data_len as u64);
        let base = buf.write_pos();
        // Re-include reserved byte + body so the realloc in `prepare` preserves
        // the body bytes, then open `header_size - MIN` extra bytes and shift.
        // SAFETY: both the placeholder and body were initialized before rewinding.
        let _ = unsafe { buf.commit(data_len + MIN_VARINT_LENGTH) };
        buf.prepare(header_size);
        buf.shift_data(base + MIN_VARINT_LENGTH, data_len, base + header_size);
        buf.data_mut_at(base, header_size)
            .copy_from_slice(&tmp[..header_size]);
        // SAFETY: shift_data initialized the extended tail.
        let _ = unsafe { buf.commit(header_size - MIN_VARINT_LENGTH) };
    }
}

// =========================================================================
// StreamReader for decoding / descriptor parsing
// =========================================================================

struct StreamReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> StreamReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    fn read_byte(&mut self) -> Result<u8, String> {
        if self.pos >= self.data.len() {
            return Err("unexpected end of stream".into());
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_varint(&mut self) -> Result<u64, String> {
        let data = self.data;
        let begin = self.pos;
        // Fast path: with at least `MAX_VARINT_LENGTH` bytes left, a complete
        // varint (<= 10 bytes) cannot run past the buffer, so we read straight
        // out of the slice without a per-byte bounds check. The loop runs at
        // most `MAX_VARINT_LENGTH` times, so every `get_unchecked` index stays
        // within `begin..begin + MAX_VARINT_LENGTH <= data.len()`.
        if data.len() - begin >= MAX_VARINT_LENGTH {
            let mut result: u64 = 0;
            for i in 0..MAX_VARINT_LENGTH {
                let byte = unsafe { *data.get_unchecked(begin + i) };
                result |= ((byte & 0x7f) as u64) << (i * 7);
                if byte & 0x80 == 0 {
                    self.pos = begin + i + 1;
                    return Ok(result);
                }
            }
            return Err("invalid varint value: too many bytes".into());
        }

        self.read_varint_slow()
    }

    /// Slow, fully bounds-checked varint read (byte by byte). Used near the end
    /// of the buffer where the fast path's 10-byte headroom is unavailable.
    #[inline]
    fn read_varint_slow(&mut self) -> Result<u64, String> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        loop {
            if shift >= 64 {
                return Err("invalid varint value: too many bytes".into());
            }
            let byte = self.read_byte()?;
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
    }

    fn read_fixed(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.remaining() < n {
            return Err("read_fixed: buffer length not enough".into());
        }
        let start = self.pos;
        self.pos += n;
        Ok(&self.data[start..start + n])
    }

    fn read_u32(&mut self) -> Result<u32, String> {
        let b = self.read_fixed(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, String> {
        let b = self.read_fixed(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn read_string(&mut self) -> Result<&'a [u8], String> {
        let len = self.read_varint()? as usize;
        if len > self.remaining() {
            return Err(format!(
                "read_string: need {} bytes, have {}",
                len,
                self.remaining()
            ));
        }
        let start = self.pos;
        self.pos += len;
        Ok(&self.data[start..start + len])
    }

    /// Read a length-delimited block and return a sub-reader over it.
    fn read_len_prefixed(&mut self) -> Result<StreamReader<'a>, String> {
        let s = self.read_string()?;
        Ok(StreamReader::new(s))
    }
}

fn skip_field(stream: &mut StreamReader, tag: u32) -> Result<(), String> {
    match wire_type_from_u8((tag & 0x07) as u8) {
        WireType::Varint => {
            stream.read_varint()?;
        }
        WireType::Fixed64 => {
            stream.read_fixed(8)?;
        }
        WireType::Fixed32 => {
            stream.read_fixed(4)?;
        }
        WireType::LengthDelimited => {
            stream.read_string()?;
        }
        WireType::Unknown => {
            return Err(format!("skip_field: invalid wire type {}", tag & 0x07));
        }
    }
    Ok(())
}

// =========================================================================
// Stack helpers
// =========================================================================

fn abs_index(state: LuaState, idx: i32) -> i32 {
    if idx >= 0 || idx <= ffi::LUA_REGISTRYINDEX {
        idx
    } else {
        unsafe { ffi::lua_gettop(state.as_ptr()) + idx + 1 }
    }
}

// =========================================================================
// Protocol (encode / decode)
// =========================================================================

struct Protobuf {
    descriptor: &'static PbDescriptor,
}

impl Protobuf {
    fn new() -> Option<Self> {
        get_global_descriptor().map(|d| Self { descriptor: d })
    }

    fn get_message<'b>(&'b self, field: &PbField) -> Option<&'b PbMessage> {
        field.message.map(|i| self.descriptor.message_at(i))
    }

    // ----- Decode (each call pushes exactly one value onto the Lua stack) -----

    fn fill_message(&self, state: LuaState, msg: &PbMessage) {
        unsafe {
            ffi::lua_createtable(state.as_ptr(), 0, msg.all_fields.len() as c_int);

            for field in &msg.all_fields {
                // Repeated/map fields default to an (empty) table that decode
                // appends into.
                if field.is_repeated() || field.is_map {
                    ffi::lua_createtable(state.as_ptr(), 0, 0);
                    ffi::lua_setfield(state.as_ptr(), -2, field.name_c.as_ptr());
                    continue;
                }

                // Singular fields are pre-populated with their proto3 default
                // value directly in the result table (no `__index` metatable);
                // decode overwrites any field actually present on the wire.
                // Message-typed fields have no scalar default, so they are left
                // absent (read back as `nil`).
                match field.type_ {
                    FieldType::Message => continue,
                    FieldType::Bool => ffi::lua_pushboolean(state.as_ptr(), 0),
                    FieldType::Double | FieldType::Float => {
                        ffi::lua_pushnumber(state.as_ptr(), 0.0)
                    }
                    FieldType::String | FieldType::Bytes => {
                        ffi::lua_pushlstring(state.as_ptr(), b"".as_ptr() as _, 0);
                    }
                    _ => ffi::lua_pushinteger(state.as_ptr(), 0),
                }
                ffi::lua_setfield(state.as_ptr(), -2, field.name_c.as_ptr());
            }
        }
    }

    fn decode_field(
        &self,
        state: LuaState,
        stream: &mut StreamReader,
        field: &PbField,
        depth: usize,
    ) -> Result<(), String> {
        match field.type_ {
            FieldType::Float => {
                let v = f32::from_bits(stream.read_u32()?);
                unsafe { ffi::lua_pushnumber(state.as_ptr(), v as f64) };
            }
            FieldType::Double => {
                let v = f64::from_bits(stream.read_u64()?);
                unsafe { ffi::lua_pushnumber(state.as_ptr(), v) };
            }
            FieldType::Fixed32 => {
                let v = stream.read_u32()?;
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer) };
            }
            FieldType::Fixed64 => {
                let v = stream.read_u64()?;
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer) };
            }
            FieldType::Bool => {
                let v = stream.read_varint()?;
                unsafe { ffi::lua_pushboolean(state.as_ptr(), (v != 0) as c_int) };
            }
            FieldType::Int32 => {
                let v = stream.read_varint()? as u32 as i32;
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer) };
            }
            FieldType::Int64 => {
                let v = stream.read_varint()? as i64;
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer) };
            }
            FieldType::Uint32 => {
                let v = stream.read_varint()? as u32;
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer) };
            }
            FieldType::Uint64 => {
                let v = stream.read_varint()?;
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer) };
            }
            FieldType::Sint32 => {
                let v = decode_sint(stream.read_varint()? as u32 as u64);
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as i32 as ffi::lua_Integer) };
            }
            FieldType::Sint64 => {
                let v = decode_sint(stream.read_varint()?);
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer) };
            }
            FieldType::Sfixed32 => {
                let v = stream.read_u32()? as i32;
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer) };
            }
            FieldType::Sfixed64 => {
                let v = stream.read_u64()? as i64;
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer) };
            }
            FieldType::Enum => {
                let v = stream.read_varint()? as u32;
                unsafe { ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer) };
            }
            FieldType::Message => {
                let msg = self.get_message(field).ok_or_else(|| {
                    format!("decode: missing message type for field '{}'", field.name)
                })?;
                let mut sub = stream.read_len_prefixed()?;
                self.decode_message(state, &mut sub, msg, depth + 1)?;
            }
            FieldType::Bytes | FieldType::String => {
                let s = stream.read_string()?;
                unsafe { ffi::lua_pushlstring(state.as_ptr(), s.as_ptr() as _, s.len()) };
            }
            FieldType::None | FieldType::Group => {
                return Err(format!(
                    "decode: invalid field type for field '{}'",
                    field.name
                ));
            }
        }
        Ok(())
    }

    fn decode_map(
        &self,
        state: LuaState,
        stream: &mut StreamReader,
        field: &PbField,
        table_abs: i32,
        depth: usize,
    ) -> Result<(), String> {
        let msg = self
            .get_message(field)
            .ok_or_else(|| format!("decode_map: no message type for field '{}'", field.name))?;

        // Push the pre-created map table (created by fill_message).
        laux::lua_push(state, field.name.as_str());
        unsafe { ffi::lua_rawget(state.as_ptr(), table_abs) };
        let map_abs = abs_index(state, -1);

        let mut sub = stream.read_len_prefixed()?;
        let mut has_key = false;
        while !sub.is_empty() {
            let tag = sub.read_varint()? as u32;
            let kvfield = match msg.find_field_by_tag(tag) {
                Some(f) => f,
                None => {
                    skip_field(&mut sub, tag)?;
                    continue;
                }
            };
            self.decode_field(state, &mut sub, kvfield, depth)?;
            if kvfield.number == 1 {
                if has_key {
                    // Duplicate key inside one map entry (possible with a
                    // truncated or malicious stream). A map entry is just a
                    // message, so the last key wins: drop the previously-pushed
                    // key sitting just below the new one. This keeps at most one
                    // key pending on the stack so the trailing `lua_pop`s below
                    // stay balanced instead of leaking a value into the caller.
                    unsafe { ffi::lua_remove(state.as_ptr(), -2) };
                }
                has_key = true;
            } else if kvfield.number == 2 && has_key {
                unsafe { ffi::lua_rawset(state.as_ptr(), map_abs) };
                has_key = false;
            } else {
                unsafe { ffi::lua_pop(state.as_ptr(), 1) };
            }
        }
        if has_key {
            unsafe { ffi::lua_pop(state.as_ptr(), 1) };
        }

        unsafe { ffi::lua_pop(state.as_ptr(), 1) }; // pop the map table
        Ok(())
    }

    fn decode_message(
        &self,
        state: LuaState,
        stream: &mut StreamReader,
        msg: &PbMessage,
        depth: usize,
    ) -> Result<(), String> {
        if depth >= MAX_RECURSION_DEPTH {
            return Err(format!(
                "decode: maximum recursion depth exceeded at '{}'",
                msg.full_name
            ));
        }

        laux::lua_checkstack(state, 8, std::ptr::null())?;
        let stack_base = unsafe { ffi::lua_gettop(state.as_ptr()) };
        self.fill_message(state, msg);
        let table_abs = stack_base + 1;

        // Cache for the most-recently-appended non-map repeated field. protobuf
        // encoders emit a repeated field's elements contiguously, so we keep its
        // list table on the Lua stack and track the running length locally,
        // avoiding a `lua_rawget` + `lua_rawlen` per element. `cached_num ==
        // -1` means no list is currently held. Correctness for interleaved
        // fields is preserved: switching fields pops the cached table and the
        // next access re-reads the true length via `lua_rawlen`.
        let mut cached_num: i32 = -1;
        let mut cached_list_abs: i32 = 0;
        let mut cached_len: ffi::lua_Integer = 0;

        macro_rules! drop_cached_list {
            () => {
                if cached_num != -1 {
                    unsafe { ffi::lua_pop(state.as_ptr(), 1) };
                    cached_num = -1;
                }
            };
        }

        while !stream.is_empty() {
            let tag = stream.read_varint()? as u32;
            let wire_type = wire_type_from_u8((tag & 0x07) as u8);
            if (tag >> 3) == 0 {
                return Err(format!(
                    "decode: invalid tag (field_number=0) in '{}'",
                    msg.full_name
                ));
            }
            let field = match msg.find_field_by_tag(tag) {
                Some(f) => f,
                None => {
                    skip_field(stream, tag)?;
                    continue;
                }
            };

            if field.is_map {
                drop_cached_list!();
                if wire_type != WireType::LengthDelimited {
                    return Err(format!(
                        "decode: invalid wire type for map field '{}.{}'",
                        msg.full_name, field.name
                    ));
                }
                self.decode_map(state, stream, field, table_abs, depth + 1)?;
                continue;
            }

            if field.is_repeated() {
                // Make sure this field's list table is the one cached on top.
                if cached_num != field.number {
                    if cached_num != -1 {
                        unsafe { ffi::lua_pop(state.as_ptr(), 1) };
                    }
                    laux::lua_push(state, field.name.as_str());
                    unsafe { ffi::lua_rawget(state.as_ptr(), table_abs) };
                    cached_list_abs = abs_index(state, -1);
                    cached_len = unsafe { ffi::lua_rawlen(state.as_ptr(), cached_list_abs) }
                        as ffi::lua_Integer;
                    cached_num = field.number;
                }

                if wire_type == WireType::LengthDelimited && field.type_.is_packable() {
                    // Packed block: one length-delimited run of values.
                    let mut sub = stream.read_len_prefixed()?;
                    while !sub.is_empty() {
                        self.decode_field(state, &mut sub, field, depth)?;
                        cached_len += 1;
                        unsafe { ffi::lua_rawseti(state.as_ptr(), cached_list_abs, cached_len) };
                    }
                } else {
                    self.decode_field(state, stream, field, depth)?;
                    cached_len += 1;
                    unsafe { ffi::lua_rawseti(state.as_ptr(), cached_list_abs, cached_len) };
                }
                continue;
            }

            // Singular scalar/message field: any cached repeated run ends here.
            drop_cached_list!();

            if wire_type != field.wtype {
                return Err(format!(
                    "decode: wire type mismatch for field '{}.{}'",
                    msg.full_name, field.name
                ));
            }

            self.decode_field(state, stream, field, depth)?;
            unsafe { ffi::lua_setfield(state.as_ptr(), table_abs, field.name_c.as_ptr()) };

            // oneof bookkeeping: also record which field is set under the oneof name.
            if field.oneof_index >= 0 && (field.oneof_index as usize) < msg.oneof_decl.len() {
                unsafe {
                    ffi::lua_pushlstring(
                        state.as_ptr(),
                        field.name.as_ptr() as _,
                        field.name.len(),
                    );
                    ffi::lua_setfield(
                        state.as_ptr(),
                        table_abs,
                        msg.oneof_decl[field.oneof_index as usize].as_ptr(),
                    );
                }
            }
        }

        // Leave only the result table on top of the stack.
        if cached_num != -1 {
            unsafe { ffi::lua_pop(state.as_ptr(), 1) };
        }

        // Contract: a successful decode pushes exactly one value (the result
        // table). Guards the on-stack list caching above against leaks.
        debug_assert_eq!(
            unsafe { ffi::lua_gettop(state.as_ptr()) },
            stack_base + 1,
            "decode_message '{}' left an unbalanced Lua stack",
            msg.full_name
        );

        Ok(())
    }

    // ----- Encode (reads from Lua stack, writes to Buffer) -----

    /// Read the Lua value at `index` for `field` and write its wire-format
    /// bytes directly to `buf` (the caller has already written the field tag,
    /// if any). Returns whether the value was empty (zero / default / empty
    /// string), which the proto3 `ignore_empty` path uses to drop the field.
    ///
    /// This merges value extraction and wire encoding into a single dispatch on
    /// `field.type_`, avoiding an intermediate `PbValue` and a second match.
    /// Message-typed fields are encoded in place via the reserve/backfill trick
    /// (see [`buffer_reserve_varint_space`]).
    fn write_field_value(
        &self,
        state: LuaState,
        buf: &mut Buffer,
        field: &PbField,
        index: i32,
        depth: usize,
    ) -> Result<bool, String> {
        let to_int =
            || unsafe { ffi::lua_tointegerx(state.as_ptr(), index, std::ptr::null_mut()) } as i64;
        let to_num =
            || unsafe { ffi::lua_tonumberx(state.as_ptr(), index, std::ptr::null_mut()) } as f64;

        match field.type_ {
            FieldType::Float => {
                let n = to_num();
                buf.write_slice(&(n as f32).to_le_bytes());
                Ok(n == 0.0)
            }
            FieldType::Double => {
                let n = to_num();
                buf.write_slice(&n.to_le_bytes());
                Ok(n == 0.0)
            }
            FieldType::Fixed32 => {
                let v = to_int();
                if v < 0 || v > u32::MAX as i64 {
                    return Err(format!("encode: '{}' value out of u32 range", field.name));
                }
                buf.write_slice(&(v as u32).to_le_bytes());
                Ok(v == 0)
            }
            FieldType::Uint32 => {
                let v = to_int();
                if v < 0 || v > u32::MAX as i64 {
                    return Err(format!("encode: '{}' value out of u32 range", field.name));
                }
                write_varint(buf, v as u32 as u64);
                Ok(v == 0)
            }
            FieldType::Fixed64 => {
                let v = to_int();
                if v < 0 {
                    return Err(format!("encode: '{}' must be non-negative", field.name));
                }
                buf.write_slice(&(v as u64).to_le_bytes());
                Ok(v == 0)
            }
            FieldType::Uint64 => {
                let v = to_int();
                if v < 0 {
                    return Err(format!("encode: '{}' must be non-negative", field.name));
                }
                write_varint(buf, v as u64);
                Ok(v == 0)
            }
            FieldType::Int32 => {
                let v = to_int();
                if v < i32::MIN as i64 || v > i32::MAX as i64 {
                    return Err(format!("encode: '{}' value out of i32 range", field.name));
                }
                write_varint(buf, v as i32 as u32 as u64);
                Ok(v == 0)
            }
            FieldType::Sint32 => {
                let v = to_int();
                if v < i32::MIN as i64 || v > i32::MAX as i64 {
                    return Err(format!("encode: '{}' value out of i32 range", field.name));
                }
                write_varint(buf, encode_sint(v as i32 as i64));
                Ok(v == 0)
            }
            FieldType::Sfixed32 => {
                let v = to_int();
                if v < i32::MIN as i64 || v > i32::MAX as i64 {
                    return Err(format!("encode: '{}' value out of i32 range", field.name));
                }
                buf.write_slice(&(v as i32).to_le_bytes());
                Ok(v == 0)
            }
            FieldType::Int64 => {
                let v = to_int();
                write_varint(buf, v as u64);
                Ok(v == 0)
            }
            FieldType::Sint64 => {
                let v = to_int();
                write_varint(buf, encode_sint(v));
                Ok(v == 0)
            }
            FieldType::Sfixed64 => {
                let v = to_int();
                buf.write_slice(&v.to_le_bytes());
                Ok(v == 0)
            }
            FieldType::Enum => {
                let v = to_int();
                write_varint(buf, v as u32 as u64);
                Ok(v == 0)
            }
            FieldType::Bool => {
                let b = unsafe { ffi::lua_toboolean(state.as_ptr(), index) };
                write_varint(buf, (b != 0) as u64);
                Ok(b == 0)
            }
            FieldType::Bytes | FieldType::String => {
                if unsafe { ffi::lua_type(state.as_ptr(), index) } != ffi::LUA_TSTRING {
                    return Err(format!(
                        "encode: expected string for field '{}'",
                        field.name
                    ));
                }
                let mut len = 0usize;
                let ptr = unsafe { ffi::lua_tolstring(state.as_ptr(), index, &mut len) };
                let bytes = if len == 0 || ptr.is_null() {
                    &[][..]
                } else {
                    unsafe { std::slice::from_raw_parts(ptr as *const u8, len) }
                };
                write_string(buf, bytes);
                Ok(len == 0)
            }
            FieldType::Message => {
                let msg = self.get_message(field).ok_or_else(|| {
                    format!("encode: missing message type for field '{}'", field.name)
                })?;
                let base = buffer_reserve_varint_space(buf);
                self.encode_message(state, buf, msg, depth + 1)?;
                let data_len = buffer_revert_varint_space(buf, base);
                write_len_prefixed(buf, data_len);
                Ok(data_len == 0)
            }
            FieldType::None | FieldType::Group => Err(format!(
                "encode: unsupported field type for '{}'",
                field.name
            )),
        }
    }

    fn encode_map(
        &self,
        state: LuaState,
        buf: &mut Buffer,
        field: &PbField,
        index: i32,
        depth: usize,
    ) -> Result<(), String> {
        if unsafe { ffi::lua_type(state.as_ptr(), index) } != ffi::LUA_TTABLE {
            return Err(format!("encode_map: field '{}' expected table", field.name));
        }
        let msg = self
            .get_message(field)
            .ok_or_else(|| format!("encode_map: no message type for field '{}'", field.name))?;
        let kfield = msg
            .find_field_by_number(1)
            .ok_or("encode_map: missing key field")?;
        let vfield = msg
            .find_field_by_number(2)
            .ok_or("encode_map: missing value field")?;

        let index = abs_index(state, index);
        unsafe { ffi::lua_pushnil(state.as_ptr()) };
        while unsafe { ffi::lua_next(state.as_ptr(), index) } != 0 {
            write_wire_type(buf, field.number, field.wtype);
            let base = buffer_reserve_varint_space(buf);

            write_wire_type(buf, kfield.number, kfield.wtype);
            self.write_field_value(state, buf, kfield, -2, depth)?;

            write_wire_type(buf, vfield.number, vfield.wtype);
            self.write_field_value(state, buf, vfield, -1, depth)?;

            let data_len = buffer_revert_varint_space(buf, base);
            write_len_prefixed(buf, data_len);

            unsafe { ffi::lua_pop(state.as_ptr(), 1) };
        }
        Ok(())
    }

    fn encode_repeated(
        &self,
        state: LuaState,
        buf: &mut Buffer,
        field: &PbField,
        index: i32,
        depth: usize,
    ) -> Result<(), String> {
        if unsafe { ffi::lua_type(state.as_ptr(), index) } != ffi::LUA_TTABLE {
            return Err(format!(
                "encode_repeated: field '{}' expected table",
                field.name
            ));
        }
        let index = abs_index(state, index);
        let rawlen = unsafe { ffi::lua_rawlen(state.as_ptr(), index) } as ffi::lua_Integer;
        if rawlen == 0 && self.descriptor.ignore_empty {
            return Ok(());
        }

        if field.packed {
            write_wire_type(buf, field.number, WireType::LengthDelimited);
            let base = buffer_reserve_varint_space(buf);
            for i in 1..=rawlen {
                unsafe { ffi::lua_rawgeti(state.as_ptr(), index, i) };
                self.write_field_value(state, buf, field, -1, depth)?;
                unsafe { ffi::lua_pop(state.as_ptr(), 1) };
            }
            let data_len = buffer_revert_varint_space(buf, base);
            write_len_prefixed(buf, data_len);
        } else {
            for i in 1..=rawlen {
                unsafe { ffi::lua_rawgeti(state.as_ptr(), index, i) };
                write_wire_type(buf, field.number, field.wtype);
                self.write_field_value(state, buf, field, -1, depth)?;
                unsafe { ffi::lua_pop(state.as_ptr(), 1) };
            }
        }
        Ok(())
    }

    fn encode_message(
        &self,
        state: LuaState,
        buf: &mut Buffer,
        msg: &PbMessage,
        depth: usize,
    ) -> Result<(), String> {
        if depth >= MAX_RECURSION_DEPTH {
            return Err(format!(
                "encode: maximum recursion depth exceeded at '{}'",
                msg.full_name
            ));
        }
        laux::lua_checkstack(state, 8, std::ptr::null())?;
        let stack_base = unsafe { ffi::lua_gettop(state.as_ptr()) };

        let mut oneof_encoded = false;
        unsafe { ffi::lua_pushnil(state.as_ptr()) };
        while unsafe { ffi::lua_next(state.as_ptr(), -2) } != 0 {
            if unsafe { ffi::lua_type(state.as_ptr(), -2) } == ffi::LUA_TSTRING {
                let mut len = 0;
                let ptr = unsafe { ffi::lua_tolstring(state.as_ptr(), -2, &mut len) };
                let key = if ptr.is_null() {
                    &[][..]
                } else {
                    unsafe { std::slice::from_raw_parts(ptr as *const u8, len) }
                };
                let field = msg.find_field_by_name(key);
                if let Some(field) = field {
                    let value_abs = stack_base + 2;
                    if field.is_map {
                        self.encode_map(state, buf, field, value_abs, depth + 1)?;
                    } else if field.is_repeated() {
                        self.encode_repeated(state, buf, field, value_abs, depth)?;
                    } else {
                        // A oneof member that `ignore_empty` drops (empty
                        // string / zero / empty message) is treated as absent:
                        // it must neither count toward the oneof nor trip the
                        // multiple-set check below. Only members that are
                        // actually committed conflict, so the exclusivity flag
                        // is set after the empty-drop decision.
                        let origin_size = buf.write_pos();
                        write_wire_type(buf, field.number, field.wtype);
                        let is_empty =
                            self.write_field_value(state, buf, field, value_abs, depth)?;
                        if is_empty && self.descriptor.ignore_empty {
                            buf.revert(buf.write_pos() - origin_size);
                        } else if field.oneof_index >= 0 {
                            if oneof_encoded {
                                return Err(format!(
                                    "encode: multiple oneof fields set in '{}'",
                                    msg.full_name
                                ));
                            }
                            oneof_encoded = true;
                        }
                    }
                }
            }
            unsafe { ffi::lua_pop(state.as_ptr(), 1) };
        }

        // Contract: encoding a message is stack-neutral (the `lua_next`
        // traversal pushes and pops its own key/value pairs).
        debug_assert_eq!(
            unsafe { ffi::lua_gettop(state.as_ptr()) },
            stack_base,
            "encode_message '{}' left an unbalanced Lua stack",
            msg.full_name
        );
        Ok(())
    }
}

// =========================================================================
// Descriptor parsing (FileDescriptorSet)
// =========================================================================

struct Loader {
    syntax: String,
    all_messages: Vec<PbMessage>,
    enums: FxHashSet<String>,
}

impl Loader {
    fn new() -> Self {
        Self {
            syntax: String::new(),
            all_messages: Vec::new(),
            enums: FxHashSet::default(),
        }
    }

    fn read_field(&mut self, stream: &mut StreamReader, msg: &mut PbMessage) -> Result<(), String> {
        let mut s = stream.read_len_prefixed()?;
        let mut name = String::new();
        let mut number = -1i32;
        let mut label = -1i32;
        let mut type_i = 0i32;
        let mut type_name = String::new();
        let mut oneof_index = -1i32;
        let mut packed_explicit = false;
        let mut packed_set = false;

        while !s.is_empty() {
            let tag = s.read_varint()? as u32;
            match tag {
                t if t == pb_tag_ft(1, WireType::LengthDelimited) => {
                    name = String::from_utf8_lossy(s.read_string()?).into_owned();
                }
                t if t == pb_tag_ft(3, WireType::Varint) => {
                    number = s.read_varint()? as i32;
                }
                t if t == pb_tag_ft(4, WireType::Varint) => {
                    label = s.read_varint()? as i32;
                }
                t if t == pb_tag_ft(5, WireType::Varint) => {
                    type_i = s.read_varint()? as i32;
                }
                t if t == pb_tag_ft(6, WireType::LengthDelimited) => {
                    // Strip the leading '.' from the fully-qualified type name.
                    let raw = String::from_utf8_lossy(s.read_string()?).into_owned();
                    type_name = raw.strip_prefix('.').unwrap_or(&raw).to_string();
                }
                t if t == pb_tag_ft(8, WireType::LengthDelimited) => {
                    let mut opt = s.read_len_prefixed()?;
                    while !opt.is_empty() {
                        let otag = opt.read_varint()? as u32;
                        if otag == pb_tag_ft(2, WireType::Varint) {
                            packed_explicit = opt.read_varint()? != 0;
                            packed_set = true;
                        } else {
                            skip_field(&mut opt, otag)?;
                        }
                    }
                }
                t if t == pb_tag_ft(9, WireType::Varint) => {
                    oneof_index = s.read_varint()? as i32;
                }
                _ => skip_field(&mut s, tag)?,
            }
        }

        let type_ = FieldType::from_i32(type_i);
        let wtype = type_.wire_type();
        msg.all_fields.push(PbField {
            packed: packed_explicit,
            packed_set,
            type_,
            wtype,
            oneof_index,
            number,
            label,
            message: None,
            is_map: false,
            name_c: CString::new(name.as_str()).unwrap_or_default(),
            name,
            type_name,
        });
        Ok(())
    }

    /// Read a length-delimited `EnumDescriptorProto` entry.
    fn read_enum(&mut self, stream: &mut StreamReader, package: &str) -> Result<(), String> {
        let body = stream.read_string()?;
        let mut s = StreamReader::new(body);
        self.parse_enum_body(&mut s, package)
    }

    /// Read a length-delimited `DescriptorProto` entry.
    fn read_message(&mut self, stream: &mut StreamReader, package: &str) -> Result<(), String> {
        let body = stream.read_string()?;
        let mut s = StreamReader::new(body);
        self.parse_message_body(&mut s, package)
    }

    /// Parse a `DescriptorProto` body (already unwrapped from its length prefix).
    fn parse_message_body(&mut self, s: &mut StreamReader, package: &str) -> Result<(), String> {
        let mut name = String::new();
        let mut full_name = String::new();
        let mut is_map = false;
        let mut all_fields: Vec<PbField> = Vec::new();
        let mut oneof_decl: Vec<CString> = Vec::new();
        // Nested declarations are parsed after we know this message's full name.
        // Collect their byte ranges (offset/len within this body) for later.
        let mut nested_messages: Vec<(usize, usize)> = Vec::new();
        let mut nested_enums: Vec<(usize, usize)> = Vec::new();

        while !s.is_empty() {
            let tag = s.read_varint()? as u32;
            match tag {
                t if t == pb_tag_ft(1, WireType::LengthDelimited) => {
                    name = String::from_utf8_lossy(s.read_string()?).into_owned();
                    full_name = if package.is_empty() {
                        name.clone()
                    } else {
                        format!("{}.{}", package, name)
                    };
                }
                t if t == pb_tag_ft(2, WireType::LengthDelimited) => {
                    // field (FieldDescriptorProto)
                    let mut tmp = PbMessage {
                        is_map: false,
                        name: String::new(),
                        full_name: String::new(),
                        all_fields: std::mem::take(&mut all_fields),
                        oneof_decl: Vec::new(),
                        fast_fields: [None; 32],
                        fields_by_number: FxHashMap::default(),
                        fields_by_name: FxHashMap::default(),
                    };
                    self.read_field(s, &mut tmp)?;
                    all_fields = tmp.all_fields;
                }
                t if t == pb_tag_ft(3, WireType::LengthDelimited) => {
                    // nested_type: defer (need full_name first)
                    let blob = s.read_string()?;
                    let start = blob.as_ptr() as usize - s.data.as_ptr() as usize;
                    nested_messages.push((start, blob.len()));
                }
                t if t == pb_tag_ft(4, WireType::LengthDelimited) => {
                    // enum_type
                    let blob = s.read_string()?;
                    let start = blob.as_ptr() as usize - s.data.as_ptr() as usize;
                    nested_enums.push((start, blob.len()));
                }
                t if t == pb_tag_ft(7, WireType::LengthDelimited) => {
                    // MessageOptions: map_entry is field 7
                    let mut opt = s.read_len_prefixed()?;
                    while !opt.is_empty() {
                        let otag = opt.read_varint()? as u32;
                        if otag == pb_tag_ft(7, WireType::Varint) {
                            is_map = opt.read_varint()? != 0;
                        } else {
                            skip_field(&mut opt, otag)?;
                        }
                    }
                }
                t if t == pb_tag_ft(8, WireType::LengthDelimited) => {
                    // oneof_decl
                    let mut od = s.read_len_prefixed()?;
                    while !od.is_empty() {
                        let otag = od.read_varint()? as u32;
                        if otag == pb_tag_ft(1, WireType::LengthDelimited) {
                            let on = String::from_utf8_lossy(od.read_string()?).into_owned();
                            oneof_decl.push(CString::new(on).unwrap_or_default());
                        } else {
                            skip_field(&mut od, otag)?;
                        }
                    }
                }
                _ => skip_field(s, tag)?,
            }
        }

        self.all_messages.push(PbMessage {
            is_map,
            name,
            full_name: full_name.clone(),
            all_fields,
            oneof_decl,
            fast_fields: [None; 32],
            fields_by_number: FxHashMap::default(),
            fields_by_name: FxHashMap::default(),
        });

        // Parse nested declarations now that the parent's full name is known.
        for (off, len) in nested_messages {
            let mut sub = StreamReader::new(&s.data[off..off + len]);
            self.parse_message_body(&mut sub, &full_name)?;
        }
        for (off, len) in nested_enums {
            let mut sub = StreamReader::new(&s.data[off..off + len]);
            self.parse_enum_body(&mut sub, &full_name)?;
        }
        Ok(())
    }

    /// Parse an `EnumDescriptorProto` body (already unwrapped). Only the name is
    /// retained; values are decoded/encoded as plain integers.
    fn parse_enum_body(&mut self, s: &mut StreamReader, package: &str) -> Result<(), String> {
        let mut name = String::new();
        while !s.is_empty() {
            let tag = s.read_varint()? as u32;
            match tag {
                t if t == pb_tag_ft(1, WireType::LengthDelimited) => {
                    name = String::from_utf8_lossy(s.read_string()?).into_owned();
                }
                _ => skip_field(s, tag)?,
            }
        }
        let full = if package.is_empty() {
            name
        } else {
            format!("{}.{}", package, name)
        };
        self.enums.insert(full);
        Ok(())
    }

    fn read_file_descriptor(&mut self, stream: &mut StreamReader) -> Result<(), String> {
        let mut s = stream.read_len_prefixed()?;
        let mut package = String::new();
        while !s.is_empty() {
            let tag = s.read_varint()? as u32;
            match tag {
                t if t == pb_tag_ft(2, WireType::LengthDelimited) => {
                    package = String::from_utf8_lossy(s.read_string()?).into_owned();
                }
                t if t == pb_tag_ft(4, WireType::LengthDelimited) => {
                    let pkg = package.clone();
                    self.read_message(&mut s, &pkg)?;
                }
                t if t == pb_tag_ft(5, WireType::LengthDelimited) => {
                    let pkg = package.clone();
                    self.read_enum(&mut s, &pkg)?;
                }
                t if t == pb_tag_ft(12, WireType::LengthDelimited) => {
                    self.syntax = String::from_utf8_lossy(s.read_string()?).into_owned();
                }
                _ => skip_field(&mut s, tag)?,
            }
        }
        Ok(())
    }

    fn finalize(mut self) -> Result<PbDescriptor, String> {
        let mut messages: FxHashMap<String, usize> =
            HashMap::with_capacity_and_hasher(self.all_messages.len(), FxBuildHasher::default());
        for (idx, m) in self.all_messages.iter().enumerate() {
            messages.insert(m.full_name.clone(), idx);
        }

        let is_proto3 = self.syntax == "proto3";

        // Resolve message-typed fields and finalize the packed flag.
        for mi in 0..self.all_messages.len() {
            for fi in 0..self.all_messages[mi].all_fields.len() {
                let (type_name, type_, label, explicit_packed, packed_set) = {
                    let f = &self.all_messages[mi].all_fields[fi];
                    (
                        f.type_name.clone(),
                        f.type_,
                        f.label,
                        f.packed,
                        f.packed_set,
                    )
                };
                if !type_name.is_empty() {
                    if let Some(&idx) = messages.get(&type_name) {
                        let is_map = self.all_messages[idx].is_map;
                        let f = &mut self.all_messages[mi].all_fields[fi];
                        f.message = Some(idx);
                        f.is_map = is_map;
                    }
                }
                // An explicit `packed` option always wins; otherwise proto3
                // repeated scalar fields default to packed. Only packable wire
                // types (numeric/enum/bool) can ever be packed.
                let packed = type_.is_packable()
                    && if packed_set {
                        explicit_packed
                    } else {
                        label == 3 && is_proto3
                    };
                self.all_messages[mi].all_fields[fi].packed = packed;
            }
        }

        // Validate map entries and build per-message lookup tables.
        for mi in 0..self.all_messages.len() {
            self.all_messages[mi].init();
        }
        for mi in 0..self.all_messages.len() {
            for fi in 0..self.all_messages[mi].all_fields.len() {
                let field = &self.all_messages[mi].all_fields[fi];
                let msg_idx = field.message;
                let is_map = msg_idx
                    .map(|i| self.all_messages[i].is_map)
                    .unwrap_or(false);
                if is_map {
                    let m = &self.all_messages[msg_idx.unwrap()];
                    if m.all_fields.len() != 2 {
                        return Err(format!(
                            "invalid map entry message for field '{}'",
                            field.name
                        ));
                    }
                    let kf = m
                        .find_field_by_number(1)
                        .ok_or_else(|| format!("map entry missing key for '{}'", field.name))?;
                    if !kf.type_.is_allowed_map_key() {
                        return Err(format!("invalid map key type in field '{}'", field.name));
                    }
                }
            }
        }

        Ok(PbDescriptor {
            ignore_empty: true,
            all_messages: self.all_messages,
            enums: self.enums,
            messages,
        })
    }
}

fn do_load(data_slices: &[&[u8]]) -> Result<PbDescriptor, String> {
    let mut loader = Loader::new();
    for data in data_slices {
        let mut stream = StreamReader::new(data);
        // FileDescriptorSet: repeated FileDescriptorProto file = 1;
        while !stream.is_empty() {
            let tag = stream.read_varint()? as u32;
            if tag == pb_tag_ft(1, WireType::LengthDelimited) {
                loader.read_file_descriptor(&mut stream)?;
            } else {
                skip_field(&mut stream, tag)?;
            }
        }
    }
    loader.finalize()
}

// =========================================================================
// Thread-local encode buffer
// =========================================================================

/// Returns the reusable encoder buffer for the current Lua worker thread.
///
/// # Safety contract
/// Protobuf encoding must not be re-entered on the same thread while the
/// returned reference is live. Raw table reads avoid __index calls, but GC
/// finalizers must still not call protobuf.encode recursively.
fn get_thread_encode_buffer() -> &'static mut Buffer {
    thread_local! {
        static ENCODE_BUF: std::cell::UnsafeCell<Buffer> =
            std::cell::UnsafeCell::new(Buffer::with_capacity(64 * 1024));
    }
    ENCODE_BUF.with(|cell| unsafe { &mut *cell.get() })
}

// =========================================================================
// Lua-facing functions
// =========================================================================

fn pb_load(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let nargs = unsafe { ffi::lua_gettop(state.as_ptr()) } as usize;
    if nargs == 0 {
        return Err("protobuf.load: expected at least 1 argument (FileDescriptorSet data)".into());
    }
    // `do_load` parses synchronously and stores owned descriptor fields; it
    // never retains the input slices. Keep the Lua strings rooted until parsing
    // completes, then release the borrows before mutating the Lua stack.
    let loaded = {
        let slices: Vec<&[u8]> = (1..=nargs)
            .map(|i| {
                lua.value(i as c_int)
                    .as_bytes()
                    .ok_or_else(|| format!("protobuf.load: argument #{} must be a string", i))
            })
            .collect::<Result<_, _>>()?;
        do_load(&slices)
    };
    match loaded {
        Ok(desc) => set_global_descriptor(Box::new(desc)),
        Err(e) => return Err(format!("protobuf.load error: {}", e)),
    }
    laux::lua_push(state, true);
    Ok(1)
}

fn pb_encode(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    laux::lua_checktype(state, 2, ffi::LUA_TTABLE)
        .map_err(|err| format!("protobuf.encode: argument #2 {err}"))?;

    let pb = match Protobuf::new() {
        Some(p) => p,
        None => return Err("protobuf.encode: descriptor not loaded".into()),
    };
    let msg = {
        let cmd_name = lua
            .value(1)
            .as_str()
            .ok_or_else(|| "protobuf.encode: UTF-8 message name expected".to_string())?;
        match pb.descriptor.find_message(cmd_name) {
            Some(m) => m,
            None => return Err(format!("protobuf.encode: message '{}' not found", cmd_name)),
        }
    };

    let buf = get_thread_encode_buffer();
    buf.clear();
    // The table to encode is at index 2.
    unsafe { ffi::lua_settop(state.as_ptr(), 2) };
    if let Err(e) = pb.encode_message(state, buf, msg, 0) {
        return Err(format!("protobuf.encode error: {}", e));
    }
    laux::lua_push(state, buf.data());
    Ok(1)
}

fn pb_decode(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let value = lua.value(2);
    let data = match value.kind() {
        LuaType::LightUserData => {
            let len = lua
                .get::<i64>(3)
                .map_err(|_| "protobuf.decode: buffer length must be an integer".to_string())?;
            if len < 0 {
                return Err("protobuf.decode: buffer length must not be negative".to_string());
            }
            let ptr = value
                .as_light_userdata()
                .and_then(|ptr| std::ptr::NonNull::new(ptr.cast::<u8>()))
                .ok_or_else(|| "protobuf.decode: non-null lightuserdata expected".to_string())?;
            // SAFETY: the native buffer is owned by the Lua caller and remains
            // valid for this synchronous decode call. No slice escapes.
            unsafe { std::slice::from_raw_parts(ptr.as_ptr(), len as usize) }
        }
        LuaType::String => {
            // SAFETY: argument #2 remains rooted while decoding. `decode_message`
            // only appends/updates the new result table and never replaces the
            // source string slot, so its bytes stay valid for this synchronous call.
            let bytes = unsafe { value.as_bytes_append_only() }
                .ok_or_else(|| "protobuf.decode: string data is unavailable".to_string())?;
            bytes
        }
        _ => {
            return Err(format!(
                "protobuf.decode: argument #2 must be a string or buffer, got {}",
                value.name()
            ));
        }
    };

    let pb = match Protobuf::new() {
        Some(p) => p,
        None => return Err("protobuf.decode: descriptor not loaded".into()),
    };
    let msg = {
        let cmd_name = lua
            .value(1)
            .as_str()
            .ok_or_else(|| "protobuf.decode: UTF-8 message name expected".to_string())?;
        match pb.descriptor.find_message(cmd_name) {
            Some(m) => m,
            None => return Err(format!("protobuf.decode: message '{}' not found", cmd_name)),
        }
    };

    let mut stream = StreamReader::new(data);
    if let Err(e) = pb.decode_message(state, &mut stream, msg, 0) {
        return Err(format!("protobuf.decode error: {}", e));
    }
    Ok(1)
}

fn pb_messages(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let descriptor = match get_global_descriptor() {
        Some(d) => d,
        None => return Err("protobuf.messages: descriptor not loaded".into()),
    };
    unsafe {
        ffi::lua_createtable(state.as_ptr(), 0, descriptor.messages.len() as c_int);
    }
    for (full_name, &idx) in &descriptor.messages {
        laux::lua_push(state, full_name.as_str());
        laux::lua_push(state, descriptor.all_messages[idx].name.as_str());
        unsafe { ffi::lua_rawset(state.as_ptr(), -3) };
    }
    Ok(1)
}

fn pb_fields(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let descriptor = match get_global_descriptor() {
        Some(d) => d,
        None => return Err("protobuf.fields: descriptor not loaded".into()),
    };
    let msg = {
        let full_name = lua
            .value(1)
            .as_str()
            .ok_or_else(|| "protobuf.fields: UTF-8 message name expected".to_string())?;
        descriptor.find_message(full_name)
    };
    unsafe { ffi::lua_createtable(state.as_ptr(), 0, 16) };
    if let Some(msg) = msg {
        for field in &msg.all_fields {
            laux::lua_push(state, field.name.as_str());
            laux::lua_push(state, field.type_ as i64);
            unsafe { ffi::lua_rawset(state.as_ptr(), -3) };
        }
    }
    Ok(1)
}

fn pb_enums(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let descriptor = match get_global_descriptor() {
        Some(d) => d,
        None => return Err("protobuf.enums: descriptor not loaded".into()),
    };
    unsafe { ffi::lua_createtable(state.as_ptr(), descriptor.enums.len() as c_int, 0) };
    let mut i: ffi::lua_Integer = 0;
    for name in descriptor.enums.iter() {
        laux::lua_push(state, name.as_str());
        i += 1;
        unsafe { ffi::lua_rawseti(state.as_ptr(), -2, i) };
    }
    Ok(1)
}

pub extern "C-unwind" fn luaopen_protobuf(state: LuaState) -> c_int {
    let l = [
        lreg_try!("load", pb_load),
        lreg_try!("encode", pb_encode),
        lreg_try!("decode", pb_decode),
        lreg_try!("messages", pb_messages),
        lreg_try!("fields", pb_fields),
        lreg_try!("enums", pb_enums),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
#[path = "../../tests/unit/modules/lua_protobuf.rs"]
mod tests;
