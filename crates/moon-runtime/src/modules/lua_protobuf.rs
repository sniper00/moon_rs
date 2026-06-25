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

use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::ffi::c_int;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicPtr, Ordering};

use moon_base::laux::LuaValue;
use moon_base::{
    cstr, ffi, laux,
    laux::LuaState,
    lreg, lreg_null, luaL_newlib,
};
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
// key). std's default SipHash is overkill there, so we use FxHash — the same
// fast, non-cryptographic hash rustc uses internally. These keys are never
// attacker-controlled in a way that matters (the descriptor is loaded from a
// trusted `.proto` build artifact), so collision-DoS resistance is unneeded.

/// FxHash: `hash = (hash.rotate_left(5) ^ word) * SEED`, processing 8 bytes at
/// a time.
#[derive(Default)]
struct FxHasher {
    hash: u64,
}

impl FxHasher {
    const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

    #[inline]
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(Self::SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            self.add(u64::from_le_bytes(c.try_into().unwrap()));
        }
        let rem = chunks.remainder();
        if !rem.is_empty() {
            let mut buf = [0u8; 8];
            buf[..rem.len()].copy_from_slice(rem);
            self.add(u64::from_le_bytes(buf));
        }
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.add(i as u64);
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.add(i as u64);
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.add(i);
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.add(i as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

type FxBuildHasher = BuildHasherDefault<FxHasher>;
type FxHashMap<K, V> = HashMap<K, V, FxBuildHasher>;
type FxHashSet<T> = HashSet<T, FxBuildHasher>;

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
            FieldType::String
            | FieldType::Group
            | FieldType::Message
            | FieldType::Bytes => WireType::LengthDelimited,
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
    fields_by_name: FxHashMap<String, usize>,
}

impl PbMessage {
    fn init(&mut self) {
        for (idx, field) in self.all_fields.iter().enumerate() {
            self.fields_by_number.entry(field.number).or_insert(idx);
            self.fields_by_name
                .entry(field.name.clone())
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

    fn find_field_by_name(&self, name: &str) -> Option<&PbField> {
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
    while val >= 0x80 {
        buf.unsafe_write(0x80 | (val & 0x7f) as u8);
        val >>= 7;
    }
    buf.unsafe_write(val as u8);
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
    let _ = buf.commit(MIN_VARINT_LENGTH);
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
        let _ = buf.commit(data_len);
    } else {
        let mut tmp = [0u8; 16];
        let header_size = write_varint_to_slice(&mut tmp, data_len as u64);
        let base = buf.write_pos();
        // Re-include reserved byte + body so the realloc in `prepare` preserves
        // the body bytes, then open `header_size - MIN` extra bytes and shift.
        let _ = buf.commit(data_len + MIN_VARINT_LENGTH);
        buf.prepare(header_size);
        buf.shift_data(base + MIN_VARINT_LENGTH, data_len, base + header_size);
        buf.data_mut_at(base, header_size)
            .copy_from_slice(&tmp[..header_size]);
        let _ = buf.commit(header_size - MIN_VARINT_LENGTH);
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
        unsafe { ffi::lua_getfield(state.as_ptr(), table_abs, field.name_c.as_ptr()) };
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

        unsafe { ffi::luaL_checkstack(state.as_ptr(), 8, std::ptr::null()) };
        let stack_base = unsafe { ffi::lua_gettop(state.as_ptr()) };
        self.fill_message(state, msg);
        let table_abs = abs_index(state, -1);

        // Cache for the most-recently-appended non-map repeated field. protobuf
        // encoders emit a repeated field's elements contiguously, so we keep its
        // list table on the Lua stack and track the running length locally,
        // avoiding a `lua_getfield` + `lua_rawlen` per element. `cached_num ==
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
                    unsafe { ffi::lua_getfield(state.as_ptr(), table_abs, field.name_c.as_ptr()) };
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
            if field.oneof_index >= 0
                && (field.oneof_index as usize) < msg.oneof_decl.len()
            {
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
        let to_int = || unsafe { ffi::lua_tointegerx(state.as_ptr(), index, std::ptr::null_mut()) }
            as i64;
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
            FieldType::None | FieldType::Group => {
                Err(format!("encode: unsupported field type for '{}'", field.name))
            }
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
                unsafe { ffi::lua_geti(state.as_ptr(), index, i) };
                self.write_field_value(state, buf, field, -1, depth)?;
                unsafe { ffi::lua_pop(state.as_ptr(), 1) };
            }
            let data_len = buffer_revert_varint_space(buf, base);
            write_len_prefixed(buf, data_len);
        } else {
            for i in 1..=rawlen {
                unsafe { ffi::lua_geti(state.as_ptr(), index, i) };
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
        unsafe { ffi::luaL_checkstack(state.as_ptr(), 8, std::ptr::null()) };
        let stack_base = unsafe { ffi::lua_gettop(state.as_ptr()) };

        let mut oneof_encoded = false;
        unsafe { ffi::lua_pushnil(state.as_ptr()) };
        while unsafe { ffi::lua_next(state.as_ptr(), -2) } != 0 {
            if unsafe { ffi::lua_type(state.as_ptr(), -2) } == ffi::LUA_TSTRING {
                let key = unsafe { laux::lua_to_str(state, -2) };
                if let Some(field) = msg.find_field_by_name(key) {
                    let value_abs = abs_index(state, -1);
                    if field.is_map {
                        self.encode_map(state, buf, field, value_abs, depth + 1)?;
                    } else if field.is_repeated() {
                        self.encode_repeated(state, buf, field, value_abs, depth)?;
                    } else {
                        if field.oneof_index >= 0 {
                            if oneof_encoded {
                                return Err(format!(
                                    "encode: multiple oneof fields set in '{}'",
                                    msg.full_name
                                ));
                            }
                            oneof_encoded = true;
                        }
                        let origin_size = buf.write_pos();
                        write_wire_type(buf, field.number, field.wtype);
                        let is_empty = self.write_field_value(state, buf, field, value_abs, depth)?;
                        if is_empty && self.descriptor.ignore_empty {
                            buf.revert(buf.write_pos() - origin_size);
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
                    (f.type_name.clone(), f.type_, f.label, f.packed, f.packed_set)
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
                let is_map = msg_idx.map(|i| self.all_messages[i].is_map).unwrap_or(false);
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

fn do_load(data: &[u8]) -> Result<PbDescriptor, String> {
    let mut loader = Loader::new();
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
    loader.finalize()
}

// =========================================================================
// Thread-local encode buffer
// =========================================================================

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

extern "C-unwind" fn pb_load(state: LuaState) -> c_int {
    let data = unsafe { laux::lua_check_lstring(state, 1) };
    match do_load(data) {
        Ok(desc) => set_global_descriptor(Box::new(desc)),
        Err(e) => laux::lua_error(state, format!("protobuf.load error: {}", e)),
    }
    laux::lua_push(state, true);
    1
}

extern "C-unwind" fn pb_encode(state: LuaState) -> c_int {
    let cmd_name = unsafe { laux::lua_check_str(state, 1) };
    laux::lua_checktype(state, 2, ffi::LUA_TTABLE);

    let pb = match Protobuf::new() {
        Some(p) => p,
        None => laux::lua_error(state, "protobuf.encode: descriptor not loaded".into()),
    };
    let msg = match pb.descriptor.find_message(cmd_name) {
        Some(m) => m,
        None => laux::lua_error(state, format!("protobuf.encode: message '{}' not found", cmd_name)),
    };

    let buf = get_thread_encode_buffer();
    buf.clear();
    // The table to encode is at index 2.
    unsafe { ffi::lua_settop(state.as_ptr(), 2) };
    if let Err(e) = pb.encode_message(state, buf, msg, 0) {
        laux::lua_error(state, format!("protobuf.encode error: {}", e));
    }
    laux::lua_push(state, buf.data());
    1
}

extern "C-unwind" fn pb_decode(state: LuaState) -> c_int {
    let cmd_name = unsafe { laux::lua_check_str(state, 1) };
    let data = if let LuaValue::LightUserData(ptr) = LuaValue::from_stack(state, 2) {
        let len = laux::lua_get(state, 3);
        unsafe { std::slice::from_raw_parts(ptr as *const u8, len) }
    }else {
        unsafe { laux::lua_check_lstring(state, 2) }
    };

    let pb = match Protobuf::new() {
        Some(p) => p,
        None => laux::lua_error(state, "protobuf.decode: descriptor not loaded".into()),
    };
    let msg = match pb.descriptor.find_message(cmd_name) {
        Some(m) => m,
        None => laux::lua_error(state, format!("protobuf.decode: message '{}' not found", cmd_name)),
    };

    let mut stream = StreamReader::new(data);
    if let Err(e) = pb.decode_message(state, &mut stream, msg, 0) {
        laux::lua_error(state, format!("protobuf.decode error: {}", e));
    }
    1
}

extern "C-unwind" fn pb_messages(state: LuaState) -> c_int {
    let descriptor = match get_global_descriptor() {
        Some(d) => d,
        None => laux::lua_error(state, "protobuf.messages: descriptor not loaded".into()),
    };
    unsafe {
        ffi::lua_createtable(state.as_ptr(), 0, descriptor.messages.len() as c_int);
    }
    for (full_name, &idx) in &descriptor.messages {
        laux::lua_push(state, full_name.as_str());
        laux::lua_push(state, descriptor.all_messages[idx].name.as_str());
        unsafe { ffi::lua_rawset(state.as_ptr(), -3) };
    }
    1
}

extern "C-unwind" fn pb_fields(state: LuaState) -> c_int {
    let full_name = unsafe { laux::lua_check_str(state, 1) };
    let descriptor = match get_global_descriptor() {
        Some(d) => d,
        None => laux::lua_error(state, "protobuf.fields: descriptor not loaded".into()),
    };
    unsafe { ffi::lua_createtable(state.as_ptr(), 0, 16) };
    if let Some(msg) = descriptor.find_message(full_name) {
        for field in &msg.all_fields {
            laux::lua_push(state, field.name.as_str());
            laux::lua_push(state, field.type_ as i64);
            unsafe { ffi::lua_rawset(state.as_ptr(), -3) };
        }
    }
    1
}

extern "C-unwind" fn pb_enums(state: LuaState) -> c_int {
    let descriptor = match get_global_descriptor() {
        Some(d) => d,
        None => laux::lua_error(state, "protobuf.enums: descriptor not loaded".into()),
    };
    unsafe { ffi::lua_createtable(state.as_ptr(), descriptor.enums.len() as c_int, 0) };
    let mut i: ffi::lua_Integer = 0;
    for name in descriptor.enums.iter() {
        laux::lua_push(state, name.as_str());
        i += 1;
        unsafe { ffi::lua_rawseti(state.as_ptr(), -2, i) };
    }
    1
}

pub extern "C-unwind" fn luaopen_protobuf(state: LuaState) -> c_int {
    let l = [
        lreg!("load", pb_load),
        lreg!("encode", pb_encode),
        lreg!("decode", pb_decode),
        lreg!("messages", pb_messages),
        lreg!("fields", pb_fields),
        lreg!("enums", pb_enums),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
mod tests {
    use super::*;
    use moon_base::laux::LuaGlobalState;
    use serial_test::serial;
    use std::ffi::CString;

    // --- minimal FileDescriptorSet wire-format builder ---------------------

    fn put_varint(out: &mut Vec<u8>, mut v: u64) {
        while v >= 0x80 {
            out.push(0x80 | (v & 0x7f) as u8);
            v >>= 7;
        }
        out.push(v as u8);
    }

    /// `LEN`-delimited field: tag + length + payload.
    fn put_len(out: &mut Vec<u8>, field: u32, payload: &[u8]) {
        put_varint(out, pb_tag(field, 2) as u64);
        put_varint(out, payload.len() as u64);
        out.extend_from_slice(payload);
    }

    /// `VARINT` field.
    fn put_var(out: &mut Vec<u8>, field: u32, v: u64) {
        put_varint(out, pb_tag(field, 0) as u64);
        put_varint(out, v);
    }

    /// FieldDescriptorProto: name(1), number(3), label(4), type(5), type_name(6).
    fn field_proto(name: &str, number: u32, label: u32, type_: u32, type_name: &str) -> Vec<u8> {
        let mut f = Vec::new();
        put_len(&mut f, 1, name.as_bytes());
        put_var(&mut f, 3, number as u64);
        put_var(&mut f, 4, label as u64);
        put_var(&mut f, 5, type_ as u64);
        if !type_name.is_empty() {
            put_len(&mut f, 6, type_name.as_bytes());
        }
        f
    }

    /// Build a FileDescriptorSet describing:
    ///   syntax = "proto3"; package test;
    ///   message Foo { int32 a = 1; string b = 2; repeated int32 c = 3; }
    ///   message Bar { Foo foo = 1; map<string,int32> m = 2; }
    fn build_descriptor_set() -> Vec<u8> {
        // Foo
        let mut foo = Vec::new();
        put_len(&mut foo, 1, b"Foo");
        put_len(&mut foo, 2, &field_proto("a", 1, 1, 5, "")); // int32
        put_len(&mut foo, 2, &field_proto("b", 2, 1, 9, "")); // string
        put_len(&mut foo, 2, &field_proto("c", 3, 3, 5, "")); // repeated int32

        // Bar.MEntry (synthetic map entry: map<string,int32>)
        let mut mentry = Vec::new();
        put_len(&mut mentry, 1, b"MEntry");
        put_len(&mut mentry, 2, &field_proto("key", 1, 1, 9, "")); // string
        put_len(&mut mentry, 2, &field_proto("value", 2, 1, 5, "")); // int32
        // MessageOptions: map_entry = true (field 7)
        let mut mopts = Vec::new();
        put_var(&mut mopts, 7, 1);
        put_len(&mut mentry, 7, &mopts);

        // Bar
        let mut bar = Vec::new();
        put_len(&mut bar, 1, b"Bar");
        put_len(&mut bar, 2, &field_proto("foo", 1, 1, 11, ".test.Foo")); // message
        put_len(
            &mut bar,
            2,
            &field_proto("m", 2, 3, 11, ".test.Bar.MEntry"),
        ); // repeated map entry
        put_len(&mut bar, 3, &mentry); // nested_type

        // FileDescriptorProto
        let mut file = Vec::new();
        put_len(&mut file, 2, b"test"); // package
        put_len(&mut file, 4, &foo); // message_type
        put_len(&mut file, 4, &bar); // message_type
        put_len(&mut file, 12, b"proto3"); // syntax

        // FileDescriptorSet
        let mut set = Vec::new();
        put_len(&mut set, 1, &file);
        set
    }

    // --- general descriptor builder ---------------------------------------
    //
    // Field type numbers (descriptor.proto FieldDescriptorProto.Type).
    const T_DOUBLE: u32 = 1;
    const T_FLOAT: u32 = 2;
    const T_INT64: u32 = 3;
    const T_UINT64: u32 = 4;
    const T_INT32: u32 = 5;
    const T_FIXED64: u32 = 6;
    const T_FIXED32: u32 = 7;
    const T_BOOL: u32 = 8;
    const T_STRING: u32 = 9;
    const T_MESSAGE: u32 = 11;
    const T_BYTES: u32 = 12;
    const T_UINT32: u32 = 13;
    const T_ENUM: u32 = 14;
    const T_SFIXED32: u32 = 15;
    const T_SFIXED64: u32 = 16;
    const T_SINT32: u32 = 17;
    const T_SINT64: u32 = 18;
    // Labels.
    const L_OPTIONAL: u32 = 1;
    const L_REPEATED: u32 = 3;

    /// FieldDescriptorProto with optional `packed` option and `oneof_index`.
    #[allow(clippy::too_many_arguments)]
    fn fld(
        name: &str,
        number: u32,
        label: u32,
        type_: u32,
        type_name: &str,
        packed: Option<bool>,
        oneof_index: Option<u32>,
    ) -> Vec<u8> {
        let mut f = Vec::new();
        put_len(&mut f, 1, name.as_bytes());
        put_var(&mut f, 3, number as u64);
        put_var(&mut f, 4, label as u64);
        put_var(&mut f, 5, type_ as u64);
        if !type_name.is_empty() {
            put_len(&mut f, 6, type_name.as_bytes());
        }
        if let Some(p) = packed {
            let mut opts = Vec::new();
            put_var(&mut opts, 2, p as u64);
            put_len(&mut f, 8, &opts);
        }
        if let Some(oi) = oneof_index {
            put_var(&mut f, 9, oi as u64);
        }
        f
    }

    /// DescriptorProto from fields + nested messages + oneof names, optionally a
    /// `map_entry` message.
    fn msg(
        name: &str,
        fields: &[Vec<u8>],
        nested: &[Vec<u8>],
        oneof_decls: &[&str],
        map_entry: bool,
    ) -> Vec<u8> {
        let mut m = Vec::new();
        put_len(&mut m, 1, name.as_bytes());
        for f in fields {
            put_len(&mut m, 2, f);
        }
        for n in nested {
            put_len(&mut m, 3, n);
        }
        for od in oneof_decls {
            let mut o = Vec::new();
            put_len(&mut o, 1, od.as_bytes());
            put_len(&mut m, 8, &o);
        }
        if map_entry {
            let mut opts = Vec::new();
            put_var(&mut opts, 7, 1);
            put_len(&mut m, 7, &opts);
        }
        m
    }

    /// Synthetic `<name>` map-entry message (key=1, value=2, map_entry=true).
    fn map_entry(
        name: &str,
        key_type: u32,
        val_type: u32,
        val_type_name: &str,
    ) -> Vec<u8> {
        msg(
            name,
            &[
                fld("key", 1, L_OPTIONAL, key_type, "", None, None),
                fld("value", 2, L_OPTIONAL, val_type, val_type_name, None, None),
            ],
            &[],
            &[],
            true,
        )
    }

    /// EnumDescriptorProto with `(name, number)` values.
    fn enum_proto(name: &str, values: &[(&str, i32)]) -> Vec<u8> {
        let mut e = Vec::new();
        put_len(&mut e, 1, name.as_bytes());
        for (vn, vv) in values {
            let mut v = Vec::new();
            put_len(&mut v, 1, vn.as_bytes());
            put_var(&mut v, 2, *vv as u32 as u64);
            put_len(&mut e, 2, &v);
        }
        e
    }

    /// FileDescriptorSet wrapping one file. `syntax = None` => proto2.
    fn file_set(
        syntax: Option<&str>,
        package: &str,
        messages: &[Vec<u8>],
        enums: &[Vec<u8>],
    ) -> Vec<u8> {
        let mut file = Vec::new();
        if !package.is_empty() {
            put_len(&mut file, 2, package.as_bytes());
        }
        for m in messages {
            put_len(&mut file, 4, m);
        }
        for e in enums {
            put_len(&mut file, 5, e);
        }
        if let Some(s) = syntax {
            put_len(&mut file, 12, s.as_bytes());
        }
        let mut set = Vec::new();
        put_len(&mut set, 1, &file);
        set
    }

    fn new_vm() -> (LuaState, LuaGlobalState) {
        unsafe {
            let raw = ffi::luaL_newstate();
            assert!(!raw.is_null());
            let state = LuaState::new(raw).unwrap();
            let guard = LuaGlobalState::new(state);
            ffi::luaL_openlibs(raw);
            ffi::luaL_requiref(
                raw,
                cstr!("protobuf"),
                crate::not_null_wrapper!(luaopen_protobuf),
                1,
            );
            ffi::lua_pop(raw, 1);
            (state, guard)
        }
    }

    fn set_global_bytes(state: LuaState, name: &str, data: &[u8]) {
        unsafe {
            ffi::lua_pushlstring(state.as_ptr(), data.as_ptr() as _, data.len());
            let cname = CString::new(name).unwrap();
            ffi::lua_setglobal(state.as_ptr(), cname.as_ptr());
        }
    }

    fn run(state: LuaState, code: &str) -> Result<(), String> {
        unsafe {
            let c = CString::new(code).unwrap();
            if ffi::luaL_dostring(state.as_ptr(), c.as_ptr()) != ffi::LUA_OK {
                let err = ffi::lua_tostring(state.as_ptr(), -1);
                let msg = if err.is_null() {
                    "unknown error".to_string()
                } else {
                    std::ffi::CStr::from_ptr(err).to_string_lossy().into_owned()
                };
                ffi::lua_pop(state.as_ptr(), 1);
                Err(msg)
            } else {
                Ok(())
            }
        }
    }

    #[test]
    #[serial]
    fn protobuf_scalar_and_repeated_roundtrip() {
        let (state, _guard) = new_vm();
        set_global_bytes(state, "_desc", &build_descriptor_set());
        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_desc), "load failed")

            local bytes = pb.encode("test.Foo", { a = 42, b = "hello", c = {1, 2, 3} })
            local t = pb.decode("test.Foo", bytes)
            assert(t.a == 42, "a mismatch: " .. tostring(t.a))
            assert(t.b == "hello", "b mismatch: " .. tostring(t.b))
            assert(#t.c == 3 and t.c[1] == 1 and t.c[3] == 3, "repeated mismatch")

            -- proto3 default (zero/empty) fields are set directly on the table
            local empty = pb.decode("test.Foo", pb.encode("test.Foo", {}))
            assert(empty.a == 0, "default int should be 0")
            assert(empty.b == "", "default string should be empty")
            assert(type(empty.c) == "table" and #empty.c == 0, "default repeated should be empty table")
        "#;
        run(state, code).expect("scalar/repeated roundtrip");
    }

    #[test]
    #[serial]
    fn protobuf_nested_and_map_roundtrip() {
        let (state, _guard) = new_vm();
        set_global_bytes(state, "_desc", &build_descriptor_set());
        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_desc), "load failed")

            local bytes = pb.encode("test.Bar", {
                foo = { a = 7, b = "nested" },
                m = { ["x"] = 10, ["y"] = 20 },
            })
            local t = pb.decode("test.Bar", bytes)
            assert(t.foo.a == 7, "nested a mismatch")
            assert(t.foo.b == "nested", "nested b mismatch")
            assert(t.m.x == 10 and t.m.y == 20, "map mismatch")
        "#;
        run(state, code).expect("nested/map roundtrip");
    }

    #[test]
    #[serial]
    fn protobuf_messages_and_fields_introspection() {
        let (state, _guard) = new_vm();
        set_global_bytes(state, "_desc", &build_descriptor_set());
        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_desc))
            local msgs = pb.messages()
            assert(msgs["test.Foo"] == "Foo", "messages should map full->simple name")
            assert(msgs["test.Bar"] == "Bar")
            local fields = pb.fields("test.Foo")
            assert(fields.a == 5, "a should be TYPE_INT32 (5)")
            assert(fields.b == 9, "b should be TYPE_STRING (9)")
        "#;
        run(state, code).expect("introspection");
    }

    // ======================================================================
    // Ported from lua-protobuf test.lua (only features lua_protobuf.rs
    // supports). Skipped there: the `protoc` text compiler, `pb.io/buffer/
    // slice/conv/typefmt`, hooks, `pb.option` state flags, `pb.pack/unpack`,
    // extensions, enum name<->value (`pb.enum`), `pb.defaults`, `pb.clear/
    // type/state`, per-file mixed syntax, and tolerant decoding of malformed
    // input. Those map to APIs/behaviors this module does not implement.
    // ======================================================================

    /// `test_type`: every scalar wire type roundtrips (proto2).
    #[test]
    #[serial]
    fn ported_all_scalar_types_roundtrip() {
        let (state, _guard) = new_vm();
        let set = file_set(
            None,
            "",
            &[msg(
                "TestTypes",
                &[
                    fld("dv", 1, L_OPTIONAL, T_DOUBLE, "", None, None),
                    fld("fv", 2, L_OPTIONAL, T_FLOAT, "", None, None),
                    fld("i64v", 3, L_OPTIONAL, T_INT64, "", None, None),
                    fld("u64v", 4, L_OPTIONAL, T_UINT64, "", None, None),
                    fld("i32v", 5, L_OPTIONAL, T_INT32, "", None, None),
                    fld("u32v", 13, L_OPTIONAL, T_UINT32, "", None, None),
                    fld("f64v", 6, L_OPTIONAL, T_FIXED64, "", None, None),
                    fld("f32v", 7, L_OPTIONAL, T_FIXED32, "", None, None),
                    fld("bv", 8, L_OPTIONAL, T_BOOL, "", None, None),
                    fld("sv", 9, L_OPTIONAL, T_STRING, "", None, None),
                    fld("btv", 12, L_OPTIONAL, T_BYTES, "", None, None),
                    fld("sf32v", 15, L_OPTIONAL, T_SFIXED32, "", None, None),
                    fld("sf64v", 16, L_OPTIONAL, T_SFIXED64, "", None, None),
                    fld("s32v", 17, L_OPTIONAL, T_SINT32, "", None, None),
                    fld("s64v", 18, L_OPTIONAL, T_SINT64, "", None, None),
                ],
                &[],
                &[],
                false,
            )],
            &[],
        );
        set_global_bytes(state, "_d", &set);
        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))
            local data = {
               dv = 0.125, fv = 0.5,
               i64v = -12345678901234567, u64v = 12345678901234567,
               i32v = -2101112222, u32v = 2101112222,
               f64v = 12345678901234567, f32v = 1231231234,
               bv = true, sv = "foo", btv = "bar",
               sf32v = -123, sf64v = -456, s32v = -1234, s64v = -4321,
            }
            local t = pb.decode("TestTypes", pb.encode("TestTypes", data))
            for k, v in pairs(data) do
               assert(t[k] == v, k .. " mismatch: " .. tostring(t[k]))
            end
        "#;
        run(state, code).expect("scalar types roundtrip");
    }

    /// `test_packed`: proto3 packs scalar repeateds by default, honors an
    /// explicit `[packed=false]`, and proto2 honors `[packed=true]`.
    #[test]
    #[serial]
    fn ported_packed_wire_bytes() {
        let (state, _guard) = new_vm();
        let d1 = file_set(
            Some("proto3"),
            "",
            &[msg(
                "MyMessage",
                &[
                    fld("intList", 1, L_REPEATED, T_INT32, "", None, None),
                    fld("nopacks", 2, L_REPEATED, T_INT32, "", Some(false), None),
                ],
                &[],
                &[],
                false,
            )],
            &[],
        );
        let d2 = file_set(
            None,
            "",
            &[
                msg("Empty", &[], &[], &[], false),
                msg(
                    "TestPacked",
                    &[fld("packs", 1, L_REPEATED, T_INT64, "", Some(true), None)],
                    &[],
                    &[],
                    false,
                ),
            ],
            &[],
        );
        set_global_bytes(state, "_d1", &d1);
        set_global_bytes(state, "_d2", &d2);
        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d1))
            -- proto3 repeated scalar => packed
            assert(pb.encode("MyMessage", { intList = {1,2,3} }) == "\10\3\1\2\3",
                   "intList should be packed")
            -- explicit [packed=false] => one tag per element
            assert(pb.encode("MyMessage", { nopacks = {1,2,3} }) == "\16\1\16\2\16\3",
                   "nopacks should be unpacked")

            assert(pb.load(_d2))
            local t = pb.decode("TestPacked", pb.encode("TestPacked", { packs = {1,2,3,4,-1,-2,3} }))
            assert(#t.packs == 7, "packs length")
            assert(t.packs[5] == -1 and t.packs[6] == -2, "packs negatives")
            local e = pb.decode("TestPacked", pb.encode("TestPacked", { packs = {} }))
            assert(type(e.packs) == "table" and #e.packs == 0, "empty packed")
        "#;
        run(state, code).expect("packed wire bytes");
    }

    /// `test_map`: string-keyed maps to scalar and to message values (proto3).
    #[test]
    #[serial]
    fn ported_map_roundtrip() {
        let (state, _guard) = new_vm();
        let testmap = msg(
            "TestMap",
            &[
                fld("map", 1, L_REPEATED, T_MESSAGE, ".TestMap.MapEntry", None, None),
                fld(
                    "packed_map",
                    2,
                    L_REPEATED,
                    T_MESSAGE,
                    ".TestMap.PackedMapEntry",
                    Some(true),
                    None,
                ),
                fld(
                    "msg_map",
                    3,
                    L_REPEATED,
                    T_MESSAGE,
                    ".TestMap.MsgMapEntry",
                    None,
                    None,
                ),
            ],
            &[
                map_entry("MapEntry", T_STRING, T_INT32, ""),
                map_entry("PackedMapEntry", T_STRING, T_INT32, ""),
                map_entry("MsgMapEntry", T_STRING, T_MESSAGE, ".TestEmpty"),
            ],
            &[],
            false,
        );
        let set = file_set(
            Some("proto3"),
            "",
            &[msg("TestEmpty", &[], &[], &[], false), testmap],
            &[],
        );
        set_global_bytes(state, "_d", &set);
        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            -- empty maps roundtrip to empty tables
            local e = pb.decode("TestMap", pb.encode("TestMap",
                { map = {}, packed_map = {}, msg_map = {} }))
            assert(type(e.map) == "table" and next(e.map) == nil, "empty map")
            assert(type(e.msg_map) == "table" and next(e.msg_map) == nil, "empty msg_map")

            -- populated scalar maps; msg_map defaults to empty
            local t = pb.decode("TestMap", pb.encode("TestMap",
                { map = { one = 1, two = 2, three = 3 }, packed_map = { a = 10 } }))
            assert(t.map.one == 1 and t.map.two == 2 and t.map.three == 3, "scalar map")
            assert(t.packed_map.a == 10, "packed_map")
            assert(type(t.msg_map) == "table" and next(t.msg_map) == nil, "msg_map default")

            -- map<string, message> with an empty-message value
            local m = pb.decode("TestMap", pb.encode("TestMap", { msg_map = { [""] = {} } }))
            assert(type(m.msg_map[""]) == "table", "msg_map empty-message value")
        "#;
        run(state, code).expect("map roundtrip");
    }

    /// `test_oneof`: decode records the active field under the oneof name.
    /// (Empty-message oneof members are dropped by `ignore_empty`, so only
    /// non-empty members are exercised.)
    #[test]
    #[serial]
    fn ported_oneof_discriminator() {
        let (state, _guard) = new_vm();
        let testoneof = msg(
            "TestOneof",
            &[
                fld("foo", 1, L_OPTIONAL, T_UINT32, "", None, Some(0)),
                fld("bar", 2, L_OPTIONAL, T_STRING, "", None, Some(0)),
            ],
            &[],
            &["body"],
            false,
        );
        let outter = msg(
            "Outter",
            &[fld("msg", 1, L_OPTIONAL, T_MESSAGE, ".TestOneof", None, None)],
            &[],
            &[],
            false,
        );
        let set = file_set(Some("proto3"), "", &[testoneof, outter], &[]);
        set_global_bytes(state, "_d", &set);
        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            local a = pb.decode("TestOneof", pb.encode("TestOneof", { foo = 5 }))
            assert(a.foo == 5 and a.body == "foo", "oneof foo discriminator")

            local b = pb.decode("TestOneof", pb.encode("TestOneof", { bar = "hi" }))
            assert(b.bar == "hi" and b.body == "bar", "oneof bar discriminator")

            local o = pb.decode("Outter", pb.encode("Outter", { msg = { foo = 7 } }))
            assert(o.msg.foo == 7 and o.msg.body == "foo", "nested oneof")
        "#;
        run(state, code).expect("oneof discriminator");
    }

    /// `test_enum`: enums encode/decode as integers; `enums()` and `fields()`
    /// expose them. (Enum name<->value conversion is not supported.)
    #[test]
    #[serial]
    fn ported_enum_as_integer() {
        let (state, _guard) = new_vm();
        let set = file_set(
            None,
            "",
            &[msg(
                "TestEnum",
                &[fld("color", 1, L_OPTIONAL, T_ENUM, ".Color", None, None)],
                &[],
                &[],
                false,
            )],
            &[enum_proto("Color", &[("Red", 0), ("Green", 1), ("Blue", 2)])],
        );
        set_global_bytes(state, "_d", &set);
        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            local t = pb.decode("TestEnum", pb.encode("TestEnum", { color = 2 }))
            assert(t.color == 2, "enum integer roundtrip")

            local found = false
            for _, name in ipairs(pb.enums()) do
               if name == "Color" then found = true end
            end
            assert(found, "Color should be listed by enums()")

            assert(pb.fields("TestEnum").color == 14, "color should be TYPE_ENUM (14)")
        "#;
        run(state, code).expect("enum as integer");
    }

    /// `test_io`-style behavior: unknown fields are skipped across all wire
    /// types, leaving known fields intact.
    #[test]
    #[serial]
    fn ported_unknown_field_skipping() {
        let (state, _guard) = new_vm();
        let set = file_set(
            Some("proto3"),
            "",
            &[msg(
                "Msg",
                &[fld("a", 1, L_OPTIONAL, T_INT32, "", None, None)],
                &[],
                &[],
                false,
            )],
            &[],
        );
        set_global_bytes(state, "_d", &set);
        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            -- unknown varint field 5, then a=7
            assert(pb.decode("Msg", "\40\99\8\7").a == 7, "skip unknown varint")
            -- unknown fixed64 field 6 (8 bytes), then a=9
            assert(pb.decode("Msg", "\49\1\2\3\4\5\6\7\8\8\9").a == 9, "skip unknown fixed64")
            -- unknown fixed32 field 4 (4 bytes), then a=4
            assert(pb.decode("Msg", "\37\1\2\3\4\8\4").a == 4, "skip unknown fixed32")
            -- unknown length-delimited field 7, then a=3
            assert(pb.decode("Msg", "\58\3xyz\8\3").a == 3, "skip unknown bytes")
        "#;
        run(state, code).expect("unknown field skipping");
    }

    /// Error paths: wire-type mismatch, wrong Lua value type, unknown message.
    #[test]
    #[serial]
    fn ported_error_paths() {
        let (state, _guard) = new_vm();
        let set = file_set(
            Some("proto3"),
            "",
            &[msg(
                "M",
                &[fld("s", 1, L_OPTIONAL, T_STRING, "", None, None)],
                &[],
                &[],
                false,
            )],
            &[],
        );
        set_global_bytes(state, "_d", &set);
        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            -- field 's' is a string (LEN) but bytes carry a varint => mismatch
            assert(not pcall(pb.decode, "M", "\8\1"), "wire type mismatch must error")
            -- string field given a number
            assert(not pcall(pb.encode, "M", { s = 123 }), "wrong value type must error")
            -- unknown message names
            assert(not pcall(pb.encode, "Nope", {}), "unknown message encode must error")
            assert(not pcall(pb.decode, "Nope", ""), "unknown message decode must error")
        "#;
        run(state, code).expect("error paths");
    }

    /// Varint-heavy encode/decode throughput. Ignored by default; run with:
    ///   cargo test -p moon-runtime --release -- --ignored --nocapture protobuf_bench
    #[test]
    #[ignore]
    #[serial]
    fn protobuf_bench_varint_roundtrip() {
        use std::time::Instant;

        let (state, _guard) = new_vm();
        set_global_bytes(state, "_desc", &build_descriptor_set());

        // Warm up: load descriptor + build a varint-heavy payload (a large
        // packed repeated int32) once, then loop encode/decode in Lua so the
        // measured region is dominated by the Rust varint read/write paths.
        let iters = 200_000u64;
        let setup = r#"
            local pb = require("protobuf")
            assert(pb.load(_desc), "load failed")
            -- Foo.c is `repeated int32` (proto3 => packed): fill with mixed-size
            -- varints so the encoder/decoder exercise 1..5 byte varints.
            local c = {}
            for i = 1, 256 do c[i] = (i * 2654435761) % 2147483647 end
            _msg = { a = 123456, b = "benchmark-payload-string", c = c }
            _bytes = pb.encode("test.Foo", _msg)
        "#;
        run(state, setup).expect("bench setup");

        let bench = format!(
            r#"
            local pb = require("protobuf")
            local enc, dec = pb.encode, pb.decode
            local msg, bytes = _msg, _bytes
            local sink = 0
            for _ = 1, {iters} do
                local b = enc("test.Foo", msg)
                local t = dec("test.Foo", b)
                sink = sink + t.a
            end
            _sink = sink
        "#
        );

        let start = Instant::now();
        run(state, &bench).expect("bench loop");
        let elapsed = start.elapsed();

        let per = elapsed.as_secs_f64() / iters as f64;
        eprintln!(
            "protobuf_bench: {iters} encode+decode roundtrips in {:.3?} ({:.0} ns/op, {:.2} M ops/s)",
            elapsed,
            per * 1e9,
            (iters as f64 / elapsed.as_secs_f64()) / 1e6
        );
    }

    // --- baseline (pre-optimization) varint paths, for A/B comparison --------

    fn baseline_write_varint(buf: &mut Buffer, mut val: u64) {
        while val >= 0x80 {
            buf.write(0x80 | (val & 0x7f) as u8);
            val >>= 7;
        }
        buf.write(val as u8);
    }

    /// Exercises the cached-list decode path: interleaved non-packed repeated
    /// fields (cache must flush + recover the true length via `lua_rawlen`) and
    /// contiguous repeated messages (cache reuse across recursive decodes).
    #[test]
    #[serial]
    fn ported_repeated_interleaved_and_messages() {
        let (state, _guard) = new_vm();

        // proto2 (syntax = None) => repeated int32 defaults to unpacked, so each
        // element is its own varint-tagged occurrence in the wire stream.
        let inner = msg(
            "Inner",
            &[fld("v", 1, L_OPTIONAL, T_INT32, "", None, None)],
            &[],
            &[],
            false,
        );
        let m = msg(
            "M",
            &[
                fld("a", 1, L_REPEATED, T_INT32, "", None, None),
                fld("b", 2, L_REPEATED, T_INT32, "", None, None),
                fld("items", 3, L_REPEATED, T_MESSAGE, ".Inner", None, None),
            ],
            &[],
            &[],
            false,
        );
        let set = file_set(None, "", &[m, inner], &[]);
        set_global_bytes(state, "_d", &set);

        // Hand-crafted INTERLEAVED wire: a=10, b=20, a=11, b=21.
        // tag(a)=field1,varint=0x08 ; tag(b)=field2,varint=0x10.
        let wire = [0x08u8, 10, 0x10, 20, 0x08, 11, 0x10, 21];
        set_global_bytes(state, "_wire", &wire);

        let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            -- interleaved repeated scalars: order within each field preserved
            local t = pb.decode("M", _wire)
            assert(#t.a == 2 and t.a[1] == 10 and t.a[2] == 11, "a interleaved mismatch")
            assert(#t.b == 2 and t.b[1] == 20 and t.b[2] == 21, "b interleaved mismatch")

            -- contiguous repeated message (recursion + cache reuse)
            local enc = pb.encode("M", { items = { {v=1}, {v=2}, {v=3} } })
            local d = pb.decode("M", enc)
            assert(#d.items == 3, "items length mismatch")
            assert(d.items[1].v == 1 and d.items[2].v == 2 and d.items[3].v == 3,
                "items values mismatch")

            -- mixed: scalars and repeated messages together
            local enc2 = pb.encode("M", { a = {1,2,3}, items = { {v=7} }, b = {9} })
            local d2 = pb.decode("M", enc2)
            assert(#d2.a == 3 and d2.a[3] == 3, "mixed a mismatch")
            assert(#d2.b == 1 and d2.b[1] == 9, "mixed b mismatch")
            assert(#d2.items == 1 and d2.items[1].v == 7, "mixed items mismatch")
        "#;
        run(state, code).expect("repeated interleaved/messages roundtrip");
    }

    /// Verifies the native `pb.encode` / `pb.decode` C functions are
    /// stack-balanced: each leaves exactly one result and nothing else on the
    /// Lua stack. (The in-function `debug_assert_eq!` stack checks additionally
    /// guard every nested encode/decode during all the other tests.)
    #[test]
    #[serial]
    fn native_calls_keep_lua_stack_balanced() {
        let (state, _guard) = new_vm();
        set_global_bytes(state, "_desc", &build_descriptor_set());
        run(
            state,
            r#"
            local pb = require("protobuf")
            assert(pb.load(_desc))
            _enc, _dec = pb.encode, pb.decode
            _tbl = { foo = { a = 7, b = "hi", c = {1, 2, 3} }, m = { alpha = 1, beta = 2 } }
            _bytes = pb.encode("test.Bar", _tbl)
        "#,
        )
        .expect("setup");

        let getglobal = |name: &str| {
            let c = CString::new(name).unwrap();
            unsafe { ffi::lua_getglobal(state.as_ptr(), c.as_ptr()) };
        };
        let pushstr = |s: &str| {
            let c = CString::new(s).unwrap();
            unsafe { ffi::lua_pushstring(state.as_ptr(), c.as_ptr()) };
        };

        unsafe {
            let l = state.as_ptr();

            // pb.decode("test.Bar", _bytes)
            let top0 = ffi::lua_gettop(l);
            getglobal("_dec");
            pushstr("test.Bar");
            getglobal("_bytes");
            assert_eq!(
                ffi::lua_pcall(l, 2, 1, 0),
                ffi::LUA_OK,
                "pb.decode call failed"
            );
            assert_eq!(
                ffi::lua_gettop(l),
                top0 + 1,
                "decode must push exactly one result"
            );
            ffi::lua_pop(l, 1);
            assert_eq!(ffi::lua_gettop(l), top0, "decode left the stack unbalanced");

            // pb.encode("test.Bar", _tbl)
            let top1 = ffi::lua_gettop(l);
            getglobal("_enc");
            pushstr("test.Bar");
            getglobal("_tbl");
            assert_eq!(
                ffi::lua_pcall(l, 2, 1, 0),
                ffi::LUA_OK,
                "pb.encode call failed"
            );
            assert_eq!(
                ffi::lua_gettop(l),
                top1 + 1,
                "encode must push exactly one result"
            );
            ffi::lua_pop(l, 1);
            assert_eq!(ffi::lua_gettop(l), top1, "encode left the stack unbalanced");
        }
    }

    /// Encode-side field-name lookup throughput: the std SipHash map (previous
    /// implementation) vs the FxHash map now used for `fields_by_name`. This is
    /// the per-key cost paid by `encode_message` for every entry in the user's
    /// Lua table, so it directly reflects the encode-path change.
    ///   cargo test -p moon-runtime --release -- --ignored --nocapture protobuf_bench_name_lookup
    #[test]
    #[ignore]
    fn protobuf_bench_name_lookup() {
        use std::collections::HashMap;
        use std::hint::black_box;
        use std::time::{Duration, Instant};

        // Representative protobuf field names for a medium-sized message.
        let names: Vec<String> = [
            "id",
            "name",
            "created_at",
            "updated_at",
            "owner_id",
            "status",
            "score",
            "tags",
            "description",
            "is_active",
            "parent_id",
            "metadata",
            "version",
            "checksum",
            "payload",
            "extra_options",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        let mut std_map: HashMap<String, usize> = HashMap::default();
        let mut fx_map: FxHashMap<String, usize> = FxHashMap::default();
        for (i, n) in names.iter().enumerate() {
            std_map.insert(n.clone(), i);
            fx_map.insert(n.clone(), i);
        }

        let rounds = 500_000u64;

        let t = Instant::now();
        let mut acc = 0usize;
        for _ in 0..rounds {
            for n in &names {
                acc += *std_map.get(black_box(n.as_str())).unwrap();
            }
        }
        black_box(acc);
        let std_d = t.elapsed();

        let t = Instant::now();
        let mut acc = 0usize;
        for _ in 0..rounds {
            for n in &names {
                acc += *fx_map.get(black_box(n.as_str())).unwrap();
            }
        }
        black_box(acc);
        let fx_d = t.elapsed();

        let total = rounds * names.len() as u64;
        let ns = |d: Duration| d.as_secs_f64() / total as f64 * 1e9;
        eprintln!("protobuf_bench_name_lookup ({total} lookups each):");
        eprintln!(
            "  field name lookup: SipHash {:.2} ns/op -> FxHash {:.2} ns/op  ({:+.1}%)",
            ns(std_d),
            ns(fx_d),
            (ns(fx_d) - ns(std_d)) / ns(std_d) * 100.0
        );
    }

    /// Isolated varint read/write throughput (no Lua). Compares the optimized
    /// paths against the previous baseline so the speedup is visible without
    /// the Lua interpreter / table overhead that dominates the roundtrip bench.
    ///   cargo test -p moon-runtime --release -- --ignored --nocapture protobuf_bench_varint_only
    #[test]
    #[ignore]
    fn protobuf_bench_varint_only() {
        use std::hint::black_box;
        use std::time::Instant;

        // Mixed-size varints: 1..=10 bytes, weighted toward small values like
        // real protobuf payloads (field tags, small ints, lengths).
        let mut values: Vec<u64> = Vec::with_capacity(4096);
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for i in 0..4096u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Spread across byte-lengths: mask to (i % 9 + 1) * 7 bits.
            let bits = ((i % 9) + 1) * 7;
            values.push(x & ((1u128 << bits) - 1) as u64);
        }
        let rounds = 20_000u64;

        // --- writes ---
        let mut buf = Buffer::new();
        let t = Instant::now();
        for _ in 0..rounds {
            buf.clear();
            for &v in &values {
                baseline_write_varint(&mut buf, black_box(v));
            }
            black_box(buf.data());
        }
        let base_w = t.elapsed();

        let mut buf = Buffer::new();
        let t = Instant::now();
        for _ in 0..rounds {
            buf.clear();
            for &v in &values {
                write_varint(&mut buf, black_box(v));
            }
            black_box(buf.data());
        }
        let opt_w = t.elapsed();

        // Encoded blob to decode (the optimized writer, padded so the fast path
        // engages until the very end).
        let mut blob = Buffer::new();
        for &v in &values {
            write_varint(&mut blob, v);
        }
        let bytes = blob.data().to_vec();
        let n = values.len() as u64;

        // --- reads (both go through StreamReader + Result so the only
        // difference measured is fast-path vs forced slow-path body) ---
        let t = Instant::now();
        for _ in 0..rounds {
            let mut sr = StreamReader::new(&bytes);
            for _ in 0..n {
                black_box(sr.read_varint_slow().unwrap());
            }
        }
        let base_r = t.elapsed();

        let t = Instant::now();
        for _ in 0..rounds {
            let mut sr = StreamReader::new(&bytes);
            for _ in 0..n {
                black_box(sr.read_varint().unwrap());
            }
        }
        let opt_r = t.elapsed();

        let total = rounds * n;
        let ns = |d: std::time::Duration| d.as_secs_f64() / total as f64 * 1e9;
        eprintln!("protobuf_bench_varint_only ({total} ops each):");
        eprintln!(
            "  write: baseline {:.2} ns/op -> optimized {:.2} ns/op  ({:+.1}%)",
            ns(base_w),
            ns(opt_w),
            (ns(opt_w) - ns(base_w)) / ns(base_w) * 100.0
        );
        eprintln!(
            "  read:  baseline {:.2} ns/op -> optimized {:.2} ns/op  ({:+.1}%)",
            ns(base_r),
            ns(opt_r),
            (ns(opt_r) - ns(base_r)) / ns(base_r) * 100.0
        );
    }
}
