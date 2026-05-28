use std::collections::HashMap;
use std::sync::atomic::{AtomicPtr, Ordering};

use actor::buffer::Buffer;
use luars::{Lua, LuaApi, LuaRawTable, LuaResult, LuaState, LuaTable, LuaValue};

use crate::{lua_check_bytes, lua_check_str};

const MAX_RECURSION_DEPTH: usize = 64;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(dead_code)]
enum TType {
    Stop = 0,
    Void = 1,
    Bool = 2,
    Byte = 3,
    Double = 4,
    I16 = 6,
    I32 = 8,
    I64 = 10,
    String = 11,
    Struct = 12,
    Map = 13,
    Set = 14,
    List = 15,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CompactType {
    Stop = 0x00,
    BooleanTrue = 0x01,
    BooleanFalse = 0x02,
    Byte = 0x03,
    I16 = 0x04,
    I32 = 0x05,
    I64 = 0x06,
    Double = 0x07,
    Binary = 0x08,
    List = 0x09,
    Set = 0x0A,
    Map = 0x0B,
    Struct = 0x0C,
}

impl CompactType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x00 => Some(Self::Stop),
            0x01 => Some(Self::BooleanTrue),
            0x02 => Some(Self::BooleanFalse),
            0x03 => Some(Self::Byte),
            0x04 => Some(Self::I16),
            0x05 => Some(Self::I32),
            0x06 => Some(Self::I64),
            0x07 => Some(Self::Double),
            0x08 => Some(Self::Binary),
            0x09 => Some(Self::List),
            0x0A => Some(Self::Set),
            0x0B => Some(Self::Map),
            0x0C => Some(Self::Struct),
            _ => None,
        }
    }
}

fn ttype_to_compact(t: TType) -> CompactType {
    match t {
        TType::Stop => CompactType::Stop,
        TType::Bool => CompactType::BooleanTrue,
        TType::Byte => CompactType::Byte,
        TType::Double => CompactType::Double,
        TType::I16 => CompactType::I16,
        TType::I32 => CompactType::I32,
        TType::I64 => CompactType::I64,
        TType::String => CompactType::Binary,
        TType::Struct => CompactType::Struct,
        TType::Map => CompactType::Map,
        TType::Set => CompactType::Set,
        TType::List => CompactType::List,
        TType::Void => CompactType::Stop,
    }
}

struct ThriftField {
    type_: TType,
    id: i16,
    name: String,
    type_name: String,
    struct_type: std::cell::Cell<Option<usize>>,
}

struct ThriftStruct {
    name: String,
    meta_name: String,
    all_fields: Vec<ThriftField>,
    fast_fields: [Option<usize>; 32],
    fields_by_id: HashMap<i16, usize>,
    fields_by_name: HashMap<String, usize>,
}

impl ThriftStruct {
    fn new(name: String) -> Self {
        Self {
            meta_name: format!("__thrift_meta_{}", name),
            name,
            all_fields: Vec::new(),
            fast_fields: [None; 32],
            fields_by_id: HashMap::new(),
            fields_by_name: HashMap::new(),
        }
    }

    fn init(&mut self) {
        for (idx, field) in self.all_fields.iter().enumerate() {
            self.fields_by_id.insert(field.id, idx);
            self.fields_by_name.insert(field.name.clone(), idx);
            if field.id >= 0 && (field.id as usize) < self.fast_fields.len() {
                self.fast_fields[field.id as usize] = Some(idx);
            }
        }
    }

    fn find_field_by_id(&self, id: i16) -> Option<&ThriftField> {
        let idx = if id >= 0 && (id as usize) < self.fast_fields.len() {
            self.fast_fields[id as usize]
        } else {
            self.fields_by_id.get(&id).copied()
        };
        idx.map(|i| &self.all_fields[i])
    }

    fn find_field_by_name(&self, name: &str) -> Option<&ThriftField> {
        self.fields_by_name
            .get(name)
            .map(|&i| &self.all_fields[i])
    }
}

struct ThriftDescriptor {
    all_structs: Vec<ThriftStruct>,
    structs_by_name: HashMap<String, usize>,
    enums: HashMap<String, ()>,
}

impl ThriftDescriptor {
    fn new() -> Self {
        Self {
            all_structs: Vec::new(),
            structs_by_name: HashMap::new(),
            enums: HashMap::new(),
        }
    }

    fn find_struct(&self, name: &str) -> Option<&ThriftStruct> {
        self.structs_by_name
            .get(name)
            .map(|&i| &self.all_structs[i])
    }

    fn add_struct(&mut self, s: ThriftStruct) -> usize {
        let idx = self.all_structs.len();
        self.structs_by_name.insert(s.name.clone(), idx);
        self.all_structs.push(s);
        idx
    }
}

static GLOBAL_DESCRIPTOR: AtomicPtr<ThriftDescriptor> = AtomicPtr::new(std::ptr::null_mut());

fn get_global_descriptor() -> &'static ThriftDescriptor {
    let ptr = GLOBAL_DESCRIPTOR.load(Ordering::Acquire);
    if ptr.is_null() {
        panic!("thrift descriptor not loaded");
    }
    unsafe { &*ptr }
}

fn set_global_descriptor(desc: Box<ThriftDescriptor>) {
    let ptr = Box::into_raw(desc);
    let old = GLOBAL_DESCRIPTOR.swap(ptr, Ordering::AcqRel);
    if !old.is_null() {
        drop(unsafe { Box::from_raw(old) });
    }
}

// ========== Encoding helpers ==========

const MIN_VARINT_LENGTH: usize = 2;

fn zigzag_encode_i16(n: i16) -> u16 {
    ((n as u16) << 1) ^ ((n >> 15) as u16)
}

fn zigzag_encode_i32(n: i32) -> u32 {
    ((n as u32) << 1) ^ ((n >> 31) as u32)
}

fn zigzag_encode_i64(n: i64) -> u64 {
    ((n as u64) << 1) ^ ((n >> 63) as u64)
}

fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

fn write_varint(buf: &mut Buffer, mut val: u64) {
    while val >= 0x80 {
        buf.write(0x80 | (val & 0x7f) as u8);
        val >>= 7;
    }
    buf.write(val as u8);
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

fn write_signed_varint_i16(buf: &mut Buffer, val: i16) {
    write_varint(buf, zigzag_encode_i16(val) as u64);
}

fn write_signed_varint_i32(buf: &mut Buffer, val: i32) {
    write_varint(buf, zigzag_encode_i32(val) as u64);
}

fn write_signed_varint_i64(buf: &mut Buffer, val: i64) {
    write_varint(buf, zigzag_encode_i64(val) as u64);
}

fn write_binary(buf: &mut Buffer, data: &[u8]) {
    write_varint(buf, data.len() as u64);
    buf.write_slice(data);
}

fn write_field_header(buf: &mut Buffer, field: &ThriftField, last_field_id: &mut i16, type_override: i8) {
    let ct = ttype_to_compact(field.type_);
    let type_to_write = if type_override == -1 {
        ct as u8
    } else {
        type_override as u8
    };

    if field.id > *last_field_id && field.id - *last_field_id <= 15 {
        buf.write(((field.id - *last_field_id) as u8) << 4 | type_to_write);
    } else {
        buf.write(type_to_write);
        write_varint(buf, zigzag_encode_i16(field.id) as u64);
    }

    *last_field_id = field.id;
}

fn write_field_stop(buf: &mut Buffer) {
    buf.write(CompactType::Stop as u8);
}

fn write_boolean_value(buf: &mut Buffer, value: bool) {
    buf.write(if value {
        CompactType::BooleanTrue as u8
    } else {
        CompactType::BooleanFalse as u8
    });
}

/// Reserve space for a varint header. Returns the write position after the reserved bytes.
fn buffer_reserve_varint_space(buf: &mut Buffer) -> usize {
    buf.prepare(64);
    buf.commit(MIN_VARINT_LENGTH);
    buf.write_pos()
}

/// Revert the reserved varint space and the data written after it.
/// Returns (data_offset, data_len): the absolute offset and length of the written data.
fn buffer_revert_varint_space(buf: &mut Buffer, origin_write_pos: usize) -> (usize, usize) {
    let data_offset = origin_write_pos;
    let data_len = buf.write_pos() - origin_write_pos;
    buf.revert(data_len + MIN_VARINT_LENGTH);
    (data_offset, data_len)
}

// ========== Decoding helpers ==========

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
        let mut result: u64 = 0;
        let mut shift = 0;
        while shift < 64 {
            let byte = self.read_byte()?;
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        Err("varint too long".into())
    }

    fn read_signed_varint(&mut self) -> Result<i64, String> {
        self.read_varint().map(zigzag_decode)
    }

    fn read_fixed_f64(&mut self) -> Result<f64, String> {
        if self.remaining() < 8 {
            return Err("not enough data for double".into());
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.data[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(f64::from_le_bytes(bytes))
    }

    fn read_binary(&mut self) -> Result<&'a [u8], String> {
        let len = self.read_varint()? as usize;
        if len > self.remaining() {
            return Err(format!(
                "read_binary: need {} bytes, have {}",
                len,
                self.remaining()
            ));
        }
        let start = self.pos;
        self.pos += len;
        Ok(&self.data[start..start + len])
    }

    fn read_field_header(&mut self, last_field_id: &mut i16) -> Result<CompactType, String> {
        let byte = self.read_byte()?;
        let ct = CompactType::from_u8(byte & 0x0f)
            .ok_or_else(|| format!("invalid compact type: {}", byte & 0x0f))?;
        if ct == CompactType::Stop {
            return Ok(ct);
        }

        let modifier = (byte >> 4) as i16;
        if modifier == 0 {
            *last_field_id = self.read_signed_varint()? as i16;
        } else {
            *last_field_id += modifier;
        }

        Ok(ct)
    }

    fn read_list_set_header(&mut self) -> Result<(u64, CompactType), String> {
        let byte = self.read_byte()?;
        let mut size = ((byte >> 4) & 0x0F) as u64;
        let element_type = CompactType::from_u8(byte & 0x0F)
            .ok_or_else(|| format!("invalid list/set element type: {}", byte & 0x0F))?;
        if size == 0x0F {
            size = self.read_varint()?;
        }
        Ok((size, element_type))
    }

    fn read_map_header(&mut self) -> Result<(u64, CompactType, CompactType), String> {
        let size = self.read_varint()?;
        if size == 0 {
            return Ok((0, CompactType::Stop, CompactType::Stop));
        }
        let byte = self.read_byte()?;
        let key_type = CompactType::from_u8(byte >> 4)
            .ok_or_else(|| format!("invalid map key type: {}", byte >> 4))?;
        let value_type = CompactType::from_u8(byte & 0x0F)
            .ok_or_else(|| format!("invalid map value type: {}", byte & 0x0F))?;
        Ok((size, key_type, value_type))
    }

    fn read_boolean_value(&mut self) -> Result<bool, String> {
        let b = self.read_byte()?;
        match b {
            x if x == CompactType::BooleanTrue as u8 => Ok(true),
            x if x == CompactType::BooleanFalse as u8 => Ok(false),
            _ => Err("invalid boolean byte".into()),
        }
    }
}

// ========== Skip field ==========

fn skip_field(stream: &mut StreamReader, ct: CompactType, depth: usize) -> Result<(), String> {
    if depth > MAX_RECURSION_DEPTH {
        return Err("skip_field: maximum recursion depth exceeded".into());
    }
    match ct {
        CompactType::Stop => {}
        CompactType::BooleanTrue | CompactType::BooleanFalse => {}
        CompactType::Byte => {
            stream.read_byte()?;
        }
        CompactType::I16 | CompactType::I32 | CompactType::I64 => {
            stream.read_signed_varint()?;
        }
        CompactType::Double => {
            stream.read_fixed_f64()?;
        }
        CompactType::Binary => {
            stream.read_binary()?;
        }
        CompactType::Struct => {
            let mut last_field_id: i16 = 0;
            loop {
                let field_type = stream.read_field_header(&mut last_field_id)?;
                if field_type == CompactType::Stop {
                    break;
                }
                skip_field(stream, field_type, depth + 1)?;
            }
        }
        CompactType::Map => {
            let (size, key_type, value_type) = stream.read_map_header()?;
            if size == 0 {
                return Ok(());
            }
            for _ in 0..size {
                if is_boolean_compact_type(key_type) {
                    stream.read_boolean_value()?;
                } else {
                    skip_field(stream, key_type, depth + 1)?;
                }
                if is_boolean_compact_type(value_type) {
                    stream.read_boolean_value()?;
                } else {
                    skip_field(stream, value_type, depth + 1)?;
                }
            }
        }
        CompactType::List | CompactType::Set => {
            let (size, element_type) = stream.read_list_set_header()?;
            if size > 0 && is_boolean_compact_type(element_type) {
                for _ in 0..size {
                    stream.read_boolean_value()?;
                }
            } else {
                for _ in 0..size {
                    skip_field(stream, element_type, depth + 1)?;
                }
            }
        }
    }
    Ok(())
}

fn is_boolean_compact_type(ct: CompactType) -> bool {
    ct == CompactType::BooleanTrue
}

fn expected_compact_type(field: &ThriftField) -> CompactType {
    if field.type_ == TType::Bool {
        return CompactType::BooleanTrue;
    }
    ttype_to_compact(field.type_)
}

fn compact_type_matches_field(field: &ThriftField, ct: CompactType) -> bool {
    if field.type_ == TType::Bool {
        return ct == CompactType::BooleanTrue || ct == CompactType::BooleanFalse;
    }
    expected_compact_type(field) == ct
}

// ========== Protocol (encode/decode) ==========

struct ThriftProtocol<'a> {
    descriptor: &'a ThriftDescriptor,
}

impl<'a> ThriftProtocol<'a> {
    fn new() -> Self {
        Self {
            descriptor: get_global_descriptor(),
        }
    }

    fn get_struct_for_field(&self, field: &ThriftField) -> Option<&'a ThriftStruct> {
        if let Some(cached) = field.struct_type.get() {
            return Some(&self.descriptor.all_structs[cached]);
        }
        if field.type_name.is_empty() {
            return None;
        }
        let idx = self.descriptor.structs_by_name.get(&field.type_name)?;
        field.struct_type.set(Some(*idx));
        Some(&self.descriptor.all_structs[*idx])
    }

    fn require_struct_for_field(&self, field: &ThriftField, context: &str) -> Result<&'a ThriftStruct, String> {
        self.get_struct_for_field(field).ok_or_else(|| {
            format!(
                "{}: missing type descriptor for field '{}' ({})",
                context, field.name, field.type_name
            )
        })
    }

    // ===== Decode =====

    fn fill_struct(&self, state: &mut LuaState, struct_type: &ThriftStruct) -> LuaResult<LuaValue> {
        let table = state.create_table_with_capacity(0, struct_type.all_fields.len())?;

        let mt = if let Some(existing) = state.registry_get::<LuaTable>(&struct_type.meta_name)? {
            existing
        } else {
            self.create_metatable(state, struct_type)?
        };
        table.set_metatable(Some(&mt))?;
        Ok(unsafe {
            table.to_value()
        })
    }

    fn create_metatable(&self, state: &mut LuaState, struct_type: &ThriftStruct) -> LuaResult<LuaTable> {
        let metatable = state.create_table_with_capacity(0, 1)?;
        let index_table = state.create_table_with_capacity(0, struct_type.all_fields.len())?;

        for field in &struct_type.all_fields {
            let default_val: LuaValue = match field.type_ {
                TType::Bool => LuaValue::boolean(false),
                TType::Double => LuaValue::float(0.0),
                TType::String => state.create_string("")?,
                TType::Struct => LuaValue::nil(),
                TType::List | TType::Set | TType::Map => state.create_table(0, 0)?,
                _ => LuaValue::integer(0),
            };
            index_table.raw_set(&field.name as &str, default_val)?;
        }

        metatable.raw_set("__index", index_table)?;
        state.registry_set(&struct_type.meta_name, &metatable)?;

        Ok(metatable)
    }

    fn decode_struct(
        &self,
        state: &mut LuaState,
        stream: &mut StreamReader,
        struct_type: &ThriftStruct,
        depth: usize,
    ) -> LuaResult<LuaValue> {
        if depth > MAX_RECURSION_DEPTH {
            return Err(state.error("decode_struct: maximum recursion depth exceeded".to_string()));
        }

        let table = self.fill_struct(state, struct_type)?;
        let mut last_field_id: i16 = 0;

        while !stream.is_empty() {
            let ct = stream
                .read_field_header(&mut last_field_id)
                .map_err(|e| state.error(e))?;
            if ct == CompactType::Stop {
                break;
            }

            let field = match struct_type.find_field_by_id(last_field_id) {
                Some(f) => f,
                None => {
                    skip_field(stream, ct, depth).map_err(|e| state.error(e))?;
                    continue;
                }
            };

            if !compact_type_matches_field(field, ct) {
                return Err(state.error(format!(
                    "decode_struct: field '{}' type mismatch, expected {:?} got {:?}",
                    field.name,
                    expected_compact_type(field),
                    ct
                )));
            }

            let val = self.decode_field(state, stream, field, ct, depth)?;
            let key = state.create_string(&field.name)?;
            state.raw_set(&table, key, val);
        }

        Ok(table)
    }

    fn decode_field(
        &self,
        state: &mut LuaState,
        stream: &mut StreamReader,
        field: &ThriftField,
        ct: CompactType,
        depth: usize,
    ) -> LuaResult<LuaValue> {
        if depth > MAX_RECURSION_DEPTH {
            return Err(state.error("decode_field: maximum recursion depth exceeded".to_string()));
        }

        match ct {
            CompactType::BooleanTrue => Ok(LuaValue::boolean(true)),
            CompactType::BooleanFalse => Ok(LuaValue::boolean(false)),
            CompactType::Byte => {
                let v = stream.read_byte().map_err(|e| state.error(e))?;
                Ok(LuaValue::integer(v as i8 as i64))
            }
            CompactType::I16 => {
                let v = stream.read_signed_varint().map_err(|e| state.error(e))?;
                Ok(LuaValue::integer(v as i16 as i64))
            }
            CompactType::I32 => {
                let v = stream.read_signed_varint().map_err(|e| state.error(e))?;
                Ok(LuaValue::integer(v as i32 as i64))
            }
            CompactType::I64 => {
                let v = stream.read_signed_varint().map_err(|e| state.error(e))?;
                Ok(LuaValue::integer(v))
            }
            CompactType::Double => {
                let v = stream.read_fixed_f64().map_err(|e| state.error(e))?;
                Ok(LuaValue::float(v))
            }
            CompactType::Binary => {
                let data = stream.read_binary().map_err(|e| state.error(e))?;
                state.create_bytes(data)
            }
            CompactType::Struct => {
                let st = self
                    .require_struct_for_field(field, "decode_field struct")
                    .map_err(|e| state.error(e))?;
                self.decode_struct(state, stream, st, depth + 1)
            }
            CompactType::List | CompactType::Set => {
                let (size, element_type) = stream
                    .read_list_set_header()
                    .map_err(|e| state.error(e))?;

                let st = self
                    .require_struct_for_field(field, "decode_field list/set")
                    .map_err(|e| state.error(e))?;
                let vfield = st.find_field_by_id(1).ok_or_else(|| {
                    state.error(format!(
                        "decode_field list/set: missing value descriptor for field '{}'",
                        field.name
                    ))
                })?;

                let table = state.create_table(size.min(16) as usize, 0)?;
                for i in 0..size {
                    let val = if is_boolean_compact_type(element_type) {
                        let bv = stream.read_boolean_value().map_err(|e| state.error(e))?;
                        LuaValue::boolean(bv)
                    } else {
                        self.decode_field(state, stream, vfield, element_type, depth)?
                    };
                    state.raw_seti(&table, (i + 1) as i64, val);
                }
                Ok(table)
            }
            CompactType::Map => {
                let (size, key_type, value_type) = stream
                    .read_map_header()
                    .map_err(|e| state.error(e))?;
                if size == 0 {
                    return state.create_table(0, 0);
                }

                let st = self
                    .require_struct_for_field(field, "decode_field map")
                    .map_err(|e| state.error(e))?;
                let kfield = st.find_field_by_id(1).ok_or_else(|| {
                    state.error(format!(
                        "decode_field map: missing key descriptor for field '{}'",
                        field.name
                    ))
                })?;
                let vfield = st.find_field_by_id(2).ok_or_else(|| {
                    state.error(format!(
                        "decode_field map: missing value descriptor for field '{}'",
                        field.name
                    ))
                })?;

                let table = state.create_table(0, size as usize)?;
                for _ in 0..size {
                    let k = if is_boolean_compact_type(key_type) {
                        let kv = stream.read_boolean_value().map_err(|e| state.error(e))?;
                        LuaValue::boolean(kv)
                    } else {
                        self.decode_field(state, stream, kfield, key_type, depth)?
                    };
                    let v = if is_boolean_compact_type(value_type) {
                        let vv = stream.read_boolean_value().map_err(|e| state.error(e))?;
                        LuaValue::boolean(vv)
                    } else {
                        self.decode_field(state, stream, vfield, value_type, depth)?
                    };
                    state.raw_set(&table, k, v);
                }
                Ok(table)
            }
            CompactType::Stop => Ok(LuaValue::nil()),
        }
    }

    // ===== Encode =====

    fn encode_struct(
        &self,
        state: &mut LuaState,
        table: LuaValue,
        buf: &mut Buffer,
        struct_type: &ThriftStruct,
        depth: usize,
    ) -> Result<(), String> {
        if depth > MAX_RECURSION_DEPTH {
            return Err("encode_struct: maximum recursion depth exceeded".into());
        }

        let t = table
            .as_table()
            .ok_or_else(|| format!("encode_struct '{}': expected table", struct_type.name))?;

        let mut last_field_id: i16 = 0;
        let pairs = t.iter_all();

        for (k, v) in pairs {
            if v.is_nil() {
                continue;
            }
            let key_str = match k.as_str() {
                Some(s) => s,
                None => continue,
            };
            let field = match struct_type.find_field_by_name(key_str) {
                Some(f) => f,
                None => continue,
            };

            if field.type_ != TType::Bool {
                write_field_header(buf, field, &mut last_field_id, -1);
            }
            self.encode_field(state, v, buf, field, &mut last_field_id, depth + 1)?;
        }

        write_field_stop(buf);
        Ok(())
    }

    fn encode_field(
        &self,
        state: &mut LuaState,
        val: LuaValue,
        buf: &mut Buffer,
        field: &ThriftField,
        last_field_id: &mut i16,
        depth: usize,
    ) -> Result<(), String> {
        if depth > MAX_RECURSION_DEPTH {
            return Err("encode_field: maximum recursion depth exceeded".into());
        }

        match field.type_ {
            TType::List | TType::Set => {
                self.encode_list(state, val, buf, field, depth)?;
            }
            TType::Map => {
                self.encode_map(state, val, buf, field, depth)?;
            }
            TType::Bool => {
                let bval = val.as_boolean().unwrap_or(false);
                let type_override = if bval {
                    CompactType::BooleanTrue as i8
                } else {
                    CompactType::BooleanFalse as i8
                };
                write_field_header(buf, field, last_field_id, type_override);
            }
            TType::Byte => {
                let v = val.as_integer().unwrap_or(0) as u8;
                buf.write(v);
            }
            TType::I16 => {
                let v = val.as_integer().unwrap_or(0) as i16;
                write_signed_varint_i16(buf, v);
            }
            TType::I32 => {
                let v = val.as_integer().unwrap_or(0) as i32;
                write_signed_varint_i32(buf, v);
            }
            TType::I64 => {
                let v = val.as_integer().unwrap_or(0);
                write_signed_varint_i64(buf, v);
            }
            TType::Double => {
                let v = val.as_number().unwrap_or(0.0);
                buf.write_slice(&v.to_le_bytes());
            }
            TType::String => {
                let bytes = val.as_bytes().ok_or_else(|| {
                    format!(
                        "expected 'string' type got '{}' for field '{}'",
                        val.type_name(),
                        field.name
                    )
                })?;
                write_binary(buf, bytes);
            }
            TType::Struct => {
                let st = self.require_struct_for_field(field, "encode_field struct")?;
                self.encode_struct(state, val, buf, st, depth)?;
            }
            _ => {
                return Err(format!(
                    "encode_field: unsupported field type {:?}",
                    field.type_
                ));
            }
        }
        Ok(())
    }

    fn encode_list(
        &self,
        state: &mut LuaState,
        val: LuaValue,
        buf: &mut Buffer,
        field: &ThriftField,
        depth: usize,
    ) -> Result<(), String> {
        let t = val
            .as_table()
            .ok_or_else(|| format!("encode_list: field '{}' expected table", field.name))?;

        let st = self.require_struct_for_field(field, "encode_list")?;
        let vfield = st
            .find_field_by_id(1)
            .ok_or("encode_list: missing value field descriptor")?;

        let rawlen = t.len();
        let compact_element_type = ttype_to_compact(vfield.type_);

        if rawlen < 15 {
            let byte = ((rawlen as u8) << 4) | (compact_element_type as u8);
            buf.write(byte);
        } else {
            let byte = 0xF0 | (compact_element_type as u8);
            buf.write(byte);
            write_varint(buf, rawlen as u64);
        }

        let mut element_last_field_id: i16 = 0;
        for i in 1..=rawlen as i64 {
            let elem = t.raw_geti(i).unwrap_or(LuaValue::nil());
            if vfield.type_ == TType::Bool {
                write_boolean_value(buf, elem.as_boolean().unwrap_or(false));
            } else {
                self.encode_field(state, elem, buf, vfield, &mut element_last_field_id, depth)?;
            }
        }
        Ok(())
    }

    fn encode_map(
        &self,
        state: &mut LuaState,
        val: LuaValue,
        buf: &mut Buffer,
        field: &ThriftField,
        depth: usize,
    ) -> Result<(), String> {
        let t = val
            .as_table()
            .ok_or_else(|| format!("encode_map: field '{}' expected table", field.name))?;

        let st = self.require_struct_for_field(field, "encode_map")?;
        let kfield = st
            .find_field_by_id(1)
            .ok_or("encode_map: missing key field descriptor")?;
        let vfield = st
            .find_field_by_id(2)
            .ok_or("encode_map: missing value field descriptor")?;

        // Reserve space for the map header (varint count + key/value types byte).
        // Write entries first, then fill in the header.
        let base = buffer_reserve_varint_space(buf);
        let mut count: u32 = 0;

        let pairs = t.iter_all();
        for (k, v) in pairs {
            let mut key_last_field_id: i16 = 0;
            let mut value_last_field_id: i16 = 0;
            if kfield.type_ == TType::Bool {
                write_boolean_value(buf, k.as_boolean().unwrap_or(false));
            } else {
                self.encode_field(state, k, buf, kfield, &mut key_last_field_id, depth)?;
            }
            if vfield.type_ == TType::Bool {
                write_boolean_value(buf, v.as_boolean().unwrap_or(false));
            } else {
                self.encode_field(state, v, buf, vfield, &mut value_last_field_id, depth)?;
            }
            count += 1;
        }

        let (_data_offset, data_len) = buffer_revert_varint_space(buf, base);

        if count == 0 {
            buf.write(0);
            return Ok(());
        }

        let key_value_types =
            ((ttype_to_compact(kfield.type_) as u8) << 4) | (ttype_to_compact(vfield.type_) as u8);

        if count < 0x80 {
            // Header fits in MIN_VARINT_LENGTH (varint=1 byte + types=1 byte = 2 bytes)
            write_varint(buf, count as u64);
            buf.write(key_value_types);
            // Data is already in place after the reserved bytes, just re-commit
            buf.commit(data_len);
        } else {
            // Header is larger than reserved space, need to shift data
            let mut tmp = [0u8; 16];
            let varint_len = write_varint_to_slice(&mut tmp, count as u64);
            tmp[varint_len] = key_value_types;
            let header_size = varint_len + 1;

            let shift_base = buf.write_pos();
            // Re-commit the reserved bytes + data so shift_data can access it
            buf.commit(data_len + MIN_VARINT_LENGTH);
            // Ensure capacity for the larger header
            buf.prepare(header_size);
            // Shift the pair data from (shift_base + MIN_VARINT_LENGTH) to (shift_base + header_size)
            buf.shift_data(shift_base + MIN_VARINT_LENGTH, data_len, shift_base + header_size);
            // Write the header at shift_base
            buf.data_mut_at(shift_base, header_size).copy_from_slice(&tmp[..header_size]);
            // Commit the extra bytes (header_size - MIN_VARINT_LENGTH)
            buf.commit(header_size - MIN_VARINT_LENGTH);
        }
        Ok(())
    }
}

// ========== Lua-facing functions ==========

fn lua_to_ttype(type_str: &str) -> TType {
    match type_str {
        "boolean" => TType::Bool,
        "byte" => TType::Byte,
        "i16" => TType::I16,
        "i32" => TType::I32,
        "i64" => TType::I64,
        "double" => TType::Double,
        "string" => TType::String,
        "list" => TType::List,
        "set" => TType::Set,
        "map" => TType::Map,
        "void" => TType::Void,
        _ => TType::Struct,
    }
}

fn load_field(
    state: &mut LuaState,
    field_type_val: LuaValue,
    descriptor: &mut ThriftDescriptor,
) -> Result<(TType, String), String> {
    let ft = field_type_val
        .as_table()
        .ok_or("load_field: expected table for fieldType")?;

    let type_key = state.create_string("type").map_err(|_| "oom".to_string())?;
    let type_val = ft.raw_get(&type_key);
    let type_str_owned;
    let type_str = match &type_val {
        Some(v) => {
            let s = v.as_str().unwrap_or("void");
            if let Some(pos) = s.rfind('.') {
                type_str_owned = s[pos + 1..].to_string();
                type_str_owned.as_str()
            } else {
                s
            }
        }
        None => "void",
    };

    let mut ttype = lua_to_ttype(type_str);
    if descriptor.enums.contains_key(type_str) {
        ttype = TType::I32;
    }

    let type_name;

    match ttype {
        TType::Struct => {
            type_name = type_str.to_string();
        }
        TType::List | TType::Set => {
            let vt_key = state
                .create_string("valueType")
                .map_err(|_| "oom".to_string())?;
            let vt_val = ft
                .raw_get(&vt_key)
                .ok_or("list/set missing valueType")?;

            let (vtype, vtype_name) = load_field(state, vt_val, descriptor)?;

            let struct_name = format!("{}<{}>", type_str, vtype_name);
            type_name = struct_name.clone();

            let mut struct_def = ThriftStruct::new(struct_name);
            struct_def.all_fields.push(ThriftField {
                type_: vtype,
                id: 1,
                name: "$value".to_string(),
                type_name: vtype_name,
                struct_type: std::cell::Cell::new(None),
            });
            struct_def.init();
            descriptor.add_struct(struct_def);
        }
        TType::Map => {
            let kt_key = state
                .create_string("keyType")
                .map_err(|_| "oom".to_string())?;
            let kt_val = ft
                .raw_get(&kt_key)
                .ok_or("map missing keyType")?;
            let (ktype, ktype_name) = load_field(state, kt_val, descriptor)?;

            let vt_key = state
                .create_string("valueType")
                .map_err(|_| "oom".to_string())?;
            let vt_val = ft
                .raw_get(&vt_key)
                .ok_or("map missing valueType")?;
            let (vtype, vtype_name) = load_field(state, vt_val, descriptor)?;

            let struct_name = format!("{}<{}, {}>", type_str, ktype_name, vtype_name);
            type_name = struct_name.clone();

            let mut struct_def = ThriftStruct::new(struct_name);
            struct_def.all_fields.push(ThriftField {
                type_: ktype,
                id: 1,
                name: "$key".to_string(),
                type_name: ktype_name,
                struct_type: std::cell::Cell::new(None),
            });
            struct_def.all_fields.push(ThriftField {
                type_: vtype,
                id: 2,
                name: "$value".to_string(),
                type_name: vtype_name,
                struct_type: std::cell::Cell::new(None),
            });
            struct_def.init();
            descriptor.add_struct(struct_def);
        }
        _ => {
            type_name = type_str.to_string();
        }
    }

    Ok((ttype, type_name))
}

fn load_thrift(state: &mut LuaState) -> LuaResult<usize> {
    let arg = state
        .get_arg(1)
        .ok_or_else(|| state.error("load_thrift: expected table argument".to_string()))?;
    if !arg.is_table() {
        return Err(state.error("load_thrift: expected table argument".to_string()));
    }

    let result: Result<(), String> = (|| {
        let mut descriptor = ThriftDescriptor::new();

        // Parse enums
        let enums_key = state.create_string("enums").map_err(|_| "oom".to_string())?;
        if let Some(enums_table) = state.raw_get(&arg, &enums_key) {
            if let Some(et) = enums_table.as_table() {
                let pairs = et.iter_all();
                for (_, v) in pairs {
                    if let Some(enum_t) = v.as_table() {
                        let name_key =
                            state.create_string("name").map_err(|_| "oom".to_string())?;
                        if let Some(name_val) = enum_t.raw_get(&name_key) {
                            if let Some(name) = name_val.as_str() {
                                descriptor.enums.insert(name.to_string(), ());
                            }
                        }
                    }
                }
            }
        }

        // Parse structs
        let structs_key = state
            .create_string("structs")
            .map_err(|_| "oom".to_string())?;
        if let Some(structs_table) = state.raw_get(&arg, &structs_key) {
            if let Some(st) = structs_table.as_table() {
                let pairs = st.iter_all();
                for (_, v) in pairs {
                    if !v.is_table() {
                        continue;
                    }
                    let struct_t = v.as_table().unwrap();

                    let name_key =
                        state.create_string("name").map_err(|_| "oom".to_string())?;
                    let struct_name = match struct_t.raw_get(&name_key).and_then(|v| v.as_str().map(|s| s.to_string())) {
                        Some(n) => n,
                        None => continue,
                    };

                    let mut struct_def = ThriftStruct::new(struct_name);

                    // Parse fields
                    let fields_key =
                        state.create_string("fields").map_err(|_| "oom".to_string())?;
                    if let Some(fields_table) = struct_t.raw_get(&fields_key) {
                        if let Some(ft) = fields_table.as_table() {
                            let field_pairs = ft.iter_all();
                            for (_, fv) in field_pairs {
                                if !fv.is_table() {
                                    continue;
                                }
                                let fv_t = fv.as_table().unwrap();

                                let id_key = state
                                    .create_string("id")
                                    .map_err(|_| "oom".to_string())?;
                                let field_id = fv_t
                                    .raw_get(&id_key)
                                    .and_then(|v| v.as_integer())
                                    .unwrap_or(-1) as i16;

                                let fname_key = state
                                    .create_string("name")
                                    .map_err(|_| "oom".to_string())?;
                                let field_name = match fv_t
                                    .raw_get(&fname_key)
                                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                                {
                                    Some(n) => n,
                                    None => continue,
                                };

                                let ft_key = state
                                    .create_string("fieldType")
                                    .map_err(|_| "oom".to_string())?;
                                let ft_val = match fv_t.raw_get(&ft_key) {
                                    Some(v) if v.is_table() => v,
                                    _ => continue,
                                };

                                let (ttype, type_name) =
                                    load_field(state, ft_val, &mut descriptor)?;

                                if !field_name.is_empty() && field_id >= 0 {
                                    struct_def.all_fields.push(ThriftField {
                                        type_: ttype,
                                        id: field_id,
                                        name: field_name,
                                        type_name,
                                        struct_type: std::cell::Cell::new(None),
                                    });
                                }
                            }
                        }
                    }

                    struct_def.init();
                    descriptor.add_struct(struct_def);
                }
            }
        }

        set_global_descriptor(Box::new(descriptor));
        Ok(())
    })();

    match result {
        Ok(()) => {
            state.push_value(LuaValue::boolean(true))?;
            Ok(1)
        }
        Err(e) => Err(state.error(format!("load_thrift error: {}", e))),
    }
}

fn get_thread_encode_buffer() -> &'static mut Buffer {
    thread_local! {
        static ENCODE_BUF: std::cell::UnsafeCell<Buffer> = std::cell::UnsafeCell::new(Buffer::with_capacity(64 * 1024));
    }
    ENCODE_BUF.with(|cell| unsafe { &mut *cell.get() })
}

fn thrift_encode(state: &mut LuaState) -> LuaResult<usize> {
    let struct_name = lua_check_str(state, 1)?;
    let table_val = state
        .get_arg(2)
        .ok_or_else(|| state.error("thrift_encode: expected table argument #2".to_string()))?;
    if !table_val.is_table() {
        return Err(state.error("thrift_encode: argument #2 must be a table".to_string()));
    }

    let protocol = ThriftProtocol::new();
    let struct_type = protocol
        .descriptor
        .find_struct(struct_name)
        .ok_or_else(|| state.error(format!("thrift_encode: struct '{}' not found", struct_name)))?;

    let buf = get_thread_encode_buffer();
    buf.clear();
    protocol
        .encode_struct(state, table_val, buf, struct_type, 0)
        .map_err(|e| state.error(format!("thrift_encode error: {}", e)))?;

    let result = state.create_bytes(buf.data())?;
    state.push_value(result)?;
    Ok(1)
}

fn thrift_decode(state: &mut LuaState) -> LuaResult<usize> {
    let struct_name = lua_check_str(state, 1)?;
    let data = lua_check_bytes(state, 2)?;

    let protocol = ThriftProtocol::new();
    let struct_type = protocol
        .descriptor
        .find_struct(struct_name)
        .ok_or_else(|| state.error(format!("thrift_decode: struct '{}' not found", struct_name)))?;

    let mut stream = StreamReader::new(data);
    let result = protocol.decode_struct(state, &mut stream, struct_type, 0)?;
    state.push_value(result)?;
    Ok(1)
}

fn thrift_structs(state: &mut LuaState) -> LuaResult<usize> {
    let descriptor = get_global_descriptor();
    let table = state.create_table(0, descriptor.structs_by_name.len())?;
    for (name, _) in &descriptor.structs_by_name {
        let k = state.create_string(name)?;
        let v = state.create_string(name)?;
        state.raw_set(&table, k, v);
    }
    state.push_value(table)?;
    Ok(1)
}

pub fn register_thrift() -> luars::LibraryModule {
    luars::lua_module!("thrift", {
        "load" => load_thrift,
        "structs" => thrift_structs,
        "encode" => thrift_encode,
        "decode" => thrift_decode,
    })
}
