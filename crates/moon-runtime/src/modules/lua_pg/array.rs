//! Text-format PostgreSQL arrays. Lua tables use one-based indices and a null
//! lightuserdata sentinel, shared with json.null. Non-one array bounds are rejected.
use super::*;

const MAX_DIMENSIONS: usize = 6;

pub(super) fn element_oid(oid: i32) -> Option<i32> {
    Some(match oid {
        1000 => 16,   // bool
        1001 => 17,   // bytea (returned as PostgreSQL text, like scalar bytea)
        1005 => 21,   // int2
        1007 => 23,   // int4
        1016 => 20,   // int8
        1002 => 18,   // char
        1003 => 19,   // name
        1009 => 25,   // text
        1014 => 1042, // bpchar
        1015 => 1043, // varchar
        1021 => 700,  // float4
        1022 => 701,  // float8
        1231 => 1700, // numeric: preserve decimal text rather than round to f64
        199 => 114,   // json
        3807 => 3802, // jsonb
        1182 => 1082, // date
        1183 => 1083, // time
        1115 => 1114, // timestamp
        1185 => 1184, // timestamptz
        1270 => 1266, // timetz
        1187 => 1186, // interval
        2951 => 2950, // uuid
        _ => return None,
    })
}

pub(super) fn encode(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let top = laux::lua_top(state);
    laux::lua_checkstack(state, 32, cstr!("pg.array"))?;
    let mut buf = Vec::new();
    let result = encode_level(&mut buf, lua, 1, 0);
    laux::lua_settop(state, top);
    result?;
    laux::lua_push(state, buf.as_slice());
    Ok(1)
}

fn check_size(buf: &[u8]) -> Result<(), String> {
    if buf.len() > crate::LIMITS.max_network_read_bytes {
        Err("pg.array exceeds network byte limit".into())
    } else {
        Ok(())
    }
}

fn encode_level(
    buf: &mut Vec<u8>,
    lua: &mut LuaStack<'_>,
    index: i32,
    depth: usize,
) -> Result<Vec<usize>, String> {
    if depth >= MAX_DIMENSIONS {
        return Err("pg.array exceeds six dimensions (or contains a cycle)".into());
    }
    let state = lua.state();
    if lua.value(index).kind() != LuaType::Table {
        return Err("pg.array expects a dense sequence table".into());
    }
    let n = unsafe { ffi::lua_rawlen(state.as_ptr(), index) };
    if n > crate::LIMITS.max_network_read_bytes {
        return Err("pg.array has too many elements".into());
    }
    // rawlen alone cannot detect holes or additional map keys.
    let mut count = 0;
    laux::lua_pushnil(state);
    while unsafe { ffi::lua_next(state.as_ptr(), index) } != 0 {
        let key = lua.value(-2).as_integer();
        if !key.is_some_and(|k| k >= 1 && (k as u64) <= n as u64) {
            return Err("pg.array expects a dense sequence without map keys".into());
        }
        count += 1;
        laux::lua_pop(state, 1);
    }
    if count != n {
        return Err("pg.array contains holes; use pg.NULL for NULL elements".into());
    }
    buf.push(b'{');
    let mut child_shape = None;
    for i in 1..=n {
        if i > 1 {
            buf.push(b',');
        }
        unsafe { ffi::lua_rawgeti(state.as_ptr(), index, i as ffi::lua_Integer) };
        let child_index = laux::lua_top(state);
        let value = lua.value(child_index);
        let shape = if value.kind() == LuaType::Table {
            let shape = encode_level(buf, lua, child_index, depth + 1)?;
            if shape.contains(&0) {
                return Err("pg.array cannot contain empty subarrays".into());
            }
            shape
        } else {
            match value.kind() {
                LuaType::String => {
                    let bytes = value.as_bytes().unwrap_or_default();
                    if bytes.contains(&0) {
                        return Err("pg.array strings cannot contain NUL bytes".into());
                    }
                    if bytes.len() > crate::LIMITS.max_network_read_bytes {
                        return Err("pg.array string exceeds network byte limit".into());
                    }
                    buf.push(b'"');
                    for &byte in bytes {
                        if matches!(byte, b'"' | b'\\') {
                            buf.push(b'\\');
                        }
                        buf.push(byte);
                    }
                    buf.push(b'"');
                }
                LuaType::Integer => {
                    let mut tmp = [0; lexical_core::BUFFER_SIZE];
                    buf.extend_from_slice(lexical_core::write(
                        value.as_integer().unwrap(),
                        &mut tmp,
                    ));
                }
                LuaType::Number => {
                    use std::io::Write;
                    write!(buf, "{}", value.as_number().unwrap()).expect("Vec write");
                }
                LuaType::Boolean => buf.extend_from_slice(if value.as_bool().unwrap() {
                    b"true"
                } else {
                    b"false"
                }),
                LuaType::LightUserData
                    if value.as_light_userdata().is_some_and(|p| p.is_null()) =>
                {
                    buf.extend_from_slice(b"NULL");
                }
                _ => return Err(format!("unsupported pg.array element: {}", value.name())),
            }
            Vec::new()
        };
        laux::lua_pop(state, 1);
        if child_shape
            .as_ref()
            .is_some_and(|expected| expected != &shape)
        {
            return Err("pg.array requires rectangular arrays without mixed nesting".into());
        }
        child_shape = Some(shape);
        check_size(buf)?;
    }
    buf.push(b'}');
    check_size(buf)?;
    let mut shape = vec![n];
    shape.extend(child_shape.unwrap_or_default());
    Ok(shape)
}

pub(super) fn decode(state: LuaState, oid: i32, input: &[u8]) -> Result<(), String> {
    let top = laux::lua_top(state);
    laux::lua_checkstack(state, 32, cstr!("pg array decode"))?;
    let result = (|| {
        check_size(input)?;
        let mut parser = Parser {
            state,
            oid,
            input,
            pos: 0,
        };
        parser.whitespace();
        let mut dimensions = Vec::new();
        while parser.peek() == Some(b'[') {
            if dimensions.len() >= MAX_DIMENSIONS {
                return Err("PG array exceeds six dimensions".into());
            }
            parser.pos += 1;
            let lower = parser.bound(b':')?;
            let upper = parser.bound(b']')?;
            if lower != 1 {
                return Err("PG arrays with non-one lower bounds are unsupported".into());
            }
            let len = upper
                .checked_sub(lower)
                .and_then(|n| n.checked_add(1))
                .and_then(|n| usize::try_from(n).ok())
                .ok_or("invalid PG array bounds")?;
            dimensions.push(len);
        }
        if !dimensions.is_empty() {
            parser.expect(b'=')?;
        }
        let shape = parser.level(0)?;
        parser.whitespace();
        if parser.pos != input.len() {
            return Err("trailing bytes in PG array".into());
        }
        if !dimensions.is_empty() && dimensions != shape {
            return Err("PG array dimensions do not match contents".into());
        }
        Ok(())
    })();
    if result.is_err() {
        laux::lua_settop(state, top);
    }
    result
}

struct Parser<'a> {
    state: LuaState,
    oid: i32,
    input: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn whitespace(&mut self) {
        while self.peek().is_some_and(|b| b.is_ascii_whitespace()) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), String> {
        if self.peek() != Some(byte) {
            return Err("malformed PG array".into());
        }
        self.pos += 1;
        Ok(())
    }

    fn bound(&mut self, delimiter: u8) -> Result<i64, String> {
        let start = self.pos;
        while self.peek().is_some_and(|b| b != delimiter) {
            self.pos += 1;
        }
        let value = std::str::from_utf8(&self.input[start..self.pos])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or("invalid PG array bound")?;
        self.expect(delimiter)?;
        Ok(value)
    }

    fn level(&mut self, depth: usize) -> Result<Vec<usize>, String> {
        if depth >= MAX_DIMENSIONS {
            return Err("PG array exceeds six dimensions".into());
        }
        self.whitespace();
        self.expect(b'{')?;
        let table = LuaTable::new(self.state, 0, 0);
        self.whitespace();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(vec![0]);
        }
        let mut count = 0;
        let mut child_shape = None;
        loop {
            self.whitespace();
            let shape = if self.peek() == Some(b'{') {
                let shape = self.level(depth + 1)?;
                if shape.contains(&0) {
                    return Err("PG array contains empty subarrays".into());
                }
                shape
            } else {
                let (value, null) = self.scalar()?;
                if null {
                    laux::lua_pushlightuserdata(self.state, std::ptr::null_mut());
                } else if self.oid == 16 {
                    laux::lua_push(
                        self.state,
                        value.as_ref() == b"t" || value.eq_ignore_ascii_case(b"true"),
                    );
                } else if self.oid == 1700 {
                    // Do not round arbitrary precision numerics through f64.
                    laux::lua_push(self.state, value.as_ref());
                } else {
                    convert_scalar(self.state, self.oid, &value);
                }
                Vec::new()
            };
            if child_shape
                .as_ref()
                .is_some_and(|expected| expected != &shape)
            {
                return Err("PG array is not rectangular".into());
            }
            child_shape = Some(shape);
            count += 1;
            table.rawseti(count);
            self.whitespace();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err("unterminated PG array".into()),
            }
        }
        let mut shape = vec![count];
        shape.extend(child_shape.unwrap_or_default());
        Ok(shape)
    }

    fn scalar(&mut self) -> Result<(Cow<'a, [u8]>, bool), String> {
        let quoted = self.peek() == Some(b'"');
        if quoted {
            self.pos += 1;
        }
        let start = self.pos;
        // Borrow the wire bytes unless an escape actually needs removing.
        let mut decoded: Option<Vec<u8>> = None;
        let mut significant = 0;
        let end;
        loop {
            let byte = self.peek().ok_or("unterminated PG array element")?;
            if quoted && byte == b'"' {
                end = self.pos;
                self.pos += 1;
                break;
            }
            if !quoted && matches!(byte, b',' | b'}') {
                end = self.pos;
                break;
            }
            if !quoted && matches!(byte, b'{' | b'"') {
                return Err("malformed PG array element".into());
            }
            self.pos += 1;
            if byte == b'\\' {
                let next = self.peek().ok_or("truncated PG array escape")?;
                let value = decoded.get_or_insert_with(|| self.input[start..self.pos - 1].to_vec());
                value.push(next);
                self.pos += 1;
                significant = value.len();
            } else {
                if let Some(value) = &mut decoded {
                    value.push(byte);
                }
                if !byte.is_ascii_whitespace() {
                    significant = decoded.as_ref().map_or(self.pos - start, Vec::len);
                }
            }
        }
        let escaped = decoded.is_some();
        let value = match decoded {
            Some(mut value) => {
                if !quoted {
                    value.truncate(significant);
                }
                Cow::Owned(value)
            }
            None => {
                Cow::Borrowed(&self.input[start..if quoted { end } else { start + significant }])
            }
        };
        if !quoted && value.is_empty() {
            return Err("empty unquoted PG array element".into());
        }
        let null = !quoted && !escaped && value.eq_ignore_ascii_case(b"NULL");
        Ok((value, null))
    }
}
