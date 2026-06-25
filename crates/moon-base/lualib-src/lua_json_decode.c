#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
#include "lctype.c"

#include "yyjson/yyjson.h"

#include <stdint.h>

#define nullptr NULL

/* Mirror of the Rust `JsonOptions` struct (crates/moon-runtime/src/modules/
 * lua_json.rs). It is `#[repr(C)]` there and stored in-place inside the shared
 * `json_options` userdata upvalue, so the field order/types MUST match. */
typedef struct
{
    uint8_t empty_as_array;
    uint8_t enable_number_key;
    uint8_t enable_sparse_array;
    uint8_t has_metatfield;
    size_t  concat_buffer_size;
} json_options;

/* Fallback used only if the upvalue is missing; mirrors the defaults set in
 * `luaopen_json`. */
static const json_options DEFAULT_JSON_OPTIONS = { 1, 1, 0, 1, 512 };

/* Tag the table on the stack top with the `__object` / `__array`
 * marker metatable, matching Rust's `set_json_metatable`. `luaL_newmetatable`
 * keys by name in the registry, so Rust and C share the exact same metatable. */
static void set_json_metatable(lua_State* L, const char* meta_key)
{
    if (luaL_newmetatable(L, meta_key) != 0)
    {
        lua_pushboolean(L, 1);
        lua_setfield(L, -2, meta_key);
    }
    lua_setmetatable(L, -2);
}

#define MAXBY10		((lua_Unsigned)LUA_MAXINTEGER / 10)
#define MAXLASTD	((int)(LUA_MAXINTEGER % 10))

static int isneg (const char **s) {
  if (**s == '-') { (*s)++; return 1; }
  else if (**s == '+') (*s)++;
  return 0;
}

static const char *l_str2int(const char *s, size_t len, lua_Integer *result) {
    if(len == 0)
        return nullptr;

    lua_Unsigned a = 0;
    int empty = 1;
    const char* b = s;
    int neg = isneg(&b);
    int i = (int)(b - s);
    for(;i<(int)len;++i){
        if(!lisdigit(cast_uchar(s[i]))){
            return nullptr;
        }
        int d = s[i] - '0';
        if (a >= MAXBY10 && (a > MAXBY10 || d > MAXLASTD + neg))  /* overflow? */
            return nullptr;  /* do not accept it (as integer) */
        a = a * 10 + d;
        empty = 0;
    }
    if(empty)
        return nullptr;
    *result = l_castU2S((neg) ? 0u - a : a);
    return s+i;
}

static void decode_one(lua_State* L, yyjson_val* value, const json_options* opt)
{
    yyjson_type type = yyjson_get_type(value);
    switch (type)
    {
    case YYJSON_TYPE_ARR:
    {
        luaL_checkstack(L, 6, "json.decode.array");
        lua_createtable(L, (int)yyjson_arr_size(value), 0);
        lua_Integer pos = 1;
        yyjson_arr_iter iter;
        yyjson_arr_iter_init(value, &iter);
        while (nullptr != (value = yyjson_arr_iter_next(&iter)))
        {
            decode_one(L, value, opt);
            lua_rawseti(L, -2, pos++);
        }
        if (opt->has_metatfield)
            set_json_metatable(L, "__array");
        break;
    }
    case YYJSON_TYPE_OBJ:
    {
        luaL_checkstack(L, 6, "json.decode.object");
        lua_createtable(L, 0, (int)yyjson_obj_size(value));

        yyjson_val* key, * val;
        yyjson_obj_iter iter;
        yyjson_obj_iter_init(value, &iter);
        while (nullptr != (key = yyjson_obj_iter_next(&iter)))
        {
            val = yyjson_obj_iter_get_val(key);

            const char* key_str = unsafe_yyjson_get_str(key);
            size_t key_len = unsafe_yyjson_get_len(key);
            if (key_len > 0)
            {
                char c = key_str[0];
                if (opt->enable_number_key && (c == '-' || (c >= '0' && c <= '9')))
                {
                    const char* last = key_str + key_len;
                    lua_Integer v = 0;
                    if(l_str2int(key_str, unsafe_yyjson_get_len(key), &v) == last){
                        lua_pushinteger(L, v);
                    }
                    else
                        lua_pushlstring(L, key_str, key_len);
                }
                else
                {
                    lua_pushlstring(L, key_str, key_len);
                }
                decode_one(L, val, opt);
                lua_rawset(L, -3);
            }
        }
        if (opt->has_metatfield)
            set_json_metatable(L, "__object");
        break;
    }
    case YYJSON_TYPE_NUM:
    {
        yyjson_subtype subtype = yyjson_get_subtype(value);
        switch (subtype)
        {
        case YYJSON_SUBTYPE_UINT:
        {
            uint64_t uv = unsafe_yyjson_get_uint(value);
            if (uv > (uint64_t)INT64_MAX) {
                lua_pushnumber(L, (lua_Number)uv);
            } else {
                lua_pushinteger(L, (int64_t)uv);
            }
            break;
        }
        case YYJSON_SUBTYPE_SINT:
        {
            lua_pushinteger(L, unsafe_yyjson_get_sint(value));
            break;
        }
        case YYJSON_SUBTYPE_REAL:
        {
            lua_pushnumber(L, unsafe_yyjson_get_real(value));
            break;
        }
        }
        break;
    }
    case YYJSON_TYPE_STR:
    {
        lua_pushlstring(L, unsafe_yyjson_get_str(value), unsafe_yyjson_get_len(value));
        break;
    }
    case YYJSON_TYPE_BOOL:
        lua_pushboolean(L, (yyjson_get_subtype(value) == YYJSON_SUBTYPE_TRUE) ? 1 : 0);
        break;
    case YYJSON_TYPE_NULL:
    {
        lua_pushlightuserdata(L, nullptr);
        break;
    }
    default:
        break;
    }
}

LUALIB_API int lua_json_decode(lua_State* L)
{
    size_t len = 0;
    const char* str = nullptr;
    if (lua_type(L, 1) == LUA_TSTRING)
    {
        str = luaL_checklstring(L, 1, &len);
    }
    else
    {
        luaL_checktype(L, 1, LUA_TLIGHTUSERDATA);
        str = (const char*)lua_touserdata(L, 1);
        len = luaL_checkinteger(L, 2);
    }

    if (nullptr == str || str[0] == '\0')
        return 0;

    lua_settop(L, 1);

    /* Shared options live in upvalue 1 (the `json_options` userdata) because all
     * json.* functions are registered via luaL_setfuncs(..., 1). Fall back to
     * defaults if this function was somehow registered without the upvalue. */
    const json_options* opt = (const json_options*)lua_touserdata(L, lua_upvalueindex(1));
    if (nullptr == opt)
        opt = &DEFAULT_JSON_OPTIONS;

    yyjson_read_err err;
    yyjson_doc* doc = yyjson_read_opts((char*)str, len, 0, nullptr, &err);
    if (nullptr == doc)
    {
        return luaL_error(L, "decode error: %s code: %d at position: %d\n", err.msg, (int)err.code, (int)err.pos);
    }
    decode_one(L, yyjson_doc_get_root(doc), opt);
    yyjson_doc_free(doc);
    return 1;
}
