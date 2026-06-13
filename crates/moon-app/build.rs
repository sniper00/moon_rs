use std::path::PathBuf;

fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    match target_os.as_str() {
        "linux" | "android" => {
            println!("cargo:rustc-link-arg-bins=-rdynamic");
        }
        "macos" | "ios" => {
            println!("cargo:rustc-link-arg-bins=-Wl,-export_dynamic");
        }
        "windows" => {
            // Create an import library (moon_rs.lib) so cdylib extensions can
            // link against moon_rs.exe and import all Lua C API symbols at
            // load time. A temporary .def is generated solely for lib.exe;
            // the EXE exports are handled by LUA_BUILD_AS_DLL in moon-base.
            let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
            let def_path = out_dir.join("_lua_exports.def");
            // target/<profile> = out_dir/../../..
            let target_dir = out_dir
                .parent()
                .and_then(|p| p.parent())
                .and_then(|p| p.parent())
                .unwrap_or(&out_dir);

            let lua_symbols: &[&str] = &[
                "lua_newstate",
                "lua_close",
                "lua_newthread",
                "lua_closethread",
                "lua_atpanic",
                "lua_version",
                "lua_absindex",
                "lua_gettop",
                "lua_settop",
                "lua_pushvalue",
                "lua_rotate",
                "lua_copy",
                "lua_checkstack",
                "lua_xmove",
                "lua_isnumber",
                "lua_isstring",
                "lua_iscfunction",
                "lua_isinteger",
                "lua_isuserdata",
                "lua_type",
                "lua_typename",
                "lua_tonumberx",
                "lua_tointegerx",
                "lua_toboolean",
                "lua_tolstring",
                "lua_rawlen",
                "lua_tocfunction",
                "lua_touserdata",
                "lua_tothread",
                "lua_topointer",
                "lua_arith",
                "lua_rawequal",
                "lua_compare",
                "lua_pushnil",
                "lua_pushnumber",
                "lua_pushinteger",
                "lua_pushlstring",
                "lua_pushexternalstring",
                "lua_pushstring",
                "lua_pushvfstring",
                "lua_pushfstring",
                "lua_pushcclosure",
                "lua_pushboolean",
                "lua_pushlightuserdata",
                "lua_pushthread",
                "lua_clonefunction",
                "lua_sharefunction",
                "lua_sharestring",
                "lua_clonetable",
                "lua_getglobal",
                "lua_gettable",
                "lua_getfield",
                "lua_geti",
                "lua_rawget",
                "lua_rawgeti",
                "lua_rawgetp",
                "lua_createtable",
                "lua_newuserdatauv",
                "lua_getmetatable",
                "lua_getiuservalue",
                "lua_setglobal",
                "lua_settable",
                "lua_setfield",
                "lua_seti",
                "lua_rawset",
                "lua_rawseti",
                "lua_rawsetp",
                "lua_setmetatable",
                "lua_setiuservalue",
                "lua_callk",
                "lua_pcallk",
                "lua_load",
                "lua_dump",
                "lua_yieldk",
                "lua_resume",
                "lua_status",
                "lua_isyieldable",
                "lua_setwarnf",
                "lua_warning",
                "lua_gc",
                "lua_error",
                "lua_next",
                "lua_concat",
                "lua_len",
                "lua_numbertocstring",
                "lua_stringtonumber",
                "lua_getallocf",
                "lua_setallocf",
                "lua_toclose",
                "lua_closeslot",
                "lua_getstack",
                "lua_getinfo",
                "lua_getlocal",
                "lua_setlocal",
                "lua_getupvalue",
                "lua_setupvalue",
                "lua_upvalueid",
                "lua_upvaluejoin",
                "lua_sethook",
                "lua_gethook",
                "lua_gethookmask",
                "lua_gethookcount",
                "luaL_checkversion_",
                "luaL_getmetafield",
                "luaL_callmeta",
                "luaL_tolstring",
                "luaL_argerror",
                "luaL_typeerror",
                "luaL_checklstring",
                "luaL_optlstring",
                "luaL_checknumber",
                "luaL_optnumber",
                "luaL_checkinteger",
                "luaL_optinteger",
                "luaL_checkstack",
                "luaL_checktype",
                "luaL_checkany",
                "luaL_newmetatable",
                "luaL_setmetatable",
                "luaL_testudata",
                "luaL_checkudata",
                "luaL_where",
                "luaL_error",
                "luaL_checkoption",
                "luaL_fileresult",
                "luaL_execresult",
                "luaL_alloc",
                "luaL_ref",
                "luaL_unref",
                "luaL_loadfilex",
                "luaL_loadfilex_",
                "luaL_loadbufferx",
                "luaL_loadstring",
                "luaL_newstate",
                "luaL_makeseed",
                "luaL_len",
                "luaL_addgsub",
                "luaL_gsub",
                "luaL_setfuncs",
                "luaL_getsubtable",
                "luaL_traceback",
                "luaL_requiref",
                "luaL_buffinit",
                "luaL_prepbuffsize",
                "luaL_addlstring",
                "luaL_addstring",
                "luaL_addvalue",
                "luaL_pushresult",
                "luaL_pushresultsize",
                "luaL_buffinitsize",
            ];

            // Write .def, create import lib, then remove the .def.
            let mut def = String::from("LIBRARY moon_rs.exe\nEXPORTS\n");
            for sym in lua_symbols {
                def.push_str(sym);
                def.push('\n');
            }
            std::fs::write(&def_path, &def).expect("write _lua_exports.def");

            if let Ok(compiler) = cc::Build::new().try_get_compiler() {
                let lib_exe = compiler.path().parent().unwrap().join("lib.exe");
                if lib_exe.exists() {
                    let lib_path = target_dir.join("moon_rs.lib");
                    let _ = std::process::Command::new(&lib_exe)
                        .arg(format!("/def:{}", def_path.display()))
                        .arg("/machine:x64")
                        .arg(format!("/out:{}", lib_path.display()))
                        .status();
                }
            }

            let _ = std::fs::remove_file(&def_path);
        }
        _ => {}
    }
}
