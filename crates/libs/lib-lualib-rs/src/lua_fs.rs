use lib_lua::laux::{LuaState, LuaTable};
use lib_lua::luaL_newlib;
use lib_lua::{laux, ffi, cstr, lreg, lreg_null};
use std::ffi::c_int;
use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn listdir(res: &LuaTable, path: &Path, idx: &mut usize, ext: Option<&str>) {
    if let Some(strpath) = path.to_str() {
        if let Some(ext) = ext {
            if strpath.ends_with(ext) {
                laux::lua_push(res.lua_state(), strpath);
                *idx += 1;
                res.rawseti(*idx);
            }
        } else {
            laux::lua_push(res.lua_state(), strpath);
            *idx += 1;
            res.rawseti(*idx);
        }
    }
}

extern "C-unwind" fn lfs_listdir(state: LuaState) -> c_int {
    let path: &str = laux::lua_get(state, 1);
    let ext = laux::lua_opt::<&str>(state, 2);
    
    match fs::read_dir(path) {
        Ok(dir) => {
            let table = laux::LuaTable::new(state, 16, 0);

            let mut idx: usize = 0;
            for entry in dir.flatten() {
                if let Ok(path) = fs::canonicalize(entry.path()) {
                    listdir(&table, &path, &mut idx, ext);
                }
            }
            1
        }
        Err(err) => {
            laux::lua_error(state, format!("listdir '{}' error: {}", path, err));
        },
    }
}

extern "C-unwind" fn lfs_mkdir(state: LuaState) -> c_int {
    let path = laux::lua_get::<&str>(state, 1);
    if fs::create_dir_all(path).is_ok() {
        laux::lua_push(state, true);
    } else {
        laux::lua_error(state, format!("mkdir '{}' error", path));
    }
    0
}

extern "C-unwind" fn lfs_exists(state: LuaState) -> c_int {
    let path = laux::lua_get::<&str>(state, 1);
    laux::lua_push(state, fs::metadata(path).is_ok());
    1
}

extern "C-unwind" fn lfs_isdir(state: LuaState) -> c_int {
    let path = laux::lua_get::<&str>(state, 1);
    if let Ok(meta) = fs::metadata(path) {
        laux::lua_push(state, meta.is_dir());
    } else {
        laux::lua_push(state, false);
    }
    1
}

extern "C-unwind" fn lfs_split(state: LuaState) -> c_int {
    let path = laux::lua_get::<&str>(state, 1);
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        laux::lua_push(state, parent.as_os_str().to_string_lossy().as_ref());
    } else {
        laux::lua_pushnil(state);
    }

    if let Some(name) = path.file_name() {
        laux::lua_push(state, name.to_string_lossy().as_ref());
    } else {
        laux::lua_pushnil(state);
    }

    if let Some(ext) = path.extension() {
        laux::lua_push(state, ext.to_string_lossy().as_ref());
    } else {
        laux::lua_pushnil(state);
    }

    3
}

extern "C-unwind" fn lfs_ext(state: LuaState) -> c_int {
    let path = laux::lua_get::<&str>(state, 1);
    let path = Path::new(path);

    if let Some(ext) = path.extension() {
        laux::lua_push(state, ext.to_string_lossy().as_ref());
    } else {
        laux::lua_pushnil(state);
    }

    1
}

extern "C-unwind" fn lfs_stem(state: LuaState) -> c_int {
    let path = laux::lua_get::<&str>(state, 1);
    let path = Path::new(path);

    if let Some(name) = path.file_stem() {
        laux::lua_push(state, name.to_string_lossy().as_ref());
    } else {
        laux::lua_pushnil(state);
    }

    1
}

extern "C-unwind" fn lfs_join(state: LuaState) -> c_int {
    let mut path = PathBuf::new();

    let top = laux::lua_top(state);

    for i in 1..=top {
        let s = laux::lua_get::<&str>(state, i);
        path.push(Path::new(s));
    }

    laux::lua_push(state, path.to_string_lossy().as_ref());

    1
}

extern "C-unwind" fn lfs_pwd(state: LuaState) -> c_int {
    let current_dir = env::current_dir();
    if let Ok(dir) = current_dir {
        laux::lua_push(state, dir.to_string_lossy().as_ref());
    } else {
        laux::lua_pushnil(state);
    }

    1
}

extern "C-unwind" fn lfs_abspath(state: LuaState) -> c_int {
    let path = laux::lua_get::<&str>(state, 1);
    if let Ok(abs) = fs::canonicalize(path) {
        laux::lua_push(state, abs.to_string_lossy().as_ref());
    } else {
        laux::lua_pushnil(state);
    }

    1
}

extern "C-unwind" fn lfs_remove(state: LuaState) -> c_int {
    let path = laux::lua_get::<&str>(state, 1);
    let path = Path::new(path);
    if path.is_dir() {
        if fs::remove_dir_all(path).is_ok() {
            laux::lua_push(state, 1);
            1
        } else {
            laux::lua_error(state, format!("remove '{:?}' error", path));
        }
    } else if fs::remove_file(path).is_ok() {
        laux::lua_push(state, 1);
        1
    } else {
        laux::lua_error(state, format!("remove '{:?}' error", path));
    }
}

pub unsafe extern "C-unwind" fn luaopen_fs(state: LuaState) -> c_int {
    let l = [
        lreg!("listdir", lfs_listdir),
        lreg!("mkdir", lfs_mkdir),
        lreg!("exists", lfs_exists),
        lreg!("isdir", lfs_isdir),
        lreg!("split", lfs_split),
        lreg!("ext", lfs_ext),
        lreg!("stem", lfs_stem),
        lreg!("join", lfs_join),
        lreg!("pwd", lfs_pwd),
        lreg!("remove", lfs_remove),
        lreg!("abspath", lfs_abspath),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
