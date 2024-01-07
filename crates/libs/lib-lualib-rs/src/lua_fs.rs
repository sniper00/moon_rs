use lib_lua::{ffi, ffi::luaL_Reg};
use std::ffi::c_int;
use std::{
    env, fs,
    path::{Path, PathBuf},
};

use lib_core::{
    c_str,
    laux::{self},
    lreg, lreg_null,
};

fn listdir(state: *mut ffi::lua_State, path: &Path, idx: &mut i32, ext: &str) {
    if let Some(strpath) = path.to_str() {
        if !ext.is_empty() {
            if strpath.ends_with(ext) {
                laux::push_str(state, strpath);
                *idx += 1;
                unsafe {
                    ffi::lua_rawseti(state, -2, *idx as ffi::lua_Integer);
                }
            }
        } else {
            laux::push_str(state, strpath);
            *idx += 1;
            unsafe {
                ffi::lua_rawseti(state, -2, *idx as ffi::lua_Integer);
            }
        }
    }
}

extern "C-unwind" fn lfs_listdir(state: *mut ffi::lua_State) -> c_int {
    let path = laux::check_str(state, 1);
    let ext;
    unsafe {
        if ffi::lua_isstring(state, 2) != 0 {
            ext = laux::check_str(state, 2);
        } else {
            ext = "";
        }
    }

    match fs::read_dir(path) {
        Ok(dir) => {
            unsafe {
                ffi::lua_createtable(state, 16, 0);
            }

            let mut idx = 0;
            for entry in dir.flatten() {
                if let Ok(path) = fs::canonicalize(entry.path()) {
                    listdir(state, &path, &mut idx, ext);
                }
            }
            1
        }
        Err(err) => unsafe {
            ffi::lua_pushboolean(state, 0);
            laux::push_str(state, format!("listdir '{}' error: {}", path, err).as_str());
            2
        },
    }
}

extern "C-unwind" fn lfs_mkdir(state: *mut ffi::lua_State) -> c_int {
    let path = laux::check_str(state, 1);
    if fs::create_dir_all(path).is_ok() {
        unsafe {
            ffi::lua_pushboolean(state, 1);
            1
        }
    } else {
        unsafe {
            ffi::lua_pushboolean(state, 0);
            laux::push_str(state, format!("mkdir '{}' error", path).as_str());
            2
        }
    }
}

extern "C-unwind" fn lfs_exists(state: *mut ffi::lua_State) -> c_int {
    let path = laux::check_str(state, 1);
    if fs::metadata(path).is_ok() {
        unsafe {
            ffi::lua_pushboolean(state, 1);
            1
        }
    } else {
        unsafe {
            ffi::lua_pushboolean(state, 0);
            1
        }
    }
}

extern "C-unwind" fn lfs_isdir(state: *mut ffi::lua_State) -> c_int {
    let path = laux::check_str(state, 1);
    if let Ok(meta) = fs::metadata(path) {
        unsafe {
            if meta.is_dir() {
                ffi::lua_pushboolean(state, 1);
            } else {
                ffi::lua_pushboolean(state, 0);
            }
            1
        }
    } else {
        unsafe {
            ffi::lua_pushboolean(state, 0);
            1
        }
    }
}

extern "C-unwind" fn lfs_split(state: *mut ffi::lua_State) -> c_int {
    let path = laux::check_str(state, 1);
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        laux::push_str(state, parent.as_os_str().to_string_lossy().as_ref());
    } else {
        unsafe { ffi::lua_pushnil(state) }
    }

    if let Some(name) = path.file_name() {
        laux::push_str(state, name.to_string_lossy().as_ref());
    } else {
        unsafe { ffi::lua_pushnil(state) }
    }

    if let Some(ext) = path.extension() {
        laux::push_str(state, ext.to_string_lossy().as_ref());
    } else {
        unsafe { ffi::lua_pushnil(state) }
    }

    3
}

extern "C-unwind" fn lfs_ext(state: *mut ffi::lua_State) -> c_int {
    let path = laux::check_str(state, 1);
    let path = Path::new(path);

    if let Some(ext) = path.extension() {
        laux::push_str(state, ext.to_string_lossy().as_ref());
    } else {
        unsafe {
            ffi::lua_pushnil(state);
        }
    }

    1
}

extern "C-unwind" fn lfs_stem(state: *mut ffi::lua_State) -> c_int {
    let path = laux::check_str(state, 1);
    let path = Path::new(path);

    if let Some(name) = path.file_stem() {
        laux::push_str(state, name.to_string_lossy().as_ref());
    } else {
        unsafe {
            ffi::lua_pushnil(state);
        }
    }

    1
}

extern "C-unwind" fn lfs_join(state: *mut ffi::lua_State) -> c_int {
    let mut path = PathBuf::new();

    let top;
    unsafe {
        top = ffi::lua_gettop(state);
    }

    for i in 1..=top {
        let s = laux::check_str(state, i);
        path.push(Path::new(s));
    }

    laux::push_str(state, path.to_string_lossy().as_ref());

    1
}

extern "C-unwind" fn lfs_pwd(state: *mut ffi::lua_State) -> c_int {
    let current_dir = env::current_dir();
    if let Ok(dir) = current_dir {
        laux::push_str(state, dir.to_string_lossy().as_ref());
    } else {
        unsafe { ffi::lua_pushnil(state) }
    }

    1
}

extern "C-unwind" fn lfs_remove(state: *mut ffi::lua_State) -> c_int {
    let path = laux::check_str(state, 1);
    let path = Path::new(path);
    if path.is_dir() {
        if fs::remove_dir_all(path).is_ok() {
            unsafe {
                ffi::lua_pushboolean(state, 1);
                1
            }
        } else {
            unsafe {
                ffi::lua_pushboolean(state, 0);
                laux::push_str(state, format!("remove '{:?}' error", path).as_str());
                2
            }
        }
    } else if fs::remove_file(path).is_ok() {
        unsafe {
            ffi::lua_pushboolean(state, 1);
            1
        }
    } else {
        unsafe {
            ffi::lua_pushboolean(state, 0);
            laux::push_str(state, format!("remove '{:?}' error", path).as_str());
            2
        }
    }
}

pub unsafe extern "C-unwind" fn luaopen_fs(state: *mut ffi::lua_State) -> c_int {
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
        lreg_null!(),
    ];

    ffi::lua_createtable(state, 0, l.len() as c_int);
    ffi::luaL_setfuncs(state, l.as_ptr(), 0);

    1
}
