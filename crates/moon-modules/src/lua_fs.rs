use moon_lua::laux::{LuaState, LuaTable};
use moon_lua::luaL_newlib;
use moon_lua::{cstr, ffi, laux, lreg, lreg_null};
use std::ffi::c_int;
use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn listdir_push(res: &LuaTable, idx: &mut usize, path: &Path, ext: Option<&str>) {
    if let Some(strpath) = path.to_str() {
        let matches = match ext {
            Some(ext) => strpath.ends_with(ext),
            None => true,
        };
        if matches {
            laux::lua_push(res.lua_state(), strpath);
            *idx += 1;
            res.rawseti(*idx);
        }
    }
}

extern "C-unwind" fn lfs_listdir(state: LuaState) -> c_int {
    let path = unsafe { laux::lua_check_str(state, 1) };
    // Optional max recursion depth: 0 (or absent) means unlimited; 1 lists only
    // the immediate children. An optional extension/suffix filter may be passed
    // as the third argument.
    let max_depth: usize = laux::lua_opt(state, 2).unwrap_or(0);
    let ext = unsafe { laux::lua_opt_str(state, 3) };

    // Surface an error for an unreadable root (matches prior behavior); deeper
    // unreadable subdirectories are skipped silently during the walk.
    if let Err(err) = fs::read_dir(path) {
        laux::lua_error(state, format!("listdir '{}' error: {}", path, err));
    }

    let table = laux::LuaTable::new(state, 16, 0);
    let mut idx: usize = 0;

    // Iterative DFS so deep trees can't overflow the Rust stack, and only one
    // directory handle is open at a time. Entry paths are built directly from
    // the caller-supplied `path` (no per-entry canonicalize).
    let mut stack: Vec<(PathBuf, usize)> = vec![(PathBuf::from(path), 1)];
    while let Some((dir, depth)) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let entry_path = entry.path();
            listdir_push(&table, &mut idx, &entry_path, ext);
            // Only recurse into *real* directories, never symlinks: a symlink
            // such as `child -> ..` (or `child -> /`) would otherwise let the
            // walk loop forever / escape the tree. `DirEntry::file_type` does
            // not follow symlinks, so checking `is_symlink()` here is an
            // explicit, refactor-proof guard against that cycle.
            let recurse_into_dir = entry
                .file_type()
                .map(|t| t.is_dir() && !t.is_symlink())
                .unwrap_or(false);
            if recurse_into_dir && (max_depth == 0 || depth < max_depth) {
                stack.push((entry_path, depth + 1));
            }
        }
    }
    1
}

extern "C-unwind" fn lfs_mkdir(state: LuaState) -> c_int {
    let path = unsafe { laux::lua_check_str(state, 1) };
    if fs::create_dir_all(path).is_ok() {
        laux::lua_push(state, true);
    } else {
        laux::lua_error(state, format!("mkdir '{}' error", path));
    }
    // Return the pushed boolean: returning 0 here would discard it, so the
    // documented `fs.mkdir(path) -> boolean` contract would yield `nil`.
    1
}

extern "C-unwind" fn lfs_exists(state: LuaState) -> c_int {
    let path = unsafe { laux::lua_check_str(state, 1) };
    laux::lua_push(state, fs::metadata(path).is_ok());
    1
}

extern "C-unwind" fn lfs_isdir(state: LuaState) -> c_int {
    let path = unsafe { laux::lua_check_str(state, 1) };
    if let Ok(meta) = fs::metadata(path) {
        laux::lua_push(state, meta.is_dir());
    } else {
        laux::lua_push(state, false);
    }
    1
}

extern "C-unwind" fn lfs_split(state: LuaState) -> c_int {
    let path = unsafe { laux::lua_check_str(state, 1) };
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
    let path = unsafe { laux::lua_check_str(state, 1) };
    let path = Path::new(path);

    if let Some(ext) = path.extension() {
        laux::lua_push(state, ext.to_string_lossy().as_ref());
    } else {
        laux::lua_pushnil(state);
    }

    1
}

extern "C-unwind" fn lfs_stem(state: LuaState) -> c_int {
    let path = unsafe { laux::lua_check_str(state, 1) };
    let path = Path::new(path);

    if let Some(name) = path.file_stem() {
        laux::lua_push(state, name.to_string_lossy().as_ref());
    } else {
        laux::lua_pushnil(state);
    }

    1
}

/// Lexically clean a path (no filesystem access), like Go's `filepath.Clean`:
/// drop `.` components and resolve `..` against a preceding *normal* component.
/// A `..` that has nothing to pop is preserved for a relative path (a leading
/// `..` can't be resolved without a base) and dropped at the root (`/.. == /`).
fn lexical_clean(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Prefix(_) | Component::RootDir => out.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(out.components().next_back(), Some(Component::Normal(_))) {
                    out.pop();
                } else if !out.has_root() {
                    out.push("..");
                }
            }
            Component::Normal(c) => out.push(c),
        }
    }
    if out.as_os_str().is_empty() {
        out.push(".");
    }
    out
}

extern "C-unwind" fn lfs_join(state: LuaState) -> c_int {
    let mut path = PathBuf::new();

    let top = laux::lua_top(state);

    for i in 1..=top {
        let s = unsafe { laux::lua_check_str(state, i) };
        path.push(Path::new(s));
    }

    // Normalize the joined result so callers can't accidentally end up with a
    // traversal path (e.g. `join(base, "../../etc/passwd")` still containing
    // `..`). This mirrors Go's `filepath.Join`, which cleans its output.
    let path = lexical_clean(&path);

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
    let path = unsafe { laux::lua_check_str(state, 1) };
    if let Ok(abs) = fs::canonicalize(path) {
        laux::lua_push(state, abs.to_string_lossy().as_ref());
    } else {
        laux::lua_pushnil(state);
    }

    1
}

extern "C-unwind" fn lfs_remove(state: LuaState) -> c_int {
    let path = unsafe { laux::lua_check_str(state, 1) };
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
