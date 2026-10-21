use moon_lua_sys::laux::{FromLua, LuaNil, LuaStack, LuaState, LuaTable};
use moon_lua_sys::luaL_newlib;
use moon_lua_sys::{cstr, ffi, laux, lreg, lreg_null, lreg_try};
use std::ffi::c_int;
use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn listdir_push(state: LuaState, res: &LuaTable, idx: &mut usize, path: &Path, ext: Option<&str>) {
    if let Some(strpath) = path.to_str() {
        let matches = match ext {
            Some(ext) => strpath.ends_with(ext),
            None => true,
        };
        if matches {
            laux::lua_push(state, strpath);
            *idx += 1;
            res.rawseti(*idx);
        }
    }
}

fn lfs_listdir(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let path = crate::checked_str(lua, 1)?.to_owned();
    // Optional max recursion depth: 0 (or absent) means unlimited; 1 lists only
    // the immediate children. An optional extension/suffix filter may be passed
    // as the third argument.
    let max_depth: usize = FromLua::from_lua(lua, 2).ok().unwrap_or(0);
    let ext = lua.value(3).as_str().map(str::to_owned);

    // Surface an error for an unreadable root (matches prior behavior); deeper
    // unreadable subdirectories are skipped silently during the walk.
    if let Err(err) = fs::read_dir(&path) {
        return Err(format!("listdir '{}' error: {}", path, err));
    }

    let table = laux::LuaTable::new(state, 16, 0);
    let mut idx: usize = 0;

    // Iterative DFS so deep trees can't overflow the Rust stack, and only one
    // directory handle is open at a time. Entry paths are built directly from
    // the caller-supplied `path` (no per-entry canonicalize).
    let mut stack: Vec<(PathBuf, usize)> = vec![(PathBuf::from(&path), 1)];
    while let Some((dir, depth)) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let entry_path = entry.path();
            listdir_push(state, &table, &mut idx, &entry_path, ext.as_deref());
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
    Ok(1)
}

fn lfs_mkdir(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let path = crate::checked_str(lua, 1)?;
    if fs::create_dir_all(path).is_ok() {
        lua.push(true);
    } else {
        return Err(format!("mkdir '{}' error", path));
    }
    // Return the pushed boolean: returning 0 here would discard it, so the
    // documented `fs.mkdir(path) -> boolean` contract would yield `nil`.
    Ok(1)
}

fn lfs_exists(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let path = crate::checked_str(lua, 1)?;
    lua.push(fs::metadata(path).is_ok());
    Ok(1)
}

fn lfs_isdir(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let path = crate::checked_str(lua, 1)?;
    if let Ok(meta) = fs::metadata(path) {
        lua.push(meta.is_dir());
    } else {
        lua.push(false);
    }
    Ok(1)
}

fn lfs_split(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let (parent, name, ext) = {
        let path = Path::new(crate::checked_str(lua, 1)?);
        (
            path.parent()
                .map(|parent| parent.as_os_str().to_string_lossy().into_owned()),
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned()),
            path.extension()
                .map(|ext| ext.to_string_lossy().into_owned()),
        )
    };
    match parent {
        Some(parent) => lua.push(parent),
        None => lua.push(LuaNil),
    }
    match name {
        Some(name) => lua.push(name),
        None => lua.push(LuaNil),
    }
    match ext {
        Some(ext) => lua.push(ext),
        None => lua.push(LuaNil),
    }

    Ok(3)
}

fn lfs_ext_impl(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let extension = {
        let path = lua
            .value(1)
            .as_str()
            .ok_or_else(|| "bad argument #1 (valid UTF-8 string expected)".to_string())?;
        Path::new(path)
            .extension()
            .map(|ext| format!(".{}", ext.to_string_lossy()))
    };

    match extension {
        Some(ext) => lua.push(ext),
        None => lua.push(LuaNil),
    }

    Ok(1)
}

fn lfs_stem(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let stem = Path::new(crate::checked_str(lua, 1)?)
        .file_stem()
        .map(|name| name.to_string_lossy().into_owned());
    match stem {
        Some(stem) => lua.push(stem),
        None => lua.push(LuaNil),
    }

    Ok(1)
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

fn lfs_join(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let mut path = PathBuf::new();

    let top = lua.top();

    for i in 1..=top {
        let s = crate::checked_str(lua, i)?;
        path.push(Path::new(s));
    }

    // Normalize the joined result so callers can't accidentally end up with a
    // traversal path (e.g. `join(base, "../../etc/passwd")` still containing
    // `..`). This mirrors Go's `filepath.Join`, which cleans its output.
    let path = lexical_clean(&path);

    lua.push(path.to_string_lossy().into_owned());

    Ok(1)
}

fn lfs_pwd(lua: &mut LuaStack<'_>) -> c_int {
    let current_dir = env::current_dir();
    if let Ok(dir) = current_dir {
        lua.push(dir.to_string_lossy().into_owned());
    } else {
        lua.push(LuaNil);
    }

    1
}

fn lfs_abspath(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let path = crate::checked_str(lua, 1)?;
    if let Ok(abs) = fs::canonicalize(path) {
        lua.push(abs.to_string_lossy().into_owned());
    } else {
        lua.push(LuaNil);
    }

    Ok(1)
}

fn lfs_remove(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let path = Path::new(crate::checked_str(lua, 1)?);
    if path.is_dir() {
        if fs::remove_dir_all(path).is_ok() {
            lua.push(1);
            Ok(1)
        } else {
            Err(format!("remove '{:?}' error", path))
        }
    } else if fs::remove_file(path).is_ok() {
        lua.push(1);
        Ok(1)
    } else {
        Err(format!("remove '{:?}' error", path))
    }
}

pub unsafe extern "C-unwind" fn luaopen_fs(state: LuaState) -> c_int {
    let l = [
        lreg_try!("listdir", lfs_listdir),
        lreg_try!("mkdir", lfs_mkdir),
        lreg_try!("exists", lfs_exists),
        lreg_try!("isdir", lfs_isdir),
        lreg_try!("split", lfs_split),
        lreg_try!("ext", lfs_ext_impl),
        lreg_try!("stem", lfs_stem),
        lreg_try!("join", lfs_join),
        lreg!("pwd", lfs_pwd),
        lreg_try!("remove", lfs_remove),
        lreg_try!("abspath", lfs_abspath),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
