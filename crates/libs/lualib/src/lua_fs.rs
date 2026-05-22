use luars::{LuaResult, LuaState, LuaValue};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

use crate::{lua_check_str, lua_opt_str};

fn lfs_listdir(state: &mut LuaState) -> LuaResult<usize> {
    let path = lua_check_str(state, 1)?;
    let ext = lua_opt_str(state, 2);

    match fs::read_dir(path) {
        Ok(dir) => {
            let table = state.create_table(16, 0)?;
            let mut idx: i64 = 0;
            for entry in dir.flatten() {
                if let Ok(canon) = fs::canonicalize(entry.path())
                    && let Some(strpath) = canon.to_str() {
                        let should_add = match &ext {
                            Some(e) => strpath.ends_with(e),
                            None => true,
                        };
                        if should_add {
                            idx += 1;
                            let val = state.create_string(strpath)?;
                            state.raw_seti(&table, idx, val);
                        }
                    }
            }
            state.push_value(table)?;
            Ok(1)
        }
        Err(err) => Err(state.error(format!("fs: listdir '{}' failed: {}", path, err))),
    }
}

fn lfs_mkdir(state: &mut LuaState) -> LuaResult<usize> {
    let path = lua_check_str(state, 1)?;
    if fs::create_dir_all(path).is_ok() {
        state.push_value(LuaValue::boolean(true))?;
        Ok(1)
    } else {
        Err(state.error(format!("fs: mkdir '{}' failed", path)))
    }
}

fn lfs_exists(state: &mut LuaState) -> LuaResult<usize> {
    let path = lua_check_str(state, 1)?;
    state.push_value(LuaValue::boolean(fs::metadata(path).is_ok()))?;
    Ok(1)
}

fn lfs_isdir(state: &mut LuaState) -> LuaResult<usize> {
    let path = lua_check_str(state, 1)?;
    let is_dir = fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false);
    state.push_value(LuaValue::boolean(is_dir))?;
    Ok(1)
}

fn lfs_split(state: &mut LuaState) -> LuaResult<usize> {
    let path_str = lua_check_str(state, 1)?;
    let path = Path::new(path_str);

    if let Some(parent) = path.parent() {
        let val = state.create_string(&parent.to_string_lossy())?;
        state.push_value(val)?;
    } else {
        state.push_value(LuaValue::nil())?;
    }

    if let Some(name) = path.file_name() {
        let val = state.create_string(&name.to_string_lossy())?;
        state.push_value(val)?;
    } else {
        state.push_value(LuaValue::nil())?;
    }

    if let Some(ext) = path.extension() {
        let val = state.create_string(&ext.to_string_lossy())?;
        state.push_value(val)?;
    } else {
        state.push_value(LuaValue::nil())?;
    }

    Ok(3)
}

fn lfs_ext(state: &mut LuaState) -> LuaResult<usize> {
    let path_str = lua_check_str(state, 1)?;
    let path = Path::new(path_str);

    if let Some(ext) = path.extension() {
        let val = state.create_string(&ext.to_string_lossy())?;
        state.push_value(val)?;
    } else {
        state.push_value(LuaValue::nil())?;
    }

    Ok(1)
}

fn lfs_stem(state: &mut LuaState) -> LuaResult<usize> {
    let path_str = lua_check_str(state, 1)?;
    let path = Path::new(path_str);

    if let Some(name) = path.file_stem() {
        let val = state.create_string(&name.to_string_lossy())?;
        state.push_value(val)?;
    } else {
        state.push_value(LuaValue::nil())?;
    }

    Ok(1)
}

fn lfs_join(state: &mut LuaState) -> LuaResult<usize> {
    let mut path = PathBuf::new();
    let top = state.arg_count();

    for i in 1..=top {
        if let Some(s) = lua_opt_str(state, i) {
            path.push(Path::new(s));
        }
    }

    let val = state.create_string(&path.to_string_lossy())?;
    state.push_value(val)?;

    Ok(1)
}

fn lfs_pwd(state: &mut LuaState) -> LuaResult<usize> {
    if let Ok(dir) = env::current_dir() {
        let val = state.create_string(&dir.to_string_lossy())?;
        state.push_value(val)?;
    } else {
        state.push_value(LuaValue::nil())?;
    }

    Ok(1)
}

fn lfs_abspath(state: &mut LuaState) -> LuaResult<usize> {
    let path = lua_check_str(state, 1)?;
    if let Ok(abs) = fs::canonicalize(path) {
        let val = state.create_string(&abs.to_string_lossy())?;
        state.push_value(val)?;
    } else {
        state.push_value(LuaValue::nil())?;
    }

    Ok(1)
}

fn lfs_remove(state: &mut LuaState) -> LuaResult<usize> {
    let path_str = lua_check_str(state, 1)?;
    let path = Path::new(path_str);
    let result = if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    };

    match result {
        Ok(_) => {
            state.push_value(LuaValue::integer(1))?;
            Ok(1)
        }
        Err(err) => Err(state.error(format!("fs: remove '{}' failed: {}", path.display(), err))),
    }
}

pub fn register_fs() -> luars::LibraryModule {
    luars::lua_module!("fs", {
        "listdir" => lfs_listdir,
        "mkdir" => lfs_mkdir,
        "exists" => lfs_exists,
        "isdir" => lfs_isdir,
        "split" => lfs_split,
        "ext" => lfs_ext,
        "stem" => lfs_stem,
        "join" => lfs_join,
        "pwd" => lfs_pwd,
        "remove" => lfs_remove,
        "abspath" => lfs_abspath,
    })
}
