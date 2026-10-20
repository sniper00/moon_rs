use moon_base::{cstr, ffi, laux, lreg_null, lreg_try, luaL_newlib};
use rand::RngExt;
use std::ffi::c_int;

use moon_base::laux::{LuaStack, LuaState};

fn table_to_i64_vec(lua: &LuaStack<'_>, index: c_int) -> Result<Vec<i64>, String> {
    let state = lua.state();
    if laux::lua_type(state, index) != laux::LuaType::Table {
        return Err(format!("argument #{} must be a table", index));
    }
    let abs_index = lua.abs_index(index);
    let len = unsafe { ffi::lua_rawlen(state.as_ptr(), abs_index) };
    let mut values = Vec::with_capacity(len);

    for i in 1..=len {
        unsafe {
            ffi::lua_rawgeti(state.as_ptr(), abs_index, i as ffi::lua_Integer);
            let mut is_num = 0;
            let value = ffi::lua_tointegerx(state.as_ptr(), -1, &mut is_num);
            ffi::lua_pop(state.as_ptr(), 1);
            if is_num == 0 {
                return Err(format!("table element #{} must be an integer", i));
            }
            values.push(value as i64);
        }
    }

    Ok(values)
}

fn checked_range_len(min: i64, max: i64) -> Option<i64> {
    max.checked_sub(min)?.checked_add(1)
}

fn rand_range(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let min: i64 = lua.get(1).map_err(|e| format!("random.rand_range: {e}"))?;
    let max: i64 = lua.get(2).map_err(|e| format!("random.rand_range: {e}"))?;
    if min > max {
        return Err(format!(
            "random.rand_range: min value must be less than or equal to max value, got min={} and max={}",
            min, max
        ));
    }

    let value = rand::rng().random_range(min..=max);
    lua.push(value);
    Ok(1)
}

fn rand_range_some(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let min: i64 = lua
        .get(1)
        .map_err(|e| format!("random.rand_range_some: {e}"))?;
    let max: i64 = lua
        .get(2)
        .map_err(|e| format!("random.rand_range_some: {e}"))?;
    let count: i64 = lua
        .get(3)
        .map_err(|e| format!("random.rand_range_some: {e}"))?;

    if min > max {
        return Err(format!(
            "random.rand_range_some: min value must be less than or equal to max value, got min={} and max={}",
            min, max
        ));
    }

    let Some(range_len) = checked_range_len(min, max) else {
        return Err("random.rand_range_some: range size overflow".to_string());
    };

    if count <= 0 || range_len < count {
        return Err(format!(
            "random.rand_range_some: count must be in range [1, {}], got {}",
            range_len, count
        ));
    }

    let count_usize = usize::try_from(count)
        .map_err(|_| "random.rand_range_some: count is too large".to_string())?;

    let mut rng = rand::rng();

    // Floyd's algorithm: `count` draws, each adding either index `j` or a
    // uniformly drawn `t <= j`. Yields a uniform random subset of [min, max]
    // without ever enumerating the range, so time and memory stay O(count)
    // whatever the range width. Order is not significant (set iteration order).
    let mut chosen = std::collections::HashSet::with_capacity(count_usize);
    for j in (range_len - count)..range_len {
        let t = rng.random_range(0..=j);
        let value = if chosen.contains(&(min + t)) {
            min + j
        } else {
            min + t
        };
        chosen.insert(value);
    }

    let table = laux::LuaTable::new(lua.state(), count_usize, 0);
    for (i, value) in chosen.into_iter().enumerate() {
        lua.push(value);
        table.rawseti(i + 1);
    }

    Ok(1)
}

fn randf_range(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let min: f64 = lua.get(1).map_err(|e| format!("random.randf_range: {e}"))?;
    let max: f64 = lua.get(2).map_err(|e| format!("random.randf_range: {e}"))?;
    if !min.is_finite() || !max.is_finite() || min > max {
        return Err(format!(
            "random.randf_range: min and max must be finite with min <= max, got min={} and max={}",
            min, max
        ));
    }

    let value = if min == max {
        min
    } else {
        rand::rng().random_range(min..max)
    };
    lua.push(value);
    Ok(1)
}

fn randf_percent(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let percent: f64 = lua
        .get(1)
        .map_err(|e| format!("random.randf_percent: {e}"))?;
    let value = percent > 0.0 && rand::rng().random_range(0.0..1.0) < percent;
    lua.push(value);
    Ok(1)
}

fn choose_weighted(values: &[i64], weights: &[i64]) -> Option<i64> {
    if weights.iter().any(|weight| *weight < 0) {
        return None;
    }

    let sum: i64 = weights.iter().copied().sum();
    if sum == 0 {
        return None;
    }

    let mut cutoff = rand::rng().random_range(0..sum);
    for (value, weight) in values.iter().zip(weights) {
        if cutoff < *weight {
            return Some(*value);
        }
        cutoff -= *weight;
    }

    values.last().copied()
}

fn rand_weight(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let result = (|| {
        let values = table_to_i64_vec(lua, 1)?;
        let weights = table_to_i64_vec(lua, 2)?;
        if values.len() != weights.len() || values.is_empty() {
            return Err(
                "random.rand_weight: 'values' and 'weights' must be non-empty tables of equal length"
                    .to_string(),
            );
        }
        if weights.iter().any(|weight| *weight < 0) {
            return Err("random.rand_weight: weights must be non-negative".to_string());
        }
        Ok(choose_weighted(&values, &weights))
    })();

    match result {
        Ok(Some(value)) => {
            lua.push(value);
            Ok(1)
        }
        Ok(None) => Ok(0),
        Err(err) => Err(err),
    }
}

fn rand_weight_some(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let count: i64 = lua
        .get(3)
        .map_err(|e| format!("random.rand_weight_some: {e}"))?;

    let result = (|| {
        let mut values = table_to_i64_vec(lua, 1)?;
        let mut weights = table_to_i64_vec(lua, 2)?;
        if values.len() != weights.len()
            || values.is_empty()
            || count < 0
            || values.len() < count as usize
        {
            return Err(format!(
                "random.rand_weight_some: 'values' and 'weights' must be non-empty tables of equal length, and 'count' must be in range [0, {}], got {}",
                values.len(),
                count
            ));
        }
        if weights.iter().any(|weight| *weight < 0) {
            return Err("random.rand_weight_some: weights must be non-negative".to_string());
        }

        let mut picked = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let sum: i64 = weights.iter().copied().sum();
            if sum == 0 {
                return Ok(None);
            }

            let mut cutoff = rand::rng().random_range(0..sum);
            let mut index = 0;
            while cutoff >= weights[index] {
                cutoff -= weights[index];
                index += 1;
            }

            picked.push(values[index]);
            values.swap_remove(index);
            weights.swap_remove(index);
        }

        Ok(Some(picked))
    })();

    match result {
        Ok(Some(values)) => {
            let table = laux::LuaTable::new(lua.state(), values.len(), 0);
            for (idx, value) in values.into_iter().enumerate() {
                lua.push(value);
                table.rawseti(idx + 1);
            }
            Ok(1)
        }
        Ok(None) => Ok(0),
        Err(err) => Err(err),
    }
}

pub unsafe extern "C-unwind" fn luaopen_random(state: LuaState) -> c_int {
    let l = [
        lreg_try!("rand_range", rand_range),
        lreg_try!("rand_range_some", rand_range_some),
        lreg_try!("randf_range", randf_range),
        lreg_try!("randf_percent", randf_percent),
        lreg_try!("rand_weight", rand_weight),
        lreg_try!("rand_weight_some", rand_weight_some),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
#[path = "../../tests/unit/modules/lua_random.rs"]
mod tests;
