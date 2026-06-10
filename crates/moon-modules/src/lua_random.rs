use moon_lua::{cstr, ffi, laux, lreg, lreg_null, luaL_newlib};
use rand::RngExt;
use std::ffi::c_int;

use moon_lua::laux::LuaState;

fn table_to_i64_vec(state: LuaState, index: c_int) -> Result<Vec<i64>, String> {
    let abs_index = laux::lua_absindex(state, index);
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

extern "C-unwind" fn rand_range(state: LuaState) -> c_int {
    let min = unsafe { ffi::luaL_checkinteger(state.as_ptr(), 1) as i64 };
    let max = unsafe { ffi::luaL_checkinteger(state.as_ptr(), 2) as i64 };
    if min > max {
        laux::lua_error(
            state,
            format!(
                "random.rand_range: min value must be less than or equal to max value, got min={} and max={}",
                min, max
            ),
        );
    }

    let value = rand::rng().random_range(min..=max);
    laux::lua_push(state, value as ffi::lua_Integer);
    1
}

extern "C-unwind" fn rand_range_some(state: LuaState) -> c_int {
    let min = unsafe { ffi::luaL_checkinteger(state.as_ptr(), 1) as i64 };
    let max = unsafe { ffi::luaL_checkinteger(state.as_ptr(), 2) as i64 };
    let count = unsafe { ffi::luaL_checkinteger(state.as_ptr(), 3) as i64 };

    if min > max {
        laux::lua_error(
            state,
            format!(
                "random.rand_range_some: min value must be less than or equal to max value, got min={} and max={}",
                min, max
            ),
        );
    }

    let Some(range_len) = checked_range_len(min, max) else {
        laux::lua_error(
            state,
            "random.rand_range_some: range size overflow".to_string(),
        );
    };

    if count <= 0 || range_len < count {
        laux::lua_error(
            state,
            format!(
                "random.rand_range_some: count must be in range [1, {}], got {}",
                range_len, count
            ),
        );
    }

    let range_len_usize = usize::try_from(range_len).unwrap_or_else(|_| {
        laux::lua_error(
            state,
            "random.rand_range_some: range size is too large".to_string(),
        )
    });
    let count_usize = usize::try_from(count).unwrap_or_else(|_| {
        laux::lua_error(
            state,
            "random.rand_range_some: count is too large".to_string(),
        )
    });

    let mut values: Vec<i64> = (0..range_len_usize).map(|i| min + i as i64).collect();
    let table = laux::LuaTable::new(state, count_usize, 0);
    let mut rng = rand::rng();

    for i in 1..=count_usize {
        let index = rng.random_range(0..values.len());
        laux::lua_push(state, values[index] as ffi::lua_Integer);
        table.rawseti(i);
        values.swap_remove(index);
    }

    1
}

extern "C-unwind" fn randf_range(state: LuaState) -> c_int {
    let min = unsafe { ffi::luaL_checknumber(state.as_ptr(), 1) };
    let max = unsafe { ffi::luaL_checknumber(state.as_ptr(), 2) };
    if !min.is_finite() || !max.is_finite() || min > max {
        laux::lua_error(
            state,
            format!(
                "random.randf_range: min and max must be finite with min <= max, got min={} and max={}",
                min, max
            ),
        );
    }

    let value = if min == max {
        min
    } else {
        rand::rng().random_range(min..max)
    };
    laux::lua_push(state, value);
    1
}

extern "C-unwind" fn randf_percent(state: LuaState) -> c_int {
    let percent = unsafe { ffi::luaL_checknumber(state.as_ptr(), 1) };
    let value = percent > 0.0 && rand::rng().random_range(0.0..1.0) < percent;
    laux::lua_push(state, value);
    1
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

extern "C-unwind" fn rand_weight(state: LuaState) -> c_int {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);
    laux::lua_checktype(state, 2, ffi::LUA_TTABLE);

    let result = (|| {
        let values = table_to_i64_vec(state, 1)?;
        let weights = table_to_i64_vec(state, 2)?;
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
            laux::lua_push(state, value as ffi::lua_Integer);
            1
        }
        Ok(None) => 0,
        Err(err) => laux::lua_error(state, err),
    }
}

extern "C-unwind" fn rand_weight_some(state: LuaState) -> c_int {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);
    laux::lua_checktype(state, 2, ffi::LUA_TTABLE);
    let count = unsafe { ffi::luaL_checkinteger(state.as_ptr(), 3) as i64 };

    let result = (|| {
        let mut values = table_to_i64_vec(state, 1)?;
        let mut weights = table_to_i64_vec(state, 2)?;
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
            let table = laux::LuaTable::new(state, values.len(), 0);
            for (idx, value) in values.into_iter().enumerate() {
                laux::lua_push(state, value as ffi::lua_Integer);
                table.rawseti(idx + 1);
            }
            1
        }
        Ok(None) => 0,
        Err(err) => laux::lua_error(state, err),
    }
}

pub unsafe extern "C-unwind" fn luaopen_random(state: LuaState) -> c_int {
    let l = [
        lreg!("rand_range", rand_range),
        lreg!("rand_range_some", rand_range_some),
        lreg!("randf_range", randf_range),
        lreg!("randf_percent", randf_percent),
        lreg!("rand_weight", rand_weight),
        lreg!("rand_weight_some", rand_weight_some),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}
