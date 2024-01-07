use calamine::{open_workbook, DataType, Reader, Xlsx};
use csv::ReaderBuilder;
use lib_lua::{ffi, ffi::luaL_Reg};
use std::{os::raw::c_int, path::Path};

use lib_core::{
    c_str,
    laux::{self},
    lreg, lreg_null,
};

fn read_csv(state: *mut ffi::lua_State, path: &Path) -> c_int {
    let res = ReaderBuilder::new().has_headers(false).from_path(path);
    unsafe {
        ffi::lua_createtable(state, 0, 0);
    }
    match res {
        Ok(mut reader) => {
            unsafe {
                ffi::lua_createtable(state, 0, 2);
                laux::push_str(
                    state,
                    path.file_stem()
                        .unwrap_or_default()
                        .to_str()
                        .unwrap_or_default(),
                );
                ffi::lua_setfield(state, -2, c_str!("name"));
                ffi::lua_createtable(state, 1024, 0);
            }

            let mut idx: i64 = 0;

            for result in reader.records() {
                match result {
                    Ok(record) => unsafe {
                        ffi::lua_createtable(state, 0, record.len() as i32);
                        for (i, field) in record.iter().enumerate() {
                            laux::push_str(state, field);
                            ffi::lua_rawseti(state, -2, (i + 1) as i64);
                        }
                        idx += 1;
                        ffi::lua_rawseti(state, -2, idx);
                    },
                    Err(err) => unsafe {
                        ffi::lua_pushboolean(state, 0);
                        laux::push_str(
                            state,
                            format!("read csv '{}' error: {}", path.to_string_lossy(), err)
                                .as_str(),
                        );
                        return 2;
                    },
                }
            }

            unsafe {
                ffi::lua_setfield(state, -2, c_str!("data"));
                ffi::lua_rawseti(state, -2, 1);
            }
            1
        }
        Err(err) => {
            unsafe {
                ffi::lua_pushboolean(state, 0);
            }

            laux::push_str(
                state,
                format!("open file '{}' error: {}", path.to_string_lossy(), err).as_str(),
            );
            2
        }
    }
}

fn read_xlxs(state: *mut ffi::lua_State, path: &Path) -> c_int {
    let res: Result<Xlsx<_>, _> = open_workbook(path);
    match res {
        Ok(mut workbook) => {
            unsafe {
                ffi::lua_createtable(state, 0, 0);
            }
            let mut sheet_counter = 0;
            workbook.sheet_names().iter().for_each(|sheet| {
                if let Some(Ok(range)) = workbook.worksheet_range(sheet) {
                    unsafe {
                        ffi::lua_createtable(state, 0, 2);
                        laux::push_str(state, sheet.as_str());

                        ffi::lua_setfield(state, -2, c_str!("name"));

                        ffi::lua_createtable(state, range.rows().len() as i32, 0);
                        for (i, row) in range.rows().enumerate() {
                            //rows
                            ffi::lua_createtable(state, row.len() as i32, 0);

                            for (j, cell) in row.iter().enumerate() {
                                //columns

                                match cell {
                                    DataType::Int(v) => {
                                        ffi::lua_pushinteger(state, *v as ffi::lua_Integer)
                                    }
                                    DataType::Float(v) => ffi::lua_pushnumber(state, *v),
                                    DataType::String(v) => laux::push_str(state, v.as_str()),
                                    DataType::Bool(v) => ffi::lua_pushboolean(state, *v as i32),
                                    DataType::Error(v) => laux::push_str(state, &v.to_string()),
                                    DataType::Empty => ffi::lua_pushnil(state),
                                    DataType::DateTime(v) => laux::push_str(state, &v.to_string()),
                                    _ => ffi::lua_pushnil(state),
                                }
                                ffi::lua_rawseti(state, -2, (j + 1) as i64);
                            }
                            ffi::lua_rawseti(state, -2, (i + 1) as i64);
                        }
                        ffi::lua_setfield(state, -2, c_str!("data"));
                    }
                    sheet_counter += 1;
                    unsafe {
                        ffi::lua_rawseti(state, -2, sheet_counter as i64);
                    }
                }
            });
            1
        }
        Err(err) => unsafe {
            ffi::lua_pushboolean(state, 0);
            laux::push_str(state, format!("{}", err).as_str());
            2
        },
    }
}

extern "C-unwind" fn lua_excel_read(state: *mut ffi::lua_State) -> c_int {
    let filename = laux::check_str(state, 1);
    let path = Path::new(filename);

    match path.extension() {
        Some(ext) => {
            let ext = ext.to_string_lossy().to_string();
            match ext.as_str() {
                "csv" => read_csv(state, path),
                "xlsx" => read_xlxs(state, path),
                _ => unsafe {
                    ffi::lua_pushboolean(state, 0);
                    laux::push_str(state, format!("unsupport file type: {}", ext).as_str());
                    2
                },
            }
        }
        None => unsafe {
            ffi::lua_pushboolean(state, 0);
            laux::push_str(
                state,
                format!("unsupport file type: {}", path.to_string_lossy()).as_str(),
            );
            2
        },
    }
}

pub unsafe extern "C-unwind" fn luaopen_excel(state: *mut ffi::lua_State) -> c_int {
    let l = [lreg!("read", lua_excel_read), lreg_null!()];

    ffi::lua_createtable(state, 0, l.len() as c_int);
    ffi::luaL_setfuncs(state, l.as_ptr(), 0);

    1
}
