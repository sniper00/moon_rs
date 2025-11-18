use calamine::{Data, Reader, Xlsx, open_workbook};
use csv::ReaderBuilder;
use lib_lua::{
    self, cstr,
    ffi::{self},
    laux::{self, LuaNil, LuaState, LuaTable},
    lreg, lreg_null, luaL_newlib,
};
use std::path::Path;

fn read_csv(state: LuaState, path: &Path, max_row: usize) -> i32 {
    let res = ReaderBuilder::new().has_headers(false).from_path(path);

    let all_sheets = LuaTable::new(state, 1, 0);

    match res {
        Ok(mut reader) => {
            let one_sheet = LuaTable::new(state, 0, 2);
            one_sheet.insert(
                "sheet_name",
                path.file_stem()
                    .unwrap_or_default()
                    .to_str()
                    .unwrap_or_default(),
            );

            laux::lua_push(state, "data");

            let sheet_data = LuaTable::new(state, reader.records().count(), 0);

            for (i, result) in reader.records().enumerate() {
                if i == max_row {
                    break;
                }
                match result {
                    Ok(record) => {
                        let row = LuaTable::new(state, 0, record.len());
                        for field in record.iter() {
                            row.push(field);
                        }
                        sheet_data.push(row);
                    }
                    Err(err) => {
                        laux::lua_push(state, false);
                        laux::lua_push(
                            state,
                            format!("read csv '{}' error: {}", path.to_string_lossy(), err)
                                .as_str(),
                        );
                        return 2;
                    }
                }
            }

            one_sheet.insert_from_stack();
            all_sheets.push(one_sheet);

            1
        }
        Err(err) => {
            laux::lua_push(state, false);
            laux::lua_push(
                state,
                format!("open file '{}' error: {}", path.to_string_lossy(), err).as_str(),
            );
            2
        }
    }
}

fn read_xlxs(state: LuaState, path: &Path, max_row: usize) -> i32 {
    let res: Result<Xlsx<_>, _> = open_workbook(path);
    match res {
        Ok(mut workbook) => {
            let all_sheets = LuaTable::new(state, 4, 0);
            for sheet_name in workbook.sheet_names() {
                if let Ok(range) = workbook.worksheet_range(sheet_name.as_str()) {
                    let one_sheet = LuaTable::new(state, 0, 2);
                    one_sheet.insert("sheet_name", sheet_name.as_str());

                    laux::lua_push(state, "data");
                    let sheet_data = LuaTable::new(state, range.rows().len(), 0);

                    for (i, row) in range.rows().enumerate() {
                        if i >= max_row {
                            break;
                        }
                        //row
                        let row_data = LuaTable::new(state, 0, row.len());

                        for cell in row.iter() {
                            //columns
                            match cell {
                                Data::Int(v) => row_data.push(*v),
                                Data::Float(v) => row_data.push(*v),
                                Data::String(v) => row_data.push(v.as_str()),
                                Data::Bool(v) => row_data.push(*v as i32),
                                Data::Error(v) => row_data.push(v.to_string()),
                                Data::Empty => row_data.push(LuaNil {}),
                                Data::DateTime(v) => row_data.push(v.to_string()),
                                _ => row_data.push(LuaNil {}),
                            };
                        }
                        sheet_data.push(row_data);
                    }
                    one_sheet.insert_from_stack();
                    all_sheets.push(one_sheet);
                }
            }
            1
        }
        Err(err) => {
            laux::lua_push(state, false);
            laux::lua_push(state, format!("{}", err).as_str());
            2
        }
    }
}

extern "C-unwind" fn lua_excel_read(state: LuaState) -> i32 {
    let filename: &str = laux::lua_get(state, 1);
    let max_row: usize = laux::lua_opt(state, 2).unwrap_or(usize::MAX);
    let path = Path::new(filename);

    match path.extension() {
        Some(ext) => {
            let ext = ext.to_string_lossy().to_string();
            match ext.as_str() {
                "csv" => read_csv(state, path, max_row),
                "xlsx" => read_xlxs(state, path, max_row),
                _ => {
                    laux::lua_push(state, false);
                    laux::lua_push(state, format!("unsupport file type: {}", ext));
                    2
                }
            }
        }
        None => {
            laux::lua_push(state, false);
            laux::lua_push(
                state,
                format!("unsupport file type: {}", path.to_string_lossy()),
            );
            2
        }
    }
}

pub extern "C-unwind" fn luaopen_excel(state: LuaState) -> i32 {
    let l = [lreg!("read", lua_excel_read), lreg_null!()];
    luaL_newlib!(state, l);
    1
}
