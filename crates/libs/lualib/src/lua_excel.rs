use calamine::{Data, Reader, Xlsx, open_workbook};
use csv::ReaderBuilder;
use luars::{LuaResult, LuaState, LuaValue};
use std::path::Path;

use crate::{lua_check_str, lua_opt_integer, lua_push_error};

fn read_csv(state: &mut LuaState, path: &Path, max_row: usize) -> LuaResult<usize> {
    let res = ReaderBuilder::new().has_headers(false).from_path(path);

    match res {
        Ok(mut reader) => {
            let all_sheets = state.create_table(1, 0)?;
            let one_sheet = state.create_table(0, 2)?;

            let sheet_name_key = state.create_string("sheet_name")?;
            let sheet_name_val = state.create_string(
                path.file_stem()
                    .unwrap_or_default()
                    .to_str()
                    .unwrap_or_default(),
            )?;
            state.raw_set(&one_sheet, sheet_name_key, sheet_name_val);

            let data_key = state.create_string("data")?;
            let sheet_data = state.create_table(0, 0)?;

            let mut row_idx: i64 = 0;
            for (i, result) in reader.records().enumerate() {
                if i == max_row {
                    break;
                }
                match result {
                    Ok(record) => {
                        let row = state.create_table(record.len(), 0)?;
                        for (col_idx, field) in record.iter().enumerate() {
                            let val = state.create_string(field)?;
                            state.raw_seti(&row, (col_idx + 1) as i64, val);
                        }
                        row_idx += 1;
                        state.raw_seti(&sheet_data, row_idx, row);
                    }
                    Err(err) => {
                        return lua_push_error(state, &format!(
                            "excel: read csv '{}' failed: {}", path.to_string_lossy(), err
                        ));
                    }
                }
            }

            state.raw_set(&one_sheet, data_key, sheet_data);
            state.raw_seti(&all_sheets, 1, one_sheet);
            state.push_value(all_sheets)?;
            Ok(1)
        }
        Err(err) => {
            lua_push_error(state, &format!("excel: open '{}' failed: {}", path.to_string_lossy(), err))
        }
    }
}

fn read_xlsx(state: &mut LuaState, path: &Path, max_row: usize) -> LuaResult<usize> {
    let res: Result<Xlsx<_>, _> = open_workbook(path);
    match res {
        Ok(mut workbook) => {
            let sheet_names: Vec<String> = workbook.sheet_names().to_vec();
            let all_sheets = state.create_table(sheet_names.len(), 0)?;
            let mut sheet_idx: i64 = 0;

            for sheet_name in &sheet_names {
                if let Ok(range) = workbook.worksheet_range(sheet_name.as_str()) {
                    let one_sheet = state.create_table(0, 2)?;

                    let name_key = state.create_string("sheet_name")?;
                    let name_val = state.create_string(sheet_name.as_str())?;
                    state.raw_set(&one_sheet, name_key, name_val);

                    let data_key = state.create_string("data")?;
                    let sheet_data = state.create_table(range.rows().len(), 0)?;

                    for (i, row) in range.rows().enumerate() {
                        if i >= max_row {
                            break;
                        }
                        let row_data = state.create_table(row.len(), 0)?;

                        for (col_idx, cell) in row.iter().enumerate() {
                            let val = match cell {
                                Data::Int(v) => LuaValue::integer(*v),
                                Data::Float(v) => LuaValue::float(*v),
                                Data::String(v) => state.create_string(v.as_str())?,
                                Data::Bool(v) => LuaValue::integer(*v as i64),
                                Data::Error(v) => state.create_string(&v.to_string())?,
                                Data::Empty => LuaValue::nil(),
                                Data::DateTime(v) => state.create_string(&v.to_string())?,
                                _ => LuaValue::nil(),
                            };
                            state.raw_seti(&row_data, (col_idx + 1) as i64, val);
                        }
                        state.raw_seti(&sheet_data, (i + 1) as i64, row_data);
                    }

                    state.raw_set(&one_sheet, data_key, sheet_data);
                    sheet_idx += 1;
                    state.raw_seti(&all_sheets, sheet_idx, one_sheet);
                }
            }

            state.push_value(all_sheets)?;
            Ok(1)
        }
        Err(err) => lua_push_error(state, &err.to_string()),
    }
}

fn lua_excel_read(state: &mut LuaState) -> LuaResult<usize> {
    let filename = lua_check_str(state, 1)?.to_string();
    let max_row: usize = lua_opt_integer(state, 2).unwrap_or(usize::MAX);
    let path = Path::new(&filename);

    match path.extension() {
        Some(ext) => {
            let ext = ext.to_string_lossy().to_string();
            match ext.as_str() {
                "csv" => read_csv(state, path, max_row),
                "xlsx" => read_xlsx(state, path, max_row),
                _ => lua_push_error(state, &format!("excel: unsupported file type '{}'", ext)),
            }
        }
        None => lua_push_error(state, &format!("excel: unsupported file type '{}'", path.to_string_lossy())),
    }
}

pub fn register_excel() -> luars::LibraryModule {
    luars::lua_module!("excel", {
        "read" => lua_excel_read,
    })
}
