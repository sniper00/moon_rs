--- Player-name search wrapper over the native PostgreSQL driver (`moon.db.pg`).
---
--- Backs three search modes with the right index for each:
---   * prefix    -> B-tree on `name text_pattern_ops`            (LIKE 'x%')
---   * substring -> GIN `gin_trgm_ops` (pg_trgm)                 (ILIKE '%x%')
---   * similar   -> GIN `gin_trgm_ops` similarity ranking        (name % 'x')
---
--- All user input is bound as parameters; LIKE/ILIKE wildcards in the input are
--- escaped so a user typing `%`/`_` cannot widen the match.

---@class namesearch
---@field public db pg
---@field public tbl string
---@field public trgm_ready boolean
local M = {}
M.__index = M

-- Escape LIKE/ILIKE metacharacters (and the escape char itself).
local function escape_like(s)
    return (tostring(s):gsub("([\\%%_])", "\\%1"))
end
M.escape_like = escape_like

---@param db pg an already-connected `moon.db.pg` object
---@param table_name? string defaults to "players"
function M.new(db, table_name)
    return setmetatable({ db = db, tbl = table_name or "players" }, M)
end

--- Create the table, the pg_trgm extension and both indexes (idempotent).
--- `pg_trgm` requires privileges; if it cannot be created, substring/similar
--- modes are unavailable but prefix search still works.
---@return boolean trgm_ready whether the trigram index is usable
function M:init_schema()
    local tbl = self.tbl
    self.db:query(string.format([[
        CREATE TABLE IF NOT EXISTS %s (
            uid  bigint PRIMARY KEY,
            name text NOT NULL
        )]], tbl))

    self.db:query(string.format(
        "CREATE INDEX IF NOT EXISTS idx_%s_name_prefix ON %s (name text_pattern_ops)", tbl, tbl))

    local ext = self.db:query("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    local trgm_ready = not ext.code
    if trgm_ready then
        self.db:query(string.format(
            "CREATE INDEX IF NOT EXISTS idx_%s_name_trgm ON %s USING gin (name gin_trgm_ops)", tbl, tbl))
    end
    self.trgm_ready = trgm_ready
    return trgm_ready
end

--- Upsert one player (handles the "frequent insert/update" path).
---@param uid integer
---@param name string
function M:add(uid, name)
    return self.db:query_params(string.format(
        "INSERT INTO %s(uid,name) VALUES($1,$2) ON CONFLICT(uid) DO UPDATE SET name=EXCLUDED.name", self.tbl),
        uid, name)
end

--- Delete one player (the "frequent delete" path).
---@param uid integer
function M:remove(uid)
    return self.db:query_params(string.format("DELETE FROM %s WHERE uid=$1", self.tbl), uid)
end

--- Names starting with `prefix` (case-sensitive, index range scan).
---@param prefix string
---@param limit? integer default 20
---@return table rows, pg_result res
function M:search_prefix(prefix, limit)
    limit = limit or 20
    local pat = escape_like(prefix) .. "%"
    local res = self.db:query_params(string.format(
        "SELECT uid,name FROM %s WHERE name LIKE $1 ESCAPE '\\' ORDER BY name LIMIT %d",
        self.tbl, limit), pat)
    return res.data or {}, res
end

--- Names containing `term` anywhere (case-insensitive, trigram index).
---@param term string
---@param limit? integer default 20
---@return table rows, pg_result res
function M:search_substring(term, limit)
    limit = limit or 20
    local pat = "%" .. escape_like(term) .. "%"
    local res = self.db:query_params(string.format(
        "SELECT uid,name FROM %s WHERE name ILIKE $1 ESCAPE '\\' LIMIT %d",
        self.tbl, limit), pat)
    return res.data or {}, res
end

--- Typo-tolerant fuzzy search ranked by trigram similarity.
--- Uses the default `pg_trgm.similarity_threshold` (0.3) via the `%` operator.
---@param term string
---@param limit? integer default 20
---@return table rows, pg_result res
function M:search_similar(term, limit)
    limit = limit or 20
    local res = self.db:query_params(string.format(
        "SELECT uid,name,similarity(name,$1) AS sim FROM %s WHERE name %% $1 ORDER BY sim DESC, name LIMIT %d",
        self.tbl, limit), term)
    return res.data or {}, res
end

return M
