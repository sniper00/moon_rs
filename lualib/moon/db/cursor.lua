local moon = require "moon"

-- The fourth generic-for value closes a pending batch on break or error.
return function(session)
    local buffer
    local index = 0
    local done = false
    local pending_cursor

    local function close()
        done = true
        buffer = nil
        if pending_cursor then
            pending_cursor:close()
            pending_cursor = nil
        end
    end

    local stream_state = setmetatable({}, { __close = close })

    local function iter()
        while true do
            if buffer and index < #buffer then
                index = index + 1
                return buffer[index]
            end
            if done then
                return nil
            end
            if pending_cursor then
                local err
                session, err = pending_cursor:next()
                pending_cursor = nil
                if not session then
                    close()
                    error(err, 0)
                end
            end
            local rows, cursor = moon.wait(session)
            if not rows then
                done = true
                return nil
            end
            pending_cursor = cursor
            if #rows == 0 then
                close()
                return nil
            end
            buffer = rows
            index = 0
            done = cursor == nil
        end
    end

    return iter, nil, nil, stream_state
end
