local strformat = string.format
local strsub    = string.sub
local strlower  = string.lower
local strlen    = string.len
local strbyte   = string.byte

local tabconcat = table.concat

local utf8 = require 'lua-utf8'

local _M = {}


local unknown             = 1
local inWord              = 2 -- \S+
local inNumber            = 3   -- [0-9a-fA-Fx.-]
local inSpace             = 4   -- space, tab, \r, \n
local inOp                = 5   -- [=<>!] (usually precedes a number)
local opOrNumber          = 6   -- + in 2 + 2 or +3e-9
local inQuote             = 7   -- '...' or "..."
local subOrOLC            = 8   -- - or start of -- comment
local inDash              = 9   -- -- begins a one-line comment if followed by space
local inOLC               = 10   -- -- comment (at least one space after dash is required)
local divOrMLC            = 11  -- / operator or start of /* comment */
local mlcOrMySQLCode      = 12  -- /* comment */ or /*! MySQL-specific code */
local inMLC               = 13  -- /* comment */
local inValues            = 14  -- VALUES (1), ..., (N)
local moreValuesOrUnknown = 15  -- , (2nd+) or ON DUPLICATE KEY or end of query
local orderBy             = 16  -- ORDER BY
local onDupeKeyUpdate     = 17  -- ON DUPLICATE KEY UPDATE
local inNumberInWord      = 18  -- e.g. db23


local stateName  = {
     "unknown",
     "inWord",
     "inNumber",
     "inSpace",
     "inOp",
     "opOrNumber",
     "inQuote",
     "subOrOLC",
     "inDash",
      "inOLC",
     "divOrMLC",
     "mlcOrMySQLCode",
     "inMLC",
     "inValues",
     "moreValuesOrUnknown",
     "orderBy",
     "onDupeKeyUpdate",
     "inNumberInWord",
}

local Debug = false
local ReplaceNumbersInWords = false

local function isSpace(r)
    return r == 0x20 or r == 0x09 or r == 0x0D or r == 0x0A
end

local function wordIn(q, words)
    q = strlower(q)
    for _, word in ipairs(words) do
        if q == word then
            return true
        end
    end
    return false
end

function _M.parse(data)
    local q = data .. " " --// need range to run off end of original query
    local prevWord = ""
    local f = {}
    local fi = 1
    local pr = ""       --// previous rune
    local s = unknown   --// current state
    local sqlState = unknown
    local quoteChar = "" -- rune(0)
    local cpFromOffset = 1
    local cpToOffset = 1
    local addSpace = false
    local escape = false
    local parOpen = 0
    local parOpenTotal = 0
    local valueNo = 0
    local firstPar = 0

    local sql = utf8.escape(q)
    for qi, r in utf8.next, sql do
        if Debug then
            ngx.log(ngx.ERR, qi .. ":" .. fi .. " " ..  stateName[s] .. "/" .. stateName[sqlState] .. "[".. cpFromOffset ..":".. cpToOffset .."]" .. strformat("%#x",r) .. " ".. utf8.char(r))
        end

        --1. Skip parts of the query for certain states.
        if s == inQuote then
            --[[ We're in a 'quoted value' or "quoted value".  The quoted value
                 ends at the first non-escaped matching quote character (' or ").
            ]]--
            if r ~= quoteChar then
                -- The only char inside a quoted value we need to track is \,
                -- the escape char.  This allows us to tell that the 2nd ' in
                -- '\'' is escaped, not the ending quote char.
                if escape then
                    if Debug then
                        ngx.log(ngx.ERR, "Ignore quoted literal")
                    end
                    escape = false
                elseif r == strbyte('\\') then
                    if Debug then
                        ngx.log(ngx.ERR, "Escape")
                    end
                    escape = true
                else
                    if Debug then
                        ngx.log(ngx.ERR, "Ignore quoted value")
                    end
                end
            elseif escape then
                -- \' or \"
                if Debug then
                    ngx.log(ngx.ERR, "Quote literal")
                end
                escape = false
            else
                --// 'foo' -> ?
                --// "foo" -> ?
                if Debug then
                    ngx.log(ngx.ERR, "Quote end")
                end
                escape = false

                --// qi = the closing quote char, so +1 to ensure we don't copy
                --// anything before this, i.e. quoted value is done, move on.
                cpFromOffset = qi + 1

                if sqlState == inValues then
                    --// ('Hello world!', ...) -> VALUES (, ...)
                    --// The inValues state uses this state to skip quoted values,
                    --// so we don't replace them with ?; the inValues blocks will
                    --// replace the entire value list with ?+.
                    s = inValues
                else
                    f[fi] = '?'
                    fi = fi+1
                    s = unknown
                end
            end
            goto CONTINUE
        elseif s == inNumberInWord then
            --// Replaces number in words with ?
            --// e.g. `db37` to `db?`
            --// Parser can fall into inNumberInWord only if
            --// option ReplaceNumbersInWords is turned on
            if r >= strbyte('0') and r <= strbyte('9') then
                if Debug then
                    ngx.log(ngx.ERR, "Ignore digit in word")
                end
                goto CONTINUE
            end
            -- // 123 -> ?, 0xff -> ?, 1e-9 -> ?, etc.
            if Debug then
                ngx.log(ngx.ERR,"Number in word end")
            end

            f[fi] = '?'
            fi = fi+1
            cpFromOffset = qi
            if isSpace(r) then
                s = unknown
            else
                s = inWord
            end
        elseif s == inNumber then
            --// We're in a number which can be something simple like 123 or
            --// something trickier like 1e-9 or 0xFF.  The pathological case is
            --// like 12ff: this is valid hex number and a valid ident (e.g. table
            --// name).  We can't detect this; the best we can do is realize that
            --// 12ffz is not a number because of the z.
            if (r >= strbyte('0') and r <= strbyte('9')) or (r >= strbyte('a') and r <= strbyte('f')) or (r >= strbyte('A') and r <= strbyte('F')) or r == strbyte('.') or r == strbyte('x') or r == strbyte('-') then
                if Debug then
                    ngx.log(ngx.ERR, "Ignore digit")
                end
                goto CONTINUE
            end

            if (r >= strbyte('g') and r <= strbyte('z')) or (r >= strbyte('G') and r <= strbyte('Z')) or r == strbyte('_') then
                if Debug then
                    ngx.log(ngx.ERR, "Not a number")
                end
                cpToOffset = qi
                s = inWord
            else
                --// 123 -> ?, 0xff -> ?, 1e-9 -> ?, etc.
                if Debug then
                    ngx.log(ngx.ERR, "Number end")
                end
                f[fi] = '?'
                fi = fi+1
                cpFromOffset = qi
                cpToOffset = qi
                s = unknown
            end
        elseif s == inValues then
            --// We're in the (val1),...,(valN) after IN or VALUE[S].  A single
            --// () value ends when the parenthesis are balanced, but...
            if r == strbyte(')') then
                parOpen = parOpen -1
                parOpenTotal = parOpenTotal + 1
                if Debug then
                    ngx.oog(ngx.ERR, "Close parenthesis" .. parOpen)
                end
            elseif r == strbyte('(') then
                parOpen = parOpen + 1
                if Debug then
                    ngx.oog(ngx.ERR, "Open parenthesis", parOpen)
                end
                if parOpen == 1 then
                    firstPar = qi
                end
            elseif r == strbyte('\'') or r == strbyte('"') then
                --// VALUES ('Hello world!') -> enter inQuote state to skip
                --// the quoted value so ')' in 'This ) is a trick' doesn't
                --// balance an outer parenthesis.
                if Debug then
                    ngx.log(ngx.ERR, "Quote begin")
                end
                s = inQuote
                quoteChar = r
                goto CONTINUE
            elseif isSpace(r) then
                if Debug then
                    ngx.log(ngx.ERR, "Space")
                end
                goto CONTINUE
            end
            if parOpen > 0 then
                --// Parenthesis are not balanced yet; i.e. haven't reached
                --// closing ) for this value.
                goto CONTINUE
            end
            if parOpenTotal == 0 then
                --// SELECT value FROM t
                if Debug then
                    ngx.log(ngx.ERR,"Literal values not VALUES()")
                end
                s = inWord
                goto CONTINUE
            end
            --// (<anything>) -> (?+) only for first value
            if Debug then
                ngx.log(ngx.ERR,"Values end")
            end
            valueNo = valueNo + 1
            if valueNo == 1 then
                if qi-firstPar > 1 then
                    --copy(f[fi:fi+4], "(?+)")
                    f[fi] = '('
                    f[fi+1] = '?'
                    f[fi+2] = '+'
                    f[fi+3] = ')'
                    fi = fi+4
                else
                    --// INSERT INTO t VALUES ()
                    --copy(f[fi:fi+2], "()")
                    f[fi] = '('
                    f[fi+1] = ')'
                    fi = fi+2
                end
                firstPar = 0
            end
            --// ... the difficult part is that there may be other values, e.g.
            --// (1), (2), (3).  So we enter the following state.  The values list
            --// ends when the next char is not a comma.
            s = moreValuesOrUnknown
            pr = r
            cpFromOffset = qi + 1
            parOpenTotal = 0
            goto CONTINUE
        elseif s == inMLC then
            --// We're in a /* mutli-line comments */.  Skip and ignore it all.
            if pr == strbyte('*') and r == strbyte('/') then
                --// /* foo */ -> (nothing)
                if Debug then
                    ngx.log(ngx.ERR, "Multi-line comment end")
                end
                s = unknown
            else
                if Debug then
                    ngx.log(ngx.ERR, "Ignore multi-line comment content")
                end
            end
            goto CONTINUE
        elseif s == mlcOrMySQLCode then
            --// We're at the start of either a /* multi-line comment */ or some
            --// /*![version] some MySQL-specific code */.  The ! after the /*
            --// determines which one.
            if r ~= strbyte('!') then
                if Debug then
                    ngx.log(ngx.ERR, "Multi-line comment")
                end
                s = inMLC
                goto CONTINUE
            else
                --// /*![version] SQL_NO_CACHE */ -> /*![version] SQL_NO_CACHE */ (no change)
                if Debug then
                    ngx.log(ngx.ERR, "MySQL-specific code")
                end
                s = inWord
            end
        elseif s == inOLC then
            --// We're in a -- one line comment.  A space after -- is required.
            --// It ends at the end of the line, but there can be more query after
            --// it like:
            --//   SELECT * -- comment
            --//   FROM t
            --// is really "SELECT * FROM t".
            if r == 0x0A then --// newline
                if Debug then
                    ngx.log(ngx.ERR, "One-line comment end")
                end
                s = unknown
            end
            goto CONTINUE
        elseif isSpace(r) and isSpace(pr) then
            --// All space is collapsed into a single space, so if this char is
            --// a space and the previous was too, then skip the extra space.
            if Debug then
                ngx.log(ngx.ERR, "Skip space")
            end
            --// +1 here ensures we actually skip the extra space in certain
            --// cases like "select \n-- bar\n foo".  When a part of the query
            --// triggers a copy of preceding chars, if the only preceding char
            --// is a space then it's incorrectly copied, but +1 sets cpFromOffset
            --// to the same offset as the trigger char, thus avoiding the copy.
            --// For example in that ^ query, the offsets are:
            --//   0 's'
            --//   1 'e'
            --//   2 'l'
            --//   3 'e'
            --//   4 'c'
            --//   5 't'
            --//   6 ' '
            --//   7 '\n'
            --//   8 '-'
            --// After copying 'select ', we are here @ 7 and intend to skip the
            --// newline.  Next, the '-' @ 8 triggers a copy of any preceding
            --// chars.  So here if we set cpFromOffset = 7 then 7:8 is copied,
            --// the newline, but setting cpFromOffset = 7 + 1 is 8:8 and so
            --// nothing is copied as we want.  Actually, cpToOffset is still 6
            --// in this case, but 8:6 avoids the copy too.
            cpFromOffset = qi + 1
            pr = r
            goto CONTINUE
        end

        --2. Change state based on rune and current state.
        if r >= 0x30 and r <= 0x39 then -- // 0-9
            if s == opOrNumber then
                if Debug then
                    ngx.log(ngx.ERR, "+/-First digit")
                end
                cpToOffset = qi - 1
                s = inNumber
            elseif s == inOp then
                if Debug then
                    ngx.log(ngx.ERR, "First digit after operator")
                end
                cpToOffset = qi
                s = inNumber
            elseif s == inWord then
                if pr == '(' then
                    if Debug then
                        ngx.log(ngx.ERR, "Number in function")
                    end
                    cpToOffset = qi
                    s = inNumber
                elseif pr == ',' then
                    --// foo,4 -- 4 may be a number literal or a word/ident
                    if Debug then
                        ngx.log(ngx.ERR, "Number or word")
                    end
                    s = inNumber
                    cpToOffset = qi
                else
                    if Debug then
                        ngx.log(ngx.ERR, "Number in word")
                    end
                    if ReplaceNumbersInWords then
                        s = inNumberInWord
                        cpToOffset = qi
                    end
                end
            else
                if Debug then
                    ngx.log(ngx.ERR, "Number literal")
                end
                s = inNumber
                cpToOffset = qi
            end
        elseif isSpace(r) then
            if s == unknown then
                if Debug then
                    ngx.log(ngx.ERR, "Lost in space")
                end
                if fi > 0 and isSpace(f[fi]) == false then
                    if Debug then
                        ngx.log(ngx.ERR, "Add space")
                    end
                    f[fi] = ' '
                    fi = fi + 1
                    --// This is a common case: a space after skipping something,
                    --// e.g. col = 'foo'<space>. We want only the first space,
                    --// so advance cpFromOffset to whatever is after the space
                    --// and if it's more space then space skipping block will
                    --// handle it.
                    cpFromOffset = qi + 1
                end
            elseif s == inDash then
                if Debug then
                    ngx.log(ngx.ERR, "One-line comment begin")
                end
                s = inOLC
                if cpToOffset > 2 then
                    cpToOffset = qi - 2
                    addSpace = true
                end
            elseif s == moreValuesOrUnknown then
                if Debug then
                    ngx.log(ngx.ERR, "Space after values")
                end
                if valueNo == 1 then
                    f[fi] = ' '
                    fi = fi + 1
                end
            else
                if Debug then
                    ngx.log(ngx.ERR, "Word end")
                end
                local word = strlower(strsub(q, cpFromOffset, qi))
                --// Only match USE if it is the first word in the query, otherwise,
                --// it could be a USE INDEX
                if word == "use" and prevWord == "" then
                    return "use ?"
                elseif (word == "null" and (prevWord ~= "is" and prevWord ~= "not")) or word == "null," then
                    if Debug then
                        ngx.log(ngx.ERR, "NULL as value")
                    end
                    f[fi] = '?'
                    fi = fi+1
                    if word[strlen(word)] == ',' then
                        f[fi] = ','
                        fi = fi+1
                    end
                    f[fi] = ' '
                    fi = fi+1
                    cpFromOffset = qi + 1
                elseif prevWord == "order" and word == "by" then
                    if Debug then
                        ngx.log(ngx.ERR, "ORDER BY begin")
                    end
                    sqlState = orderBy
                elseif sqlState == orderBy and wordIn(word, {"asc", "asc,", "asc "}) then
                    if Debug then
                        ngx.log(ngx.ERR, "ORDER BY ASC")
                    end
                    cpFromOffset = qi
                    if word[strlen(word)] == ',' then
                        fi = fi-1
                        f[fi] = ','
                        f[fi+1] = ' '
                        fi = fi + 2
                    end
                elseif prevWord == "key" and word == "update" then
                    if Debug then
                        ngx.log(ngx.ERR, "ON DUPLICATE KEY UPDATE begin")
                    end
                    sqlState = onDupeKeyUpdate
                end
                s = inSpace
                cpToOffset = qi
                addSpace = true
            end
        elseif r == strbyte('\'') or r == strbyte('"') then
            if pr ~= strbyte('\\') then
                if s ~= inQuote then
                    if Debug then
                        ngx.log(ngx.ERR, "Quote begin")
                    end
                    s = inQuote
                    quoteChar = r
                    cpToOffset = qi
                    if pr == strbyte('x') or pr == strbyte('b') then
                        if Debug then
                            ngx.log(ngx.ERR, "Hex/binary value")
                        end
                        --// We're at the first quote char of x'0F'
                        --// (or b'0101', etc.), so -2 for the quote char and
                        --// the x or b char to copy anything before and up to
                        --// this value.
                        cpToOffset = -2
                    end
                end
            end
        elseif r == strbyte('=') or r == strbyte('<') or r == strbyte('>') or r == strbyte('!') then
            if Debug then
                ngx.log(ngx.ERR, "Operator")
            end
            if s ~= inWord and s ~= inOp then
                cpFromOffset = qi
            end
            s = inOp
        elseif r == strbyte('/') then
            if Debug then
                ngx.log(ngx.ERR, "Op or multi-line comment")
            end
            s = divOrMLC
        elseif r == strbyte('*') and s == divOrMLC then
            if Debug then
                ngx.log(ngx.ERR, "Multi-line comment or MySQL-specific code")
            end
            s = mlcOrMySQLCode
        elseif r == strbyte('+') then
            if Debug then
                ngx.log(ngx.ERR, "Operator or number")
            end
            s = opOrNumber
        elseif r == strbyte('-') then
            if pr == strbyte('-') then
                if Debug then
                    ngx.log(ngx.ERR, "Dash")
                end
                s = inDash
            else
                if Debug then
                    ngx.log(ngx.ERR, "Operator or number")
                end
                s = opOrNumber
            end
        elseif r == strbyte('.') then
            if s == inNumber or s == inOp then
                if Debug then
                    ngx.log(ngx.ERR, "Floating point number")
                end
                s = inNumber
                cpToOffset = qi
            end
        elseif r == strbyte('(') then
            if prevWord == "call" then
                --// 'CALL foo(...)' -> 'call foo'
                if Debug then
                    ngx.log(ngx.ERR, "CALL sp_name")
                end
                return "call " + strsub(q, cpFromOffset, qi)
            elseif sqlState ~= onDupeKeyUpdate and (((s == inSpace or s == moreValuesOrUnknown) and (prevWord == "value" or prevWord == "values" or prevWord == "in")) or wordIn(strsub(q, cpFromOffset, qi), {"value", "values", "in"})) then
                --// VALUE(, VALUE (, VALUES(, VALUES (, IN(, or IN(
                --// but not after ON DUPLICATE KEY UPDATE
                if Debug then
                    ngx.log(ngx.ERR, "Values begin")
                end
                s = inValues
                sqlState = inValues
                parOpen = 1
                firstPar = qi
                if valueNo == 0 then
                    cpToOffset = qi
                end
            elseif s ~= inWord then
                if Debug then
                    ngx.log(ngx.ERR, "Random (")
                end
                valueNo = 0
                cpFromOffset = qi
                s = inWord
            end
        elseif r == strbyte(',') and s == moreValuesOrUnknown then
            if Debug then
                ngx.log(ngx.ERR, "More values")
            end
        elseif r == strbyte(':') and prevWord == "administrator" then
            --// 'administrator command: Init DB' -> 'administrator command: Init DB' (no change)
            if Debug then
                ngx.log(ngx.ERR, "Admin cmd")
            end
            return strsub(q, 1 , strlen(q)) --// original query minus the trailing space we added
        elseif r == strbyte('#') then
            if Debug then
                ngx.log(ngx.ERR, "One-line comment begin")
            end
            s = inOLC
        else
            if s ~= inWord and s ~= inOp then
                --// If in a word or operator then keep copying the query, else
                --// previous chars were being ignored for some reasons but now
                --// we should start copying again, so set cpFromOffset.  Example:
                --// col=NOW(). 'col' will be set to copy, but then '=' will put
                --// us in inOp state which, if a value follows, will trigger a
                --// copy of "col=", but "NOW()" is not a value so "N" is caught
                --// here and since s=inOp still we do not copy yet (this block is
                --// is not entered).
                if Debug then
                    ngx.log(ngx.ERR, "Random character")
                end
                valueNo = 0
                cpFromOffset = qi

                if sqlState == inValues then
                    --// Values are comma-separated, so the first random char
                    --// marks the end of the VALUE() or IN() list.
                    if Debug then
                        ngx.log(ngx.ERR, "No more values")
                    end
                    sqlState = unknown
                end
                s = inWord
            end
        end

        --3. Copy a slice of the query into the fingerprint.

        if cpToOffset > cpFromOffset then
            local l = cpToOffset - cpFromOffset
            prevWord = strlower(strsub(q, cpFromOffset, cpToOffset))
            if Debug then
                ngx.log(ngx.ERR, "copy" .. prevWord .. "(" .. fi .. ":" .. fi+l .. "," .. cpFromOffset .. ":" .. cpToOffset ..")" ..l)
            end
            for i=1, l do
                f[fi] = strsub(prevWord, i, i)
                fi = fi + 1
            end
            --copy(f[fi:fi+l], prevWord)
            --fi = fi + l
            cpFromOffset = cpToOffset
            if wordIn(prevWord, {"in", "value", "values"}) and sqlState ~= onDupeKeyUpdate then
                --// IN ()     -> in(?+)
                --// VALUES () -> values(?+)
                addSpace = false
                s = inValues
                sqlState = inValues
            elseif addSpace then
                if Debug then
                    ngx.log(ngx.ERR, "Add space")
                end
                f[fi] = ' '
                fi = fi + 1
                cpFromOffset = cpFromOffset + 1
                addSpace = false
            end
        end

        pr = r
        ::CONTINUE::
    end

    --// Remove trailing spaces.
    while true
    do
        fi = fi-1
        if fi > 0 and isSpace(strbyte(f[fi])) == false then
            break
        end
    end

    --// Return the fingerprint.
    return strsub(tabconcat(f), 1, fi)
end

return _M