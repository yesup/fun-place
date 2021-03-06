function count(s)
    function mapper(rec)
            return 1
    end
    local function reducer(v1, v2)
        return v1 + v2
    end
    return s : map(mapper) : reduce(reducer)
end

function updateRecord(r, binName, newValue)
    record.set_ttl(r, 600)
    r[binName] = newValue
    aerospike:update(r)
end