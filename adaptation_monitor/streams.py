import redis

def get_total_pending_cg_stream(redis_db, stream_key):
    bad_return_value = redis_db.xlen(stream_key)
    cg_name = f'cg-{stream_key}'
    try:
        cgroups_info = redis_db.xinfo_groups(stream_key)
        if not cgroups_info:
            return bad_return_value

        cgroup = None
        for cg in cgroups_info:
            if cg['name'] == cg_name.encode('utf-8'):
                cgroup = cg
                break

        if not cgroup:
            return bad_return_value

        last_delivered_id = cgroup['last-delivered-id']

        if last_delivered_id is None:
            return bad_return_value

        not_consumed_stream_events_list = redis_db.xread({stream_key: last_delivered_id})
        if len(not_consumed_stream_events_list) == 0:
            return 0

        return len(not_consumed_stream_events_list[0][1])
    except redis.ResponseError:
        total_pending = bad_return_value

    return total_pending


COUNT_STREAM_WITH_RANGE_LUA_SCRIPT = """
local T = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[2])
local count = 0
local first
for i, val in pairs(T) do
 count = count + 1
end
if count == 0 then
    first = "-"
else
    first = T[1][1]
end
return {count, first}
"""


def register_lua_script(redis_db):
    return redis_db.register_script(COUNT_STREAM_WITH_RANGE_LUA_SCRIPT)


def get_total_pending_cg_stream_with_lua(redis_db, lua_script, stream_key):
    bad_return_value = redis_db.xlen(stream_key)
    cg_name = f'cg-{stream_key}'
    try:
        cgroups_info = redis_db.xinfo_groups(stream_key)
        if not cgroups_info:
            return bad_return_value

        cgroup = None
        for cg in cgroups_info:
            if cg['name'] == cg_name.encode('utf-8'):
                cgroup = cg
                break

        if not cgroup:
            return bad_return_value

        last_delivered_id = cgroup['last-delivered-id']

        if last_delivered_id is None:
            return bad_return_value

        not_consumed_events_count, first_counted = lua_script(keys=[stream_key], args=[last_delivered_id, '+'])
        if not_consumed_events_count > 0 and last_delivered_id == first_counted:
            not_consumed_events_count -= 1

        return not_consumed_events_count
    except redis.ResponseError:
        total_pending = bad_return_value

    return total_pending
