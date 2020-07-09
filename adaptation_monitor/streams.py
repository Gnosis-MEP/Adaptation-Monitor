import redis
from event_service_utils.streams.redis import RedisStreamFactory as BaseRedisFactory


class ManyKeyConsumerGroup():
    def __init__(self, redis_db, keys, max_stream_length=10, block=0):
        self.block = block
        self.redis_db = redis_db
        self.input_consumer_group = self._get_many_stream_consumer_group(keys)
        self.max_stream_length = max_stream_length

    def _get_many_stream_consumer_group(self, keys):
        single_keys_string = '-'.join(keys)
        group_name = 'cg-%s' % single_keys_string
        consumer_group = self.redis_db.consumer_group(group_name, keys)
        consumer_group.create()
        consumer_group.set_id(id='$')
        return consumer_group

    def read_stream_events_list(self, count=1):
        yield from self.input_consumer_group.read(count=count, block=self.block)


class RedisStreamFactory(BaseRedisFactory):

    def create(self, key, stype='streamAndConsumer'):
        stream = super(RedisStreamFactory, self).create(key, stype)
        if stream:
            return stream
        elif stype == 'manyKeyConsumer':
            return ManyKeyConsumerGroup(
                redis_db=self.redis_db, keys=key, max_stream_length=self.max_stream_length, block=self.block)


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
