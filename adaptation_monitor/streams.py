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
