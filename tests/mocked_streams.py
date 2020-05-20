from event_service_utils.tests.mocked_streams import MockedStreamFactory, MockedStreamAndConsumer


class ManyKeyConsumerGroupMocked():
    def __init__(self, keys, mocked_values):
        self.keys = keys
        self._update_mocked_values(mocked_values)
        # self.mocked_values = {
        #     # key: MockedStreamAndConsumer(key=key, mocked_values=mocked_dict[key]) for key in keys
        # }

    def _update_mocked_values(self, mocked_values):
        self.mocked_values = {
            key: MockedStreamAndConsumer(key=key, mocked_values=mocked_values[key])
            for key, value in mocked_values.items()
        }

    def read_stream_events_list(self, count=1):
        for stream_key, mocked_stream in self.mocked_values.items():
            event_list = list(mocked_stream.read_events(count=count))
            if event_list:
                yield stream_key, event_list


class ManyKeyConsumerMockedStreamFactory(MockedStreamFactory):

    def __init__(self, mocked_dict):
        self.mocked_dict = mocked_dict

    def create(self, key, stype=None):
        if stype == 'manyKeyConsumer':
            return ManyKeyConsumerGroupMocked(keys=key, mocked_values=self.mocked_dict)
        else:
            return super(ManyKeyConsumerMockedStreamFactory, self).create(key=key, stype=stype)
