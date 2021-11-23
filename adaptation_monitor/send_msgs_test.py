#!/usr/bin/env python
import uuid
import json
from event_service_utils.streams.redis import RedisStreamFactory

from adaptation_monitor.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_STREAM_KEY,
    LISTEN_EVENT_TYPE_REPEAT_MONITOR_STREAMS_SIZE_REQUESTED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_ANNOUNCED,
)


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def new_msg(event_data):
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}


def main():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    addworker_cmd = stream_factory.create(LISTEN_EVENT_TYPE_SERVICE_WORKER_ANNOUNCED, stype='streamOnly')
    addworker_cmd.write_events(
        new_msg(
            {
                'worker': {
                    'service_type': 'ObjectDetection',
                    'stream_key': 'objworker-key',
                    'queue_limit': 100,
                }
            }
        )
    )
    addworker_cmd.write_events(
        new_msg(
            {
                'worker': {
                    'service_type': 'ColorDetection',
                    'stream_key': 'clrworker-key',
                    'queue_limit': 100,
                }
            }
        )
    )
    import ipdb; ipdb.set_trace()

    repeat_cmd = stream_factory.create(LISTEN_EVENT_TYPE_REPEAT_MONITOR_STREAMS_SIZE_REQUESTED, stype='streamOnly')

    # repeat_cmd.write_events(
    #     new_msg(
    #         {
    #             'repeat_after_time': 1,
    #             # 'services': {
    #             #     'ObjectDetection': {
    #             #         'workers': [
    #             #             {
    #             #                 'stream_key': 'object-detection-ssd-gpu-data',
    #             #                 'queue_limit': 100
    #             #             },
    #             #             {
    #             #                 'stream_key': 'object-detection-ssd-data',
    #             #                 'queue_limit': 100
    #             #             }
    #             #         ]
    #             #     },
    #             #     'ColorDetection': {
    #             #         'workers': [
    #             #             {
    #             #                 'stream_key': 'color-detection-data',
    #             #                 'queue_limit': 100
    #             #             }
    #             #         ]
    #             #     }
    #             # }
    #         }
    #     )
    # )
    worker_stream = stream_factory.create('objworker-key', stype='streamOnly')

    worker_stream.write_events(
        new_msg(
            {
                'msg': 'a'
            }
        )
    )

if __name__ == '__main__':
    main()
