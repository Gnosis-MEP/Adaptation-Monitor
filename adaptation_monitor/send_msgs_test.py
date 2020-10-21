#!/usr/bin/env python
import uuid
import json
from event_service_utils.streams.redis import RedisStreamFactory

from adaptation_monitor.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
)


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def new_action_msg(action, event_data):
    event_data['action'] = action
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}


def send_action_msgs(service_cmd):
    msg_1 = new_action_msg(
        'updateControlFlow',
        {
            "control_flow": {
                "publisher 1": [
                    ["object-detection-data"],
                    ["wa-data"]
                ]
            },
            "query_id": "ab35e84a215f0f711ed629c2abb9efa0",
            "publisher_id": "publisher 1",
            "destination_id": [
                "object-detection-data",
                "wa-data"
            ],
        }
    )
    # msg_2 = new_action_msg(
    #     'repeatMonitorStreamsSize',
    #     {
    #         'repeat_after_time': 1,
    #         'services': {
    #             'ObjectDetection': {
    #                 'workers': [
    #                     {
    #                         'stream_key': 'object-detection-ssd-gpu-data',
    #                         'queue_limit': 100
    #                     },
    #                     {
    #                         'stream_key': 'object-detection-ssd-data',
    #                         'queue_limit': 100
    #                     }
    #                 ]
    #             },
    #             'ColorDetection': {
    #                 'workers': [
    #                     {
    #                         'stream_key': 'color-detection-data',
    #                         'queue_limit': 100
    #                     }
    #                 ]
    #             }
    #         }
    #     }
    # )

    # msg_3 = new_action_msg(
    #     'whatever',
    #     {
    #         'etc': '123456',
    #     }
    # )

    print(f'Sending msg {msg_1}')
    service_cmd.write_events(msg_1)
    # print(f'Sending msg {msg_2}')
    # service_cmd.write_events(msg_2)
    # print(f'Sending msg {msg_3}')
    # service_cmd.write_events(msg_2)

def send_data_msg(service_stream):
    data_msg = {
        'event': json.dumps(
            {
                'id': str(uuid.uuid4()),
                'some': 'data'
            }
        )
    }
    print(f'Sending msg {data_msg}')
    service_stream.write_events(data_msg)


def main():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service_cmd = stream_factory.create(SERVICE_CMD_KEY, stype='streamOnly')
    service_stream = stream_factory.create(SERVICE_STREAM_KEY, stype='streamOnly')
    monitor_stream = stream_factory.create('ed-cmd', stype='streamOnly')
    import ipdb; ipdb.set_trace()
    # send_action_msgs(service_cmd)
    send_action_msgs(monitor_stream)
    # send_action_msgs(monitor_stream)
    # send_data_msg(service_stream)


if __name__ == '__main__':
    main()
