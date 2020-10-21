from unittest.mock import patch

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from mocked_streams import ManyKeyConsumerMockedStreamFactory

from adaptation_monitor.service import AdaptationMonitor

from adaptation_monitor.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
)


class TestAdaptationMonitor(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key': SERVICE_CMD_KEY,
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = AdaptationMonitor
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        SERVICE_CMD_KEY: [],
    }

    def prepare_mocked_stream_factory(self, mocked_dict):
        self.stream_factory = ManyKeyConsumerMockedStreamFactory(mocked_dict=self.mocked_streams_dict)

    @patch('adaptation_monitor.service.AdaptationMonitor.process_action')
    def test_process_cmd_should_call_process_action(self, mocked_process_action):
        action = 'someAction'
        event_data = {
            'id': 1,
            'action': action,
            'some': 'stuff'
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        mocked_process_action.__name__ = 'process_action'

        self.service.service_cmd.mocked_values = [msg_tuple]
        self.service.process_cmd()
        self.assertTrue(mocked_process_action.called)
        self.service.process_action.assert_called_once_with(action=action, event_data=event_data, json_msg=msg_tuple[1])

    @patch('adaptation_monitor.service.AdaptationMonitor.process_update_controlflow_monitoring')
    def test_process_action_should_process_add_query_monitoring(self, mocked_up_ctrlflow_mon):
        action = 'updateControlFlow'
        event_data = {
            'id': '123',
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
            'action': action,
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        json_msg = msg_tuple[1]
        self.service.process_action(event_data['action'], event_data, json_msg)

        self.assertTrue(mocked_up_ctrlflow_mon.called)
        self.service.process_update_controlflow_monitoring.assert_called_once_with(event_data)

    @patch('adaptation_monitor.service.AdaptationMonitor.process_start_preprocessing_monitoring')
    def test_process_action_should_process_start_preprocessing_monitoring(self, mocked_start_pp_mon):
        action = 'startPreprocessing'
        event_data = {
            "id": "abc-123abc-123abc-123abc-123abc-123abc-123",
            "publisher_id": "44d7985a-e41e-4d02-a772-a8f7c1c69124",
            "source": "rtmp://localhost/live/mystream",
            "resolution": "640x480",
            "fps": "30",
            "buffer_stream_key": "buffer-stream-key",
            "query_ids": ["query-id1", "query-id2"],
            'action': action,
            'stream_key': self.service.preprocessor_cmd_stream_key
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        json_msg = msg_tuple[1]
        self.service.process_monitoring_event(event_data, json_msg)

        self.assertTrue(mocked_start_pp_mon.called)
        self.service.process_start_preprocessing_monitoring.assert_called_once_with(event_data)
