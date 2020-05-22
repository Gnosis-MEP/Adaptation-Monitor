import hashlib
import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService, tags, EVENT_ID_TAG
from event_service_utils.tracing.jaeger import init_tracer


class AdaptationMonitor(BaseTracerService):
    def __init__(self,
                 service_stream_key, service_cmd_key,
                 stream_factory,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(AdaptationMonitor, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key=service_cmd_key,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id', 'action']
        self.data_validation_fields = ['id']

        self.client_manager_cmd_stream_key = 'cm-cmd'
        self.preprocessor_cmd_stream_key = 'pp-cmd'
        self.analyser_cmd_stream_key = 'adpa-cmd'
        self.knowledge_cmd_stream_key = 'adpk-cmd'
        self.monitored_stream_keys = [
            # client_manager_cmd_stream_key,
            self.client_manager_cmd_stream_key,
            self.preprocessor_cmd_stream_key
        ]
        self.monitored_streams = self.stream_factory.create(key=self.monitored_stream_keys, stype='manyKeyConsumer')
        self.knowledge_cmd_stream = self.stream_factory.create(key=self.knowledge_cmd_stream_key, stype='streamOnly')
        self.analyser_cmd_stream = self.stream_factory.create(key=self.analyser_cmd_stream_key, stype='streamOnly')

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(AdaptationMonitor, self).process_data_event(event_data, json_msg):
            return False
        # do something here
        pass

    def send_data_to_knowledge_manager(self, json_ld_entity, action):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'entity': json_ld_entity,
            'action': action
        }

        self.logger.debug(f'Sending data "{new_event_data}" to K')
        self.write_event_with_trace(new_event_data, self.knowledge_cmd_stream)

    def send_data_to_analyser(self, json_ld_entity, change_type):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'entity': json_ld_entity,
            'action': 'notifyChangedEntity',
            'change_type': change_type
        }

        self.logger.debug(f'Sending data "{new_event_data}" to Analyser')
        self.write_event_with_trace(new_event_data, self.analyser_cmd_stream)

    def prepare_entity_to_knowledge_manager(self, event_data, namespace, entity_id):
        clean_event_data = event_data.copy()
        clean_event_data.pop('id', None)
        clean_event_data.pop('tracer', None)
        base_namespace = 'gnosis-mep'
        entity_namespace = f'{base_namespace}:{namespace}'
        entity = {
            '@id': f'{base_namespace}:{namespace}/{entity_id}',
            '@type': entity_namespace
        }
        entity.update({
            f'{entity_namespace}#{k}': v for k, v in clean_event_data.items()
        })
        return entity

    def process_add_query_monitoring(self, event_data):
        query_uid = hashlib.md5(f"{event_data['subscriber_id']}_{event_data['query_num']}".encode('utf-8')).hexdigest()
        json_ld_entity = self.prepare_entity_to_knowledge_manager(
            event_data, namespace='subscriber_query', entity_id=query_uid)
        self.send_data_to_knowledge_manager(json_ld_entity, action='addEntity')

    def process_start_preprocessing_monitoring(self, event_data):
        event_data['query_ids'] = [f'gnosis-mep:subscriber_query/{q}' for q in event_data['query_ids']]

        json_ld_entity = self.prepare_entity_to_knowledge_manager(
            event_data, namespace='buffer_stream', entity_id=event_data['buffer_stream_key']
        )
        json_ld_entity['@context'] = {
            'gnosis-mep:buffer_stream#query_ids': {'@type': '@id'},
        }
        entity_action_change_type = 'addEntity'
        self.send_data_to_knowledge_manager(json_ld_entity, action=entity_action_change_type)

        self.send_data_to_analyser(json_ld_entity, change_type=entity_action_change_type)

    def process_action(self, action, event_data, json_msg):
        if not super(AdaptationMonitor, self).process_action(action, event_data, json_msg):
            return False
        # elif action == 'otherAction':
        #     # do some other action
        #     pass

    @timer_logger
    def process_monitoring_event(self, event_data, json_msg):
        if not self.event_validation_fields(event_data, self.cmd_validation_fields):
            self.logger.info(f'Ignoring bad event data: {event_data}')
            return False
        self.logger.debug(f'Processing new monitoring event: {event_data}')

        stream_key = event_data.pop('stream_key')
        if stream_key == self.client_manager_cmd_stream_key:
            if event_data['action'] == 'addQuery':
                self.process_add_query_monitoring(event_data)

        if stream_key == self.preprocessor_cmd_stream_key:
            if event_data['action'] == 'startPreprocessing':
                self.process_start_preprocessing_monitoring(event_data)

    def process_monitoring(self):
        stream_sources_events = list(self.monitored_streams.read_stream_events_list(count=10))
        if stream_sources_events:
            self.logger.debug(f'Processing Monitoring.. {stream_sources_events}')

        for stream_key_bytes, event_list in stream_sources_events:
            stream_key = stream_key_bytes
            if type(stream_key_bytes) == bytes:
                stream_key = stream_key_bytes.decode('utf-8')
            for event_tuple in event_list:
                event_id, json_msg = event_tuple
                try:
                    event_data = self.default_event_deserializer(json_msg)
                    event_data.update({
                        'stream_key': stream_key
                    })
                    self.process_monitoring_event_wrapper(event_data, json_msg)
                except Exception as e:
                    self.logger.error(f'Error processing {json_msg}:')
                    self.logger.exception(e)

    def process_monitoring_event_wrapper(self, event_data, json_msg):
        self.event_trace_for_method_with_event_data(
            method=self.process_monitoring_event,
            method_args=(),
            method_kwargs={
                'event_data': event_data,
                'json_msg': json_msg
            },
            get_event_tracer=True,
            tracer_tags={
                tags.SPAN_KIND: tags.SPAN_KIND_CONSUMER,
                EVENT_ID_TAG: event_data['id'],
            }
        )

    def log_state(self):
        super(AdaptationMonitor, self).log_state()
        self.logger.info(f'My service name is: {self.name}')

    def run(self):
        super(AdaptationMonitor, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.monitoring_thread = threading.Thread(target=self.run_forever, args=(self.process_monitoring,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.monitoring_thread.start()

        self.cmd_thread.join()
        self.data_thread.join()
        self.monitoring_thread.join()
