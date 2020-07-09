import functools
import hashlib
import threading
import time

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService, tags, EVENT_ID_TAG
from event_service_utils.tracing.jaeger import init_tracer
from walrus.containers import make_python_attr

from .streams import get_total_pending_cg_stream


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
        self.base_namespace = 'gnosis-mep'

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(AdaptationMonitor, self).process_data_event(event_data, json_msg):
            return False
        # do something here
        pass

    def send_data_to_knowledge_manager(self, json_ld_entity, action, replaces=None):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'entity': json_ld_entity,
            'action': action
        }
        if replaces:
            new_event_data['replaces'] = replaces

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

    def get_namespace_attribute(self, entity_namespace, attribute):
        return f'{entity_namespace}#{attribute}'

    def prepare_entity_to_knowledge_manager(self, event_data, namespace, entity_id):
        clean_event_data = event_data.copy()
        clean_event_data.pop('id', None)
        clean_event_data.pop('tracer', None)
        entity_namespace = f'{self.base_namespace}:{namespace}'
        entity = {
            '@id': f'{self.base_namespace}:{namespace}/{entity_id}',
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

    def _repeat_action_after_time(self, event_data, wait_time):
        self.logger.debug(f'Waiting for {wait_time}s before repeating action...')
        time.sleep(wait_time)

        self.logger.debug(f'Done waiting, resending event to self cmd stream to repeat action: {event_data}')
        # not using traces on purpose, I think this wont be much usefull for us
        # and it may cause a very wied gigantic trace on jaeger
        self.service_cmd.write_events(self.default_event_serializer(event_data))

    def background_schedule_repeat_action(self, event_data, repeat_after_time):
        bg_thread = threading.Thread(target=self._repeat_action_after_time, args=(event_data, repeat_after_time))
        bg_thread.daemon = True
        bg_thread.start()

    def calculate_stream_pending_len(self, stream_key):
        return get_total_pending_cg_stream(self.stream_factory.redis_db, stream_key)

    # @functools.lru_cache(maxsize=5)
    # def get_cached_stream_connection(self, stream_key):
    #     return self.stream_factory.create(stream_key, stype='streamOnly')

    # def calculate_stream_len(self, stream_key):
    #     stream = self.get_cached_stream_connection(stream_key)
    #     return stream.single_io_stream.length()

    def process_stream_size_monitoring(self, event_data):
        services = event_data['services']
        service_monitoring_entities = []
        replaces = {}
        for service_type, service in services.items():
            workers = service['workers']
            for worker in workers:
                stream_key = worker['stream_key']
                queue_limit = worker['queue_limit']
                queue_size = self.calculate_stream_pending_len(stream_key)
                # queue_size = 10
                queue_space = queue_limit - queue_size
                queue_space_percent = queue_space / queue_limit
                worker['queue_space'] = queue_space
                # worker['queue_size'] = queue_size
                worker['queue_space_percent'] = queue_space_percent
                json_ld_entity = self.prepare_entity_to_knowledge_manager(
                    worker, namespace='service_worker', entity_id=stream_key
                )
                # we want to replace the queue_space and percent values
                # since they should be different and we don't want to have a list of values
                # for this attribute
                replaces[json_ld_entity['@id']] = [
                    self.get_namespace_attribute(json_ld_entity['@type'], 'queue_space'),
                    self.get_namespace_attribute(json_ld_entity['@type'], 'queue_space_percent'),
                ]
                service_monitoring_entities.append(json_ld_entity)

        json_ld_entity_graph = {
            '@graph': service_monitoring_entities
        }
        self.send_data_to_knowledge_manager(json_ld_entity_graph, action='updateEntity', replaces=replaces)

    def monitor_stream_size_and_repeat(self, event_data):
        self.process_stream_size_monitoring(event_data)
        repeat_after_time = event_data['repeat_after_time']
        self.background_schedule_repeat_action(event_data, repeat_after_time)

    @timer_logger
    def process_action(self, action, event_data, json_msg):
        if not super(AdaptationMonitor, self).process_action(action, event_data, json_msg):
            return False

        if action == 'repeatMonitorStreamsSize':
            self.monitor_stream_size_and_repeat(event_data)

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
                finally:
                    # we are always ack the events, even if they fail.
                    # in a better world we would actually do some treatments to
                    # see if the event should be re-processed or not, before ack.
                    cg_stream = getattr(self.monitored_streams.input_consumer_group, make_python_attr(stream_key))
                    cg_stream.ack(event_id)

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

    def mocked_services_anouncement_for_stream_check(self):
        "Using this while we don't have a discovery service that will anounce this info"
        time.sleep(1)
        event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'repeatMonitorStreamsSize',
            'repeat_after_time': 1,
            'services': {
                'ObjectDetection': {
                    'workers': [
                        {
                            'stream_key': 'object-detection-ssd-gpu-data',
                            'queue_limit': 100
                        },
                        {
                            'stream_key': 'object-detection-ssd-data',
                            'queue_limit': 100
                        }
                    ]
                },
                'ColorDetection': {
                    'workers': [
                        {
                            'stream_key': 'color-detection-data',
                            'queue_limit': 100
                        }
                    ]
                }
            }
        }
        self.service_cmd.write_events(self.default_event_serializer(event_data))

    def run(self):
        super(AdaptationMonitor, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.monitoring_thread = threading.Thread(target=self.run_forever, args=(self.process_monitoring,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.monitoring_thread.start()

        self.mocked_services_anouncement_for_stream_check()
        self.cmd_thread.join()
        self.data_thread.join()
        self.monitoring_thread.join()
