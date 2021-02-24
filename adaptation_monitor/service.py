import hashlib
import threading
import time

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService, tags, EVENT_ID_TAG
from event_service_utils.tracing.jaeger import init_tracer
from walrus.containers import make_python_attr

from .streams import get_total_pending_cg_stream
from .conf import MOCKED_WORKERS_ENERGY_USAGE_DICT


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

        self.preprocessor_cmd_stream_key = 'pp-cmd'
        self.service_registry_cmd_key = 'sr-cmd'

        self.analyser_cmd_stream_key = 'adpa-cmd'
        self.knowledge_cmd_stream_key = 'adpk-cmd'
        self.monitored_stream_keys = [
            self.preprocessor_cmd_stream_key,
            self.service_registry_cmd_key
        ]
        self.monitored_streams = self.stream_factory.create(key=self.monitored_stream_keys, stype='manyKeyConsumer')
        self.knowledge_cmd_stream = self.stream_factory.create(key=self.knowledge_cmd_stream_key, stype='streamOnly')
        self.analyser_cmd_stream = self.stream_factory.create(key=self.analyser_cmd_stream_key, stype='streamOnly')
        self.base_namespace = 'gnosis-mep'

        self.services_to_monitor = {}

    # def _query_preprocessing_race_condition_workaround(self, event_data, event_type):
    #     """This is a ugly workaround for the fact that now the query event can be
    #     reived after the preprocessing event.
    #     In this case it will hold in the preprocessing event and only send it once a query event is received
    #     """
    #     if event_data.get('action', '') == 'startPreprocessing':
    #         self.last_preprocessing_event = event_data
    #     elif event_data.get('action', '') == 'updateControlFlow':
    #         pass
    #     self.last_preprocessing_event = None

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

    def send_data_to_analyser(self, json_ld_entity, action, change_type):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'entity': json_ld_entity,
            'action': action,
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

    def _create_query_qos_policies_entity(self, query_id, qos_policies):
        qos_entity_id = f'{query_id}-qos_policies'
        qos_policies['query_id'] = f'gnosis-mep:subscriber_query/{query_id}'

        qos_json_ld_entity = self.prepare_entity_to_knowledge_manager(
            qos_policies, namespace='query_qos_policy', entity_id=qos_entity_id)
        return qos_json_ld_entity

    def process_update_controlflow_monitoring(self, event_data):
        # qos_policies = event_data.pop('qos_policies', {})

        # qos_json_ld_entity = self._create_query_qos_policies_entity(event_data['query_id'], qos_policies)
        qos_policies = event_data.pop('qos_policies', {})
        qos_policies = [[f"{k}:{v}"] for k, v in qos_policies.items()]
        event_data['qos_policies'] = qos_policies

        json_ld_entity = self.prepare_entity_to_knowledge_manager(
            event_data, namespace='subscriber_query', entity_id=event_data['query_id'])

        entity_action_change_type = 'addEntity'

        # graph_entities = [qos_json_ld_entity, json_ld_entity, ]
        # json_ld_entity_graph = {
        #     '@graph': service_monitoring_entities
        # }
        self.send_data_to_knowledge_manager(json_ld_entity, action=entity_action_change_type)
        self.send_data_to_analyser(json_ld_entity, action='notifyChangedEntity', change_type=entity_action_change_type)

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

        self.send_data_to_analyser(json_ld_entity, action='notifyChangedEntity', change_type=entity_action_change_type)

    def process_new_service_worker_monitoring(self, event_data):
        worker = event_data['worker']
        service_type = worker['service_type']
        stream_key = worker['stream_key']
        service_dict = self.services_to_monitor.setdefault(service_type, {'workers': {}})
        service_dict['workers'][stream_key] = worker

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

    def process_stream_size_monitoring(self):
        service_monitoring_entities = []
        replaces = {}
        for service_type, service in self.services_to_monitor.items():
            workers = service['workers']
            for stream_key, worker in workers.items():
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
        if service_monitoring_entities == []:
            return

        json_ld_entity_graph = {
            '@graph': service_monitoring_entities
        }
        entity_action_change_type = 'updateEntity'
        self.send_data_to_knowledge_manager(json_ld_entity_graph, action=entity_action_change_type, replaces=replaces)
        self.send_data_to_analyser(
            json_ld_entity_graph, action='notifyChangedEntityGraph', change_type=entity_action_change_type)

    def monitor_stream_size_and_repeat(self, event_data):
        self.process_stream_size_monitoring()
        repeat_after_time = event_data['repeat_after_time']
        self.background_schedule_repeat_action(event_data, repeat_after_time)

    @timer_logger
    def process_action(self, action, event_data, json_msg):
        if not super(AdaptationMonitor, self).process_action(action, event_data, json_msg):
            return False

        if action == 'repeatMonitorStreamsSize':
            self.monitor_stream_size_and_repeat(event_data)

        # This is very bad in here, but since we'll probably change the overall behaviour of the system
        # and having this sent here would be the simplest to change later, we are doing it.
        # in the current implementation it makes more sense to have the actions that the monitor receives being only
        # monitoring actions, instead of the monitored events it self.
        if event_data['action'] == 'updateControlFlow':
            self.process_update_controlflow_monitoring(event_data)

    @timer_logger
    def process_monitoring_event(self, event_data, json_msg):
        if not self.event_validation_fields(event_data, self.cmd_validation_fields):
            self.logger.info(f'Ignoring bad event data: {event_data}')
            return False
        self.logger.debug(f'Processing new monitoring event: {event_data}')

        stream_key = event_data.pop('stream_key')
        if stream_key == self.preprocessor_cmd_stream_key:
            if event_data['action'] == 'startPreprocessing':
                self.process_start_preprocessing_monitoring(event_data)
        elif stream_key == self.service_registry_cmd_key:
            if event_data['action'] == 'addServiceWorker':
                self.process_new_service_worker_monitoring(event_data)

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

    def repeat_services_monitoring_for_stream_check(self):
        time.sleep(1)
        event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'repeatMonitorStreamsSize',
            'repeat_after_time': 1,
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

        self.repeat_services_monitoring_for_stream_check()
        self.cmd_thread.join()
        self.data_thread.join()
        self.monitoring_thread.join()
