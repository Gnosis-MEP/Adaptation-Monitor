import copy
import threading
import time

from event_service_utils.logging.decorators import timer_logger

from event_service_utils.services.event_driven import BaseEventDrivenCMDService, tags, EVENT_ID_TAG
from event_service_utils.tracing.jaeger import init_tracer
from walrus.containers import make_python_attr

from .streams import get_total_pending_cg_stream_with_lua, register_lua_script


class AdaptationMonitor(BaseEventDrivenCMDService):
    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 stream_factory,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(AdaptationMonitor, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id']

        self.count_stream_size_xrange_script = None
        self.published_empty_service_workers_stream = False
        self.services_to_monitor = {}

    def publish_service_workers_stream_monitored(self, service_workers):
        new_event_data = {
            'service_workers': service_workers
        }
        new_event_data['id'] = self.service_based_random_event_id()
        self.publish_event_type_to_stream(event_type='ServiceWorkersStreamMonitored', new_event_data=new_event_data)

    def process_new_service_worker_monitoring(self, worker, service_type, stream_key):
        service_dict = self.services_to_monitor.setdefault(service_type, {'workers': {}})
        service_dict['workers'][stream_key] = worker

    def _repeat_event_type_after_time(self, event_type, event_data, wait_time):
        self.logger.debug(f'Waiting for {wait_time}s before repeating action...')
        time.sleep(wait_time)

        self.logger.debug(
            f'Done waiting, resending event type "{event_type}" to self cmd stream to repeat action: {event_data}'
        )
        # not using traces on purpose, I think this wont be much usefull for us
        # and it may cause a very weird gigantic trace on jaeger

        self.publish_event_type_to_stream_without_trace(
            event_type=event_type,
            new_event_data=event_data
        )

    def execute_in_background(self, method, args, kwargs=None):
        bg_thread = threading.Thread(target=method, args=args, kwargs=kwargs)
        bg_thread.daemon = True
        bg_thread.start()

    def get_count_stream_size_xrange_script(self):
        if self.count_stream_size_xrange_script is None:
            self.count_stream_size_xrange_script = register_lua_script(self.stream_factory.redis_db)
        return self.count_stream_size_xrange_script

    def calculate_stream_pending_len(self, stream_key):
        lua_script = self.get_count_stream_size_xrange_script()
        return get_total_pending_cg_stream_with_lua(
            self.stream_factory.redis_db, lua_script, stream_key
        )

    def process_stream_size_monitoring(self):
        service_workers = copy.deepcopy(self.services_to_monitor)
        for service_type, service in service_workers.items():
            workers = service['workers']
            for stream_key, worker in workers.items():
                queue_size = self.calculate_stream_pending_len(stream_key)
                worker['queue_size'] = queue_size
                queue_limit = worker.get('queue_limit', None)
                queue_space = None
                queue_space_percent = None
                if queue_limit is not None:
                    queue_space = queue_limit - queue_size
                    queue_space_percent = queue_space / queue_limit
                worker['queue_limit'] = queue_limit
                worker['queue_space'] = queue_space
                worker['queue_space_percent'] = queue_space_percent
            total_number_workers = len(workers.keys())
            service['total_number_workers'] = total_number_workers

        # don't publish empty dict more than once
        if len(service_workers.keys()) == 0:
            if self.published_empty_service_workers_stream:
                return
            else:
                self.published_empty_service_workers_stream = True
        else:
            self.published_empty_service_workers_stream = False

        self.publish_service_workers_stream_monitored(service_workers)

    def process_stream_size_monitoring_bg_retry_once_if_exception(self):
        try:
            self.process_stream_size_monitoring()
        except:
            time.sleep(0.01)
            self.process_stream_size_monitoring()

    def monitor_stream_size_and_repeat_in_bg(self, event_type, event_data, repeat_after_time):
        try:
            self.execute_in_background(
                method=self.process_stream_size_monitoring_bg_retry_once_if_exception,
                args=()
            )
        except Exception as e:
            self.logger.exception(e)
        self.execute_in_background(
            method=self._repeat_event_type_after_time,
            args=(event_type, event_data, repeat_after_time),
            kwargs=None
        )

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(AdaptationMonitor, self).process_event_type(event_type, event_data, json_msg):
            return False

        if event_type == 'ServiceWorkerAnnounced':
            worker = event_data['worker']
            service_type = worker['service_type']
            stream_key = worker['stream_key']
            self.process_new_service_worker_monitoring(worker=worker, service_type=service_type, stream_key=stream_key)

        elif event_type == 'RepeatMonitorStreamsSizeRequested':
            repeat_after_time = event_data['repeat_after_time']
            self.monitor_stream_size_and_repeat_in_bg(event_type, event_data, repeat_after_time)
        # elif event_type == 'QueryCreated':
        #     pass

    def log_state(self):
        super(AdaptationMonitor, self).log_state()
        self._log_dict('Services To Monitor', self.services_to_monitor)

    def repeat_services_monitoring_for_stream_check(self):
        time.sleep(1)
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'repeat_after_time': 1,
        }

        self.publish_event_type_to_stream_without_trace(
            event_type='RepeatMonitorStreamsSizeRequested',
            new_event_data=new_event_data
        )

    def publish_event_type_to_stream_without_trace(self, event_type, new_event_data):
        pub_stream = self.pub_event_stream_map.get(event_type)
        if pub_stream is None:
            raise RuntimeError(f'No publishing stream defined for event type: {event_type}!')

        self.logger.info(f'Publishing without trace "{event_type}" entity: {new_event_data}')
        pub_stream.write_events(self.default_event_serializer(new_event_data))

    def run(self):
        super(AdaptationMonitor, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.cmd_thread.start()

        self.repeat_services_monitoring_for_stream_check()
        self.cmd_thread.join()
