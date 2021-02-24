import os

from decouple import config

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SOURCE_DIR)

REDIS_ADDRESS = config('REDIS_ADDRESS', default='localhost')
REDIS_PORT = config('REDIS_PORT', default='6379')

TRACER_REPORTING_HOST = config('TRACER_REPORTING_HOST', default='localhost')
TRACER_REPORTING_PORT = config('TRACER_REPORTING_PORT', default='6831')


def string_to_dict_cast(str_value):
    if str_value == "":
        return {}
    final_dict = {}
    for service_str in str_value.split(';'):
        service_type, workers_str_list = service_str.split(':')
        workers = {}
        for worker_str in workers_str_list.split(','):
            stream_key, queue_limit, energy_consumption = worker_str.split('/')
            worker_dict = {
                'service_type': service_type,
                'stream_key': stream_key,
                'queue_limit': int(queue_limit),
                'energy_consumption': float(energy_consumption),
            }
            workers[stream_key] = worker_dict
        final_dict[service_type] = {
            'workers': workers
        }
    return final_dict


_DEFAULT_MOCKED_STR = "ObjectDetection:object-detection-ssd-gpu-data/100/163.8,object-detection-ssd-data/100/188.0"

MOCKED_WORKERS_ENERGY_USAGE_DICT = config('MOCKED_WORKERS_ENERGY_USAGE_DICT', cast=string_to_dict_cast,
                                          default=_DEFAULT_MOCKED_STR)

SERVICE_STREAM_KEY = config('SERVICE_STREAM_KEY')
SERVICE_CMD_KEY = config('SERVICE_CMD_KEY')

LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')
