import os

from decouple import config

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SOURCE_DIR)

REDIS_ADDRESS = config('REDIS_ADDRESS', default='localhost')
REDIS_PORT = config('REDIS_PORT', default='6379')

TRACER_REPORTING_HOST = config('TRACER_REPORTING_HOST', default='localhost')
TRACER_REPORTING_PORT = config('TRACER_REPORTING_PORT', default='6831')

SERVICE_STREAM_KEY = config('SERVICE_STREAM_KEY')

# LISTEN_EVENT_TYPE_QUERY_CREATED = config('LISTEN_EVENT_TYPE_QUERY_CREATED')
# LISTEN_EVENT_TYPE_QUERY_REMOVED = config('LISTEN_EVENT_TYPE_QUERY_REMOVED')
LISTEN_EVENT_TYPE_SERVICE_WORKER_ANNOUNCED = config('LISTEN_EVENT_TYPE_SERVICE_WORKER_ANNOUNCED')
LISTEN_EVENT_TYPE_REPEAT_MONITOR_STREAMS_SIZE_REQUESTED = config('LISTEN_EVENT_TYPE_REPEAT_MONITOR_STREAMS_SIZE_REQUESTED')

SERVICE_CMD_KEY_LIST = [
    # LISTEN_EVENT_TYPE_QUERY_CREATED,
    # LISTEN_EVENT_TYPE_QUERY_REMOVED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_ANNOUNCED,
    LISTEN_EVENT_TYPE_REPEAT_MONITOR_STREAMS_SIZE_REQUESTED,
]

PUB_EVENT_TYPE_REPEAT_MONITOR_STREAMS_SIZE_REQUESTED = config('PUB_EVENT_TYPE_REPEAT_MONITOR_STREAMS_SIZE_REQUESTED')
PUB_EVENT_TYPE_SERVICE_WORKERS_STREAM_MONITORED = config('PUB_EVENT_TYPE_SERVICE_WORKERS_STREAM_MONITORED')

PUB_EVENT_LIST = [
    PUB_EVENT_TYPE_REPEAT_MONITOR_STREAMS_SIZE_REQUESTED,
    PUB_EVENT_TYPE_SERVICE_WORKERS_STREAM_MONITORED
]


SERVICE_DETAILS = None

LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')
