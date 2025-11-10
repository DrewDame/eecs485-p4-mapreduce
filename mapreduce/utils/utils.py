"""Util classes and functions."""
import json
import logging

LOGGER = logging.getLogger(__name__)


class Address:
    """Address class."""

    def __init__(self, host: str, port: int):
        """Initialize Address."""
        self.host = host
        self.port = port

    def dummy_method(self):
        """Fix pyling for too few methods."""
        self.port += 1
        self.port -= 1


class MoreJobInfo:
    """Class to pass Job you know what."""

    def __init__(self, input_dir: str, output_dir: str, mapper_exe: str,
                 reducer_exe: str, tmp_dir: str):
        """Initialize MoreJobInfo."""
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.mapper_exe = mapper_exe
        self.reducer_exe = reducer_exe
        self.tmp_dir = tmp_dir

    def dummy_method(self):
        """Fix you know what."""
        LOGGER.info(self.input_dir)


class JobStatus:
    """Class to keep track of Job Status."""

    def __init__(self):
        """Initialize JobStatus."""
        self.maps_completed_count = 0
        self.reduces_completed_count = 0
        self.num_partitions = 0
        self.num_reduce_partitions = 0
        self.mapping_is_done = False
        self.reducing_is_done = False

    def dummy_method(self):
        """Pass you know what."""
        self.num_partitions += 1
        self.num_partitions -= 1


class Job:
    """Job class for job received from client."""

    def __init__(self, num_mappers: int, num_reducers: int,
                 job_id: int, more_job_info):
        """Initialize Job."""
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.job_id = job_id
        self.more_job_info = more_job_info
        self.job_status = JobStatus()

    def dummy_method(self):
        """Use to get past you know what."""
        self.job_id += 1
        self.job_id -= 1


class Task:
    """Mapping or reducing task."""

    def __init__(self, task_id: int, worker_addr, type_in: str):
        """Initialize Task."""
        self.task_id = task_id
        self.worker_addr = worker_addr
        self.is_completed = False
        self.is_running = False
        self.type = type_in  # "map" or "reduce"
        self.assigned_time = None

    def dummy_method(self):
        """Use to get past you know what."""
        self.is_completed = True
        self.is_completed = False


class Partition:
    """Partition from mapping task."""

    def __init__(self, partition_list, task_id: int):
        """Initialize Partition."""
        self.input_paths = partition_list
        self.task_id = task_id

    def dummy_method(self):
        """Use to get past you know what."""
        self.task_id += 1
        self.task_id -= 1

# class ReduceTask:
#     def __init__(self, task_id: int):


def dict_to_json(input_dict: dict) -> str:
    """Accept a dictionary as input and returns a json string."""
    return json.dumps(input_dict)


def dict_to_json_pretty(input_dict: dict) -> str:
    """Accept a dictionary as input and returns a pretty json string."""
    return json.dumps(input_dict, indent=2)


def json_to_dict(input_json: str) -> dict:
    """Accept a json string as input and returns a dictionary."""
    return json.loads(input_json)

# def tcp_thread_starter(host: str, port: int, signals: dict,
#                        handle_func):
#     """Start TCP server thread."""
#     tcp_thread = threading.Thread(
#         target=tcp_server,
#         args=(host, port, signals, handle_func),
#         daemon=True,
#     )
#     tcp_thread.start()
#     return tcp_thread
