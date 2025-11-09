import json

class Address:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

class Job:
    def __init__(self, input_dir: str, output_dir: str, mapper_exe: str, reducer_exe: str, num_mappers: int, num_reducers: int, job_id: int, tmp_dir: str):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.mapper_exe = mapper_exe
        self.reducer_exe = reducer_exe
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.job_id = job_id
        self.tmpdir = tmp_dir
        self.maps_completed_count = 0
        self.reduces_completed_count = 0
        self.num_partitions = 0
        self.num_reduce_partitions = 0
        self.mapping_is_done = False
        self.reducing_is_done = False

class Task:
    def __init__(self, task_id: int, worker_addr: Address, type: str):
        self.task_id = task_id
        self.worker_addr = worker_addr
        self.is_completed = False
        self.is_running = False
        self.type = ""  # "map" or "reduce"

class Partition:
    def __init__(self, partition_list, task_id: int):
        self.input_paths = partition_list
        self.task_id = task_id

# class ReduceTask:
#     def __init__(self, task_id: int):
        


def dict_to_json(input_dict: dict) -> str:
    """Accepts a dictionary as input and returns a json string."""
    return json.dumps(input_dict)


def dict_to_json_pretty(input_dict: dict) -> str:
    """Accepts a dictionary as input and returns a pretty json string."""
    return json.dumps(input_dict, indent=2)


def json_to_dict(input_json: str) -> dict:
    """Accepts a json string as input and returns a dictionary."""
    return json.loads(input_json)