"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import shutil
import socket
import threading
from mapreduce.utils.network import *
from mapreduce.utils.utils import *
from collections import defaultdict

# Configure logging
LOGGER = logging.getLogger(__name__)

class WorkerInfo:
    def __init__(self, host: str, port: int):
        self.addr = Address(host, port)
        self.is_busy = False
        self.is_dead = False
        #TODO: I'm sure we have to add more stuff to this at some point

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port, shared_dir=""):
        """Construct a Manager instance and start listening for messages."""

        self.workers = []
        self.worker_heartbeats = {}  
        self.shared_dir = shared_dir
        self.job_id_count = 0
        self.job_queue = []
        self.job_is_running = False
        # maps task_id to worker
        self.running_tasks = {}

        LOGGER.info(
            "Starting manager: %s", port
        )
        LOGGER.info(
            "PWD %s", os.getcwd()
        )
        # TODO: Is this the right way to do prefix for the temp dir?
        prefix = f"mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created %s", tmpdir)
            self.tmpdir = tmpdir

            # Signals dict for shutdown
            # TODO: Is shutdown the only signal?????
            self.signals = {'shutdown': False}

            # Start TCP server thread
            tcp_thread = threading.Thread(
                target=tcp_server,
                args=(host, port, self.signals, self.handle_tcp_func),
                daemon=True
            )
            tcp_thread.start()

            # Start UDP heartbeat server thread
            # TODO: Store heartbeats in a python dictionary somewhere
            udp_thread = threading.Thread(
                target=udp_server,
                args=(host, port, self.signals, self.handle_udp_func),
                daemon=True
            )
            udp_thread.start()

            # (Optional) Fault tolerance monitor thread
            # TODO: use fault thread to analyze worker's heartbeat
            # fault_thread = threading.Thread(target=self.fault_tolerance_monitor, daemon=True)
            # fault_thread.start()

            # TODO: Manager must ignore misbehaving Workers who have not yet been acknowledged
            # Block until signals['shutdown'] is True
            while not self.signals['shutdown']:
                time.sleep(0.1)  # Or do other wait logic
            LOGGER.info("Cleaned up tmpdir %s", tmpdir)

            tcp_thread.join(timeout=5)
            udp_thread.join(timeout=5)

    # TODO: handler function is supposed to:
    # Receive incoming messages from the TCP server thread
    # Inspect the message type and contents
    # Dispatch to the correct internal method or logic (like “register worker”, “accept job”, “process results”, etc.)
    # Update Manager state as needed
    # Send any necessary responses back to the sender (optional, if required by protocol)


    # TODO: Implement
    def handle_tcp_func(self, msg):
        if msg.get("message_type") == "register":
            for worker in self.workers:
                if worker.addr.host == msg["worker_host"] and worker.addr.port == msg["worker_port"]:
                    worker.is_dead = False
                    worker.is_busy = False
                    break
            LOGGER.info("registered worker %s:%i", msg["worker_host"], msg["worker_port"])
            # Add the worker to the Manager's list with host and port info
            worker = WorkerInfo(msg["worker_host"], msg["worker_port"])
            self.workers.append(worker)
            # Send registration acknowledgement as a NEW TCP client connection
            ack = {"message_type": "register_ack"}
            # NEW TCP connection to worker, then close
            self.try_tcp(worker.addr.host, worker.addr.port, ack)
            return
        elif msg.get("message_type") == "shutdown":
            # Forward shutdown to all registered workers
            for worker in self.workers:
                self.try_tcp(worker.addr.host, worker.addr.port, {"message_type": "shutdown"})
            self.signals['shutdown'] = True
            # Optionally send ack on shutdown request
            # conn.send(json.dumps({"status": "shutting_down"}).encode())
            return
        elif msg.get("message_type") == "new_manager_job":
            job = Job(msg["input_directory"], msg["output_directory"], msg["mapper_executable"],
                      msg["reducer_executable"], msg["num_mappers"], msg["num_reducers"], self.job_id_count, self.tmpdir)
            self.job_id_count += 1
            self.job_queue.append(job)
            self.start_new_job_if_possible()
            return
        elif msg.get("message_type") == "finished":

            worker = None
            for w in self.workers:
                if msg["worker_host"] == w.addr.host and msg["worker_port"] == w.addr.port:
                    worker = w
                    break
            if worker:
                worker.is_busy = False

            # --- MAP PHASE ---
            if not self.current_job.mapping_is_done:
                self.current_job.maps_completed_count += 1

                # Assign next map task (if any) to this free worker!
                if self.pending_map_tasks:
                    next_partition = self.pending_map_tasks.pop(0)
                    msg_dict = {
                        "message_type": "new_map_task",
                        "task_id": next_partition.task_id,
                        "input_paths": next_partition.input_paths,
                        "executable": self.current_job.mapper_exe,
                        "output_directory": self.current_job.tmpdir,
                        "num_partitions": self.current_job.num_reducers,
                    }
                    worker.is_busy = True
                    self.try_tcp(worker.addr.host, worker.addr.port, msg_dict)
                    LOGGER.info(f"Assigning map task {next_partition.task_id} to worker {worker.addr.host}:{worker.addr.port}")

                if self.current_job.maps_completed_count >= self.current_job.num_partitions:
                    # Start reducing once all maps are done
                    self.start_reducing(self.current_job)
                    self.current_job.mapping_is_done = True

            # --- REDUCE PHASE ---
            else:
                LOGGER.info(f"Reduce task finished from worker {worker.addr.host}:{worker.addr.port}")
                self.current_job.reduces_completed_count += 1

                if self.pending_reduce_tasks:
                    LOGGER.info(f"Pending reduce tasks remaining: {len(self.pending_reduce_tasks)}")
                    next_partition = self.pending_reduce_tasks.pop(0)
                    msg_dict = {
                    "message_type": "new_reduce_task",
                    "task_id": next_partition.task_id,
                    "input_paths": next_partition.input_paths,
                    "executable": self.current_job.reducer_exe, 
                    "output_directory": self.current_job.output_dir,
                    }
                    worker.is_busy = True
                    try_tcp(worker.addr.host, worker.addr.port, msg_dict)
                    LOGGER.info(f"Assigning reduce task {next_partition.task_id} to worker {worker.addr.host}:{worker.addr.port}")

                if self.current_job.reduces_completed_count >= self.current_job.num_reduce_partitions:
                    self.current_job.reducing_is_done = True
                    self.job_is_running = False
                    # Clean up intermediate files
                    shutil.rmtree(self.current_job.tmpdir)
                    self.start_new_job_if_possible()
        else:
            # Handle other messages as needed
            pass
        
        
    # TODO: Implement
    def handle_udp_func(self, msg_dict):
        if msg_dict.get("message_type") == "heartbeat":
            key = (Address(msg_dict["worker_host"], msg_dict["worker_port"]))
            self.worker_heartbeats[key] = time.time() 


    # TODO: Implement
    def fault_tolerance_monitor(self):
        while not self.signals.get('shutdown', False):
            now = time.time()
            for worker in self.workers:
                if worker.is_dead:
                    continue
                key = Address(worker.addr.host, worker.addr.port)
                last = self.worker_heartbeats.get(key, None)
                if (last is not None) and (now - last > 10):
                    # This worker missed 5 heartbeats, mark as dead!
                    if not worker.is_dead:
                        worker.is_dead = True
                        LOGGER.warning(f"Worker {key} missed heartbeats, marked dead")
                        # Reassign any unfinished task as needed
            time.sleep(1)


    def start_new_job_if_possible(self):
        if not self.job_is_running and self.job_queue:
            job = self.job_queue.pop(0)
            self.start_job(job)

    def start_job(self, job):
        LOGGER.info(f"Starting job {job.job_id}")
        self.job_is_running = True
        self.current_job = job
        if os.path.exists(job.output_dir):
            shutil.rmtree(job.output_dir)
        # output directory for the job
        job_dir = os.path.join(self.tmpdir, job.output_dir)
        os.makedirs(job_dir, exist_ok=True)

        # intermediate shared directory for the job
        job.tmpdir = os.path.join(self.tmpdir, f"job-{job.job_id:05d}")
        os.makedirs(job.tmpdir, exist_ok=True)

        # partition the input files into num_mappers partitions (returns a list of Task objects)
        tasks = self.partitioning(job)
        job.num_partitions = len(tasks)
        LOGGER.info(f"Job {job.job_id} has {job.num_partitions} partitions")

        self.pending_map_tasks = tasks[:]  # Copy list

        for worker in self.workers:
            if worker.is_dead:
                continue
            if self.pending_map_tasks and not worker.is_busy:
                task = self.pending_map_tasks.pop(0)
                # Assign worker to the task for tracking/fault tolerance
                task.worker_addr = worker.addr
                task.is_running = True

                msg_dict = {
                    "message_type": "new_map_task",
                    "task_id": task.task_id,
                    "input_paths": task.input_paths,
                    "executable": job.mapper_exe,
                    "output_directory": job.tmpdir,
                    "num_partitions": job.num_reducers,
                }
                worker.is_busy = True
                self.try_tcp(worker.addr.host, worker.addr.port, msg_dict)
                LOGGER.info(f"Assigning map task {task.task_id} to worker {worker.addr.host}:{worker.addr.port}")
    
    
    def partitioning(self, job):
        input_files = sorted(os.listdir(job.input_dir))
        partitionsList = [[] for _ in range(job.num_mappers)]

        # round-robin partition input files
        for i, fname in enumerate(input_files):
            partitionsList[i % job.num_mappers].append(os.path.join(job.input_dir, fname))

        tasks = []
        for i, partition in enumerate(partitionsList):
            # Create a Task for each partition. No worker assigned yet.
            t = Task(
                task_id=i,
                worker_addr=None,             # Not assigned yet
                type="map"                    # This is a map task
            )
            t.input_paths = partition        # Attach input paths; add as attribute as needed
            tasks.append(t)

        return tasks

    def start_reducing(self, job):
        intermediate_files = os.listdir(self.current_job.tmpdir)
        partitions = defaultdict(list)
        # Group intermediate files by partition number
        for fname in intermediate_files:
            # Assumes filenames like maptask00000-part00001
            parts = fname.split("-")
            partition_num = int(parts[1].replace("part", ""))
            partitions[partition_num].append(os.path.join(self.current_job.tmpdir, fname))

        # Create Task objects for each reduce partition
        reduce_tasks = []
        for partition_num, input_paths in partitions.items():
            task = Task(
                task_id=partition_num,
                worker_addr=None,   # not assigned yet
                type="reduce"
            )
            task.input_paths = input_paths  # Attach input files to reduce task
            reduce_tasks.append(task)
        self.pending_reduce_tasks = reduce_tasks[:]  # Copy list
        job.num_reduce_partitions = len(reduce_tasks)

        # Assign reduce tasks to available workers
        for worker in self.workers:
            if self.pending_reduce_tasks and not worker.is_busy:
                task = self.pending_reduce_tasks.pop(0)
                task.worker_addr = worker.addr
                task.is_running = True
                msg_dict = {
                    "message_type": "new_reduce_task",
                    "task_id": task.task_id,
                    "input_paths": task.input_paths,
                    "executable": self.current_job.reducer_exe,
                    "output_directory": self.current_job.output_dir,
                }
                worker.is_busy = True
                self.try_tcp(worker.addr.host, worker.addr.port, msg_dict)
                LOGGER.info(f"Assigning reduce task {task.task_id} to worker {worker.addr.host}:{worker.addr.port}")

    def try_tcp(self, host: int, port: int, msg_dict: dict):
        try:
            tcp_client(host, port, msg_dict)
        except ConnectionRefusedError:
            worker.is_dead = True
            # If assigning a task, put it back in the queue to assign to another worker
            pending_tasks.append(current_task)
            LOGGER.warning(f"Worker {worker.addr.host}:{worker.addr.port} is dead (ConnectionRefusedError)")
            # Proceed to assign to another Worker
        

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
