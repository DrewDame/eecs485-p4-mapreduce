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

# Configure logging
LOGGER = logging.getLogger(__name__)

class WorkerInfo:
    def __init__(self, host: str, port: int):
        self.addr = Address(host, port)
        self.is_busy = False
        #TODO: I'm sure we have to add more stuff to this at some point

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port, shared_dir=""):
        """Construct a Manager instance and start listening for messages."""

        self.workers = []
        
        self.shared_dir = shared_dir
        self.job_id_count = 0
        self.job_queue = []
        self.job_is_running = False

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

    # TODO: handler function is supposed to:
    # Receive incoming messages from the TCP server thread
    # Inspect the message type and contents
    # Dispatch to the correct internal method or logic (like “register worker”, “accept job”, “process results”, etc.)
    # Update Manager state as needed
    # Send any necessary responses back to the sender (optional, if required by protocol)


    # TODO: Implement
    def handle_tcp_func(self, msg):
        LOGGER.debug(f"received\n {dict_to_json_pretty(msg)}")
        if msg.get("message_type") == "register":
            LOGGER.info("registered worker %s:%i", msg["worker_host"], msg["worker_port"])
            # Add the worker to the Manager's list with host and port info
            worker = WorkerInfo(msg["worker_host"], msg["worker_port"])
            self.workers.append(worker)
            # Send registration acknowledgement as a NEW TCP client connection
            ack = {"message_type": "register_ack"}
            # NEW TCP connection to worker, then close
            tcp_client(worker.addr.host, worker.addr.port, ack)
            return
        elif msg.get("message_type") == "shutdown":
            # Forward shutdown to all registered workers
            for worker in self.workers:
                tcp_client(worker.addr.host, worker.addr.port, {"message_type": "shutdown"})
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
            LOGGER.info("Accepted new job %i", job.job_id)
            return
        else:
            # Handle other messages as needed
            pass
        
        
    # TODO: Implement
    def handle_udp_func():
        print("")

    # TODO: Implement
    def fault_tolerance_monitor(self):
        while not self.signals.get('shutdown', False):
            time.sleep(0.5)


    def start_new_job_if_possible(self):
        if not self.job_is_running and self.job_queue:
            job = self.job_queue.pop(0)
            self.start_job(job)

    def start_job(self, job):
        self.job_is_running = True
        if os.path.exists(job.output_dir):
            shutil.rmtree(job.output_dir)
        # output directory for the job
        job_dir = os.path.join(self.tmpdir, job.output_dir)
        os.makedirs(job_dir, exist_ok=True)

        # intermediate shared directory for the job
        job.tmpdir = os.path.join(self.tmpdir, f"job-{job.job_id:05d}")
        os.makedirs(job.tmpdir, exist_ok=True)
        # partition the input files into num_mappers partitions
        partitions = self.partitioning(job)
        # assign each partition to a worker
        next_worker_index = 0
        for partition in partitions:
            msg_dict = {
                "message_type": "new_map_task",
                "task_id": partition.task_id,
                "input_paths": partition.input_paths,
                "executable": job.mapper_exe,
                #TODO: this might not be the right output directory
                "output_directory": job.tmpdir,
                "num_partitions": job.num_reducers,
                "shared_dir": self.shared_dir
            }
            # round robin assignment of workers
            worker = self.workers[next_worker_index]
            while worker.is_busy:
                next_worker_index = (next_worker_index + 1) % len(self.workers)
                worker = self.workers[next_worker_index]
                time.sleep(0.1)
                LOGGER.info(next_worker_index)
            worker.is_busy = True
            #TODO: Currently not doing anything with this message on the Worker's side
            tcp_client(worker.addr.host, worker.addr.port, msg_dict)
            next_worker_index = (next_worker_index + 1) % len(self.workers)
        
        self.job_is_running = False
    
    def partitioning(self, job):
        # scan the input directory from the job
        input_files = os.listdir(job.input_dir)
        input_files = sorted(os.listdir(job.input_dir))
        # partition the input files into num_mappers partitions
        # partitions = [[] for _ in range(job.num_mappers)]
        partitionsList = []
        for i in range(job.num_mappers):
            partitionsList.append([])
        for i in range(len(input_files)):
            partitionsList[i % job.num_mappers].append(os.path.join(job.input_dir, input_files[i]))
        # sort partitionsList by name
        # partitionsList.sort()
        partitions = []
        for i in range(len(partitionsList)):
            partition = Partition(partitionsList[i], i)
            partitions.append(partition)
        return partitions
        



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
