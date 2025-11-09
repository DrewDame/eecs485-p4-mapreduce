"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket
import tempfile
import subprocess
import shutil
import hashlib
import contextlib
from mapreduce.utils.network import *
from mapreduce.utils.utils import *


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        self._heartbeat_thread = None        # Track thread instance (not the function)
        self._shutdown_signal = False        # Dedicated flag for heartbeat thread

        LOGGER.info(
            "Starting worker:%s",
            port,
        )

        LOGGER.info(
            "PWD %s",
            os.getcwd(),
        )
        self.host, self.port = host, port
        self.manager_host, self.manager_port = manager_host, manager_port

        # Shared signals dict for clean shutdown
        self.signals = {'shutdown': False}
        # self.server_ready_event = threading.Event()

        # 1. Start TCP server to receive messages & tasks from manager
        tcp_thread = threading.Thread(
            target=tcp_server,
            args=(host, port, self.signals, self.handle_tcp_func),
            daemon=True
        )
        tcp_thread.start()

        # 2. Wait until the TCP server is listening
        # self.server_ready_event.wait()   # <------ THIS KEEPS THE SERVER ALIVE!

        # 3. Send registration message to manager via TCP client
        register_msg = {
            "message_type": "register",
            "worker_host": host,
            "worker_port": port,
        }
        # print("Worker sending registration...", register_msg)
        tcp_client(manager_host, manager_port, register_msg)

        # 4. Keep worker alive
        while not self.signals['shutdown']:
            time.sleep(0.1)  # Or do other wait logic
        self._shutdown_signal = True
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=3)

    # TODO: Implement handle_tcp_func
    def handle_tcp_func(self, msg_dict):
        LOGGER.debug(f"received\n{dict_to_json_pretty(msg_dict)}")
        if msg_dict["message_type"] == "shutdown":
            self.signals['shutdown'] = True
            self._shutdown_signal = True
            if self._heartbeat_thread:
                self._heartbeat_thread.join(timeout=3)
            return
        elif msg_dict["message_type"] == "register_ack":
            # LOGGER.info("Received register_ack, starting heartbeat thread")
            self.on_register_ack()
            return
        elif msg_dict["message_type"] == "new_map_task":
            self.map_task(msg_dict)
        elif msg_dict["message_type"] == "new_reduce_task":
            self.map_reduce_task(msg_dict)

    def on_register_ack(self):
        # Called when Manager sends "register_ack"
        print("DEBUG: on_register_ack called (Worker starting heartbeat)", flush=True)
        t = threading.Thread(target=self.heartbeat_thread, daemon=True)
        t.start()
        self._heartbeat_thread = t

    def heartbeat_thread(self):
        print("DEBUG: heartbeat thread running")
        while not self._shutdown_signal:
            msg_dict = {
                "message_type": "heartbeat",
                "worker_host": self.host,
                "worker_port": self.port,
            }
            print("DEBUG: sending heartbeat:", msg_dict, flush=True)
            udp_client(self.manager_host, self.manager_port, msg_dict)
            time.sleep(2)

    def map_task(self, msg_dict):
        """Execute the map task."""
        # Extract job/task parameters from msg_dict
        task_id = msg_dict["task_id"]
        input_paths = msg_dict["input_paths"]
        map_executable = msg_dict["executable"]
        num_partitions = msg_dict["num_partitions"]
        shared_output_dir = msg_dict["output_directory"]

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as temp_output_dir:
            with contextlib.ExitStack() as stack:
                # Open all output files with ExitStack (all closed automatically at block exit)
                output_files = [
                    stack.enter_context(open(
                        os.path.join(temp_output_dir, f"maptask{task_id:05d}-part{p:05d}"), "w"
                    ))
                    for p in range(num_partitions)
                ]

                # Run map executable for each input file, partition output lines
                for input_path in input_paths:
                    with open(input_path) as infile:
                        with subprocess.Popen(
                            [map_executable],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            text=True
                        ) as proc:
                            for line in proc.stdout:
                                if not line.strip():
                                    continue
                                key, value = line.rstrip("\n").split('\t', 1)
                                keyhash = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
                                partition_num = keyhash % num_partitions
                                output_files[partition_num].write(line)

            # Step 3: Sort each partition file in place
            for p in range(num_partitions):
                filename = os.path.join(temp_output_dir, f"maptask{task_id:05d}-part{p:05d}")
                subprocess.run(["sort", "-o", filename, filename], check=True)

            # Step 4: Move sorted files to shared output dir
            for p in range(num_partitions):
                name = f"maptask{task_id:05d}-part{p:05d}"
                src = f"{temp_output_dir}/{name}"
                dst = f"{shared_output_dir}/{name}"
                shutil.move(src, dst)

        # Send 'finished' message to Manager
        finished_msg = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port,
        }
        tcp_client(self.manager_host, self.manager_port, finished_msg)

    def map_reduce_task(self, msg_dict):
        """Execute the map-reduce task based on fields in msg_dict."""
        # Extract job/task parameters from msg_dict
        task_id = msg_dict["task_id"]
        input_paths = msg_dict["input_paths"]
        reduce_executable = msg_dict["executable"]
        output_path = msg_dict["output_directory"]

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as t_dir:
            i_file = f"{t_dir}/reducetask{task_id:05d}-intermediate"
            # Step 1: Merge all input files into one intermediate file
            with open(i_file, "w") as outfile:
                for input_path in input_paths:
                    with open(input_path, "r") as infile:
                        shutil.copyfileobj(infile, outfile)

            # Step 2: Sort the intermediate file
            s_file = os.path.join(t_dir, f"reducetask{task_id:05d}-sorted")
            subprocess.run(["sort", i_file, "-o", s_file], check=True)

            # Step 3: Run reduce executable on sorted intermediate file
            o_f_path = os.path.join(output_path, f"part-{task_id:05d}")
            with open(s_file, "r") as i, open(o_f_path, "w") as o:
                with subprocess.Popen(
                    [reduce_executable],
                    stdin=i,
                    stdout=o,
                    text=True
                ) as proc:
                    proc.wait()

        # Send 'finished' message to Manager
        finished_msg = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port,
        }
        tcp_client(self.manager_host, self.manager_port, finished_msg)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
