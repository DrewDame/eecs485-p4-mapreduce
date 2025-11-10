"""MapReduce framework Worker node."""
import os
import logging
import time
import threading
import tempfile
import subprocess
import shutil
import hashlib
import contextlib
import click
from mapreduce.utils.network import tcp_server, tcp_client, udp_client
from mapreduce.utils.utils import dict_to_json_pretty


# Configure logging
LOGGER = logging.getLogger(__name__)


class WorkerInfo:
    """Workaround yippee."""

    def __init__(self, host, port, manager_host, manager_port):
        """Initialize WorkerInfo."""
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port

    def dummy_method(self):
        """Workaround yippee."""
        self.port += 1
        self.port -= 1


class Worker:
    """A class represeinting a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        m_host = manager_host
        m_port = manager_port
        self.more_worker_info = WorkerInfo(host, port, m_host, m_port)
        self._heartbeat_thread = None
        self._shutdown_signal = False

        LOGGER.info(
            "Starting worker:%s",
            port,
        )

        LOGGER.info(
            "PWD %s",
            os.getcwd(),
        )
        # self.host, self.port = host, port
        # self.manager_host, self.manager_port = manager_host, manager_port

        # Shared signals dict for clean shutdown
        self.signals = {'shutdown': False}

        # 1. Start TCP server to receive messages & tasks from manager
        h = host
        p = port
        tcp_thread = threading.Thread(
            target=tcp_server,
            args=(h, p, self.signals, self.handle_tcp_func),
            daemon=True,
        )
        tcp_thread.start()

        # 3. Send registration message to manager via TCP client
        register_msg = {
            "message_type": "register",
            "worker_host": host,
            "worker_port": port,
        }
        # print("Worker sending registration...", register_msg)
        tcp_client(m_host, m_host, register_msg)

        # 4. Keep worker alive
        while not self.signals['shutdown']:
            time.sleep(0.1)  # Or do other wait logic
        self._shutdown_signal = True
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=3)

    def handle_tcp_func(self, msg_dict):
        """Handle tcp messages received on worker server."""
        LOGGER.debug("received\n %s", dict_to_json_pretty(msg_dict))
        if msg_dict["message_type"] == "shutdown":
            self.signals['shutdown'] = True
            self._shutdown_signal = True
            if self._heartbeat_thread:
                self._heartbeat_thread.join(timeout=3)
            return
        if msg_dict["message_type"] == "register_ack":
            # LOGGER.info("Received register_ack, starting heartbeat thread")
            self.on_register_ack()
            return
        if msg_dict["message_type"] == "new_map_task":
            self.map_task(msg_dict)
        elif msg_dict["message_type"] == "new_reduce_task":
            self.map_reduce_task(msg_dict)

    def on_register_ack(self):
        """Start heartbeat thread."""
        # Called when Manager sends "register_ack"
        # print("DEBUG: on_register_ack called
        # (Worker starting heartbeat)", flush=True)
        t = threading.Thread(target=self.heartbeat_thread, daemon=True)
        t.start()
        self._heartbeat_thread = t

    def heartbeat_thread(self):
        """Send heartbeats every 2 seconds."""
        print("DEBUG: heartbeat thread running")
        while not self._shutdown_signal:
            msg_dict = {
                "message_type": "heartbeat",
                "worker_host": self.more_worker_info.host,
                "worker_port": self.more_worker_info.port,
            }
            print("DEBUG: sending heartbeat:", msg_dict, flush=True)
            udp_client(self.more_worker_info.manager_host,
                       self.more_worker_info.manager_port, msg_dict)
            time.sleep(2)

    # def map_task(self, msg_dict):
    #     """Execute the map task."""
    #     # Extract job/task parameters from msg_dict
    #     task_id = msg_dict["task_id"]
    #     input_paths = msg_dict["input_paths"]
    #     map_executable = msg_dict["executable"]
    #     num_partitions = msg_dict["num_partitions"]
    #     shared_output_dir = msg_dict["output_directory"]

    #     prefix = f"mapreduce-local-task{task_id:05d}-"
    #     with tempfile.TemporaryDirectory(prefix=prefix) as temp_output_dir:
    #         with contextlib.ExitStack() as stack:
    #             # Open all output files with ExitStack
    #             # (all closed automatically at block exit)
    #             output_files = [
    #                 stack.enter_context(open(
    #                     os.path.join(
    #                         temp_output_dir,
    #                         f"maptask{task_id:05d}-part{p:05d}"
    #                     ),
    #                     "w",
    #                     encoding="utf-8"
    #                 ))
    #                 for p in range(num_partitions)
    #             ]

    #             # Run map executable for each
    #             # input file, partition output lines
    #             for input_path in input_paths:
    #                 with open(input_path, encoding="utf-8") as infile:
    #                     with subprocess.Popen(
    #                         [map_executable],
    #                         stdin=infile,
    #                         stdout=subprocess.PIPE,
    #                         text=True
    #                     ) as proc:
    #                         for line in proc.stdout:
    #                             if not line.strip():
    #                                 continue
    #                             key = line.rstrip("\n").split('\t', 1)[0]
    #                             keyhash = int(
    #                                 hashlib.md5(key.encode()).hexdigest(),
    #                                 16
    #                             )
    #                             partition_num = keyhash % num_partitions
    #                             output_files[partition_num].write(line)

    #         # Step 3: Sort each partition file in place
    #         for p in range(num_partitions):
    #             filename = os.path.join(
    #                 temp_output_dir,
    #                 f"maptask{task_id:05d}-part{p:05d}"
    #             )
    #             subprocess.run(
    #                 ["sort", "-o", filename, filename],
    #                 check=True
    #             )

    #         # Step 4: Move sorted files to shared output dir
    #         for p in range(num_partitions):
    #             name = f"maptask{task_id:05d}-part{p:05d}"
    #             src = f"{temp_output_dir}/{name}"
    #             dst = f"{shared_output_dir}/{name}"
    #             shutil.move(src, dst)

    #     # Send 'finished' message to Manager
    #     finished_msg = {
    #         "message_type": "finished",
    #         "task_id": task_id,
    #         "worker_host": self.more_worker_info.host,
    #         "worker_port": self.more_worker_info.port,
    #     }
    #     tcp_client(self.more_worker_info.manager_host,
    #                self.more_worker_info.manager_port, finished_msg)

    def map_task(self, msg_dict):
        """Execute the map task."""
        # Extract job/task parameters from msg_dict
        task_id = msg_dict["task_id"]
        i_paths = msg_dict["input_paths"]
        map_exe = msg_dict["executable"]
        npart = msg_dict["num_partitions"]
        shared_dir = msg_dict["output_directory"]
        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as t_dir:
            with contextlib.ExitStack() as s:
                ofiles = self._open_partition_files(s, t_dir, task_id, npart)
                self._run_and_partition(map_exe, i_paths, ofiles, npart)
            self._sort_partitions(t_dir, task_id, npart)
            self._move_partitions(t_dir, shared_dir, task_id, npart)
        self._send_finished(task_id)

    def _parse_msg(self, msg_dict):
        """Extract job/task parameters from msg_dict."""
        return (
            msg_dict["task_id"],
            msg_dict["input_paths"],
            msg_dict["executable"],
            msg_dict["num_partitions"],
            msg_dict["output_directory"]
        )

    def _open_partition_files(self, stack, t_dir, task_id, num_part):
        """Open all partition output files and return list of handles."""
        return [
            stack.enter_context(
                open(
                    os.path.join(t_dir, f"maptask{task_id:05d}-part{p:05d}"),
                    "w",
                    encoding="utf-8"
                )
            )
            for p in range(num_part)
        ]

    def _run_and_partition(self, map_executable,
                           input_paths, output_files, num_partitions):
        """Run executable, partition output lines by key hash."""
        for input_path in input_paths:
            with open(input_path, encoding="utf-8") as infile:
                with subprocess.Popen(
                    [map_executable],
                    stdin=infile,
                    stdout=subprocess.PIPE,
                    text=True
                ) as proc:
                    for line in proc.stdout:
                        if not line.strip():
                            continue
                        key = line.rstrip("\n").split('\t', 1)[0]
                        keyhash = int(
                            hashlib.md5(key.encode("utf-8")).hexdigest(),
                            16
                        )
                        partition_num = keyhash % num_partitions
                        output_files[partition_num].write(line)

    def _sort_partitions(self, t_dir, task_id, num_partitions):
        """Sort each partition file in-place using UNIX sort."""
        for p in range(num_partitions):
            filename = os.path.join(t_dir, f"maptask{task_id:05d}-part{p:05d}")
            subprocess.run(["sort", "-o", filename, filename], check=True)

    def _move_partitions(self, temp_output_dir, s_dir, task_id, num_part):
        """Move sorted files to the shared output directory."""
        for p in range(num_part):
            name = f"maptask{task_id:05d}-part{p:05d}"
            src = os.path.join(temp_output_dir, name)
            dst = os.path.join(s_dir, name)
            shutil.move(src, dst)

    def _send_finished(self, task_id):
        """Send 'finished' message to Manager."""
        finished_msg = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.more_worker_info.host,
            "worker_port": self.more_worker_info.port,
        }
        tcp_client(self.more_worker_info.manager_host,
                   self.more_worker_info.manager_port, finished_msg)

    def map_reduce_task(self, msg_dict):
        """Execute the map-reduce task based on fields in msg_dict."""
        task_id = msg_dict["task_id"]
        input_paths = msg_dict["input_paths"]
        reduce_exe = msg_dict["executable"]
        o_path = msg_dict["output_directory"]

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as t_dir:
            s_file = self._prepare_reduce_files(task_id, input_paths, t_dir)
            self._run_reducer_and_finish(task_id, reduce_exe, s_file, o_path)

    def _prepare_reduce_files(self, task_id, input_paths, t_dir):
        """Merge and sort reduce input files, return sorted file path."""
        i_file = os.path.join(t_dir, f"reducetask{task_id:05d}-intermediate")
        with open(i_file, "w", encoding="utf-8") as outfile:
            for input_path in input_paths:
                with open(input_path, "r", encoding="utf-8") as infile:
                    shutil.copyfileobj(infile, outfile)

        s_file = os.path.join(t_dir, f"reducetask{task_id:05d}-sorted")
        subprocess.run(["sort", i_file, "-o", s_file], check=True)
        return s_file

    def _run_reducer_and_finish(self, task_id, reduce_exe, s_file, o_path):
        """Run reduce executable, write output, send finished to Manager."""
        o_f_path = os.path.join(o_path, f"part-{task_id:05d}")
        with (
            open(s_file, "r", encoding="utf-8") as infile,
            open(o_f_path, "w", encoding="utf-8") as outfile
        ):
            with subprocess.Popen(
                [reduce_exe],
                stdin=infile,
                stdout=outfile,
                text=True
            ) as proc:
                proc.wait()

        finished_msg = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.more_worker_info.host,
            "worker_port": self.more_worker_info.port,
        }
        tcp_client(self.more_worker_info.manager_host,
                   self.more_worker_info.manager_port, finished_msg)


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
