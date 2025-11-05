"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import socket
import threading
from mapreduce.utils.network import *
from mapreduce.utils.utils import *

# Configure logging
LOGGER = logging.getLogger(__name__)

class WorkerInfo:
    def __init__(self, host: str, port: int):
        self.addr = Address(host, port)
        #TODO: I'm sure we have to add more stuff to this at some point

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        self.workers = []

        # TODO: Is this the right way to do prefix for the temp dir?
        prefix = f"mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)

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
            fault_thread = threading.Thread(target=self.fault_tolerance_monitor, daemon=True)
            fault_thread.start()

            LOGGER.info(
                "Starting manager host=%s port=%s pwd=%s",
                host, port, os.getcwd(),
            )
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
    def handle_tcp_func(self, msg, conn):
        LOGGER.info(f"Manager received TCP message: {msg}")
        print("IN HANDLE_TCP_FUNC", msg, flush=True)
        if msg.get("message_type") == "register":
            # Add the worker to the Manager's list with host and port info
            worker = WorkerInfo(msg["worker_host"], msg["worker_port"])
            self.workers.append(worker)
            LOGGER.info(f"Registered worker: {msg['worker_host']}:{msg['worker_port']}")
            # Send registration acknowledgement back to Worker
            ack = {"message_type": "register_ack"}
            ack_str = dict_to_json({"message_type": "register_ack"}) + '\n'
            conn.sendall(ack_str.encode('utf-8'))
            conn.shutdown(socket.SHUT_WR)   # <-- add this line after sendall
            LOGGER.info(f"Sent register_ack to worker: {msg['worker_host']}:{msg['worker_port']}")
            return
        elif msg.get("message_type") == "shutdown":
            # Forward shutdown to all registered workers
            for worker in self.workers:
                tcp_client(worker.addr.host, worker.addr.port, {"message_type": "shutdown"})
            self.signals['shutdown'] = True
            # Optionally send ack on shutdown request
            conn.send(json.dumps({"status": "shutting_down"}).encode())
            LOGGER.info("Manager received shutdown and forwarded to workers.")
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
