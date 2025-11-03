"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket
from mapreduce.utils.network import *
from mapreduce.utils.utils import *


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""

        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        # Shared signals dict for clean shutdown
        self.signals = {'shutdown': False}

        # 1. Start TCP server to receive messages & tasks from manager
        tcp_thread = threading.Thread(
            target=tcp_server,
            args=(host, port, self.signals, self.handle_tcp_func),
            daemon=True
        )
        tcp_thread.start()

        # 2. Send registration message to manager via TCP client and wait for ack
        register_msg = {
            "message_type": "register",
            "worker_host": host,
            "worker_port": port,
        }
        response = tcp_client(
            manager_host,
            manager_port,
            register_msg
        )  # Synchronously send the register message and wait for response

        # Only start heartbeats after receiving acknowledgment
        if response and response.get("message_type") == "register_ack":
            LOGGER.info("Received register_ack from manager, starting heartbeats.")

            def heartbeat_sender():
                while not self.signals.get("shutdown", False):
                    heartbeat_msg = {
                        "message_type": "heartbeat",
                        "worker_host": host,
                        "worker_port": port,
                    }
                    udp_client(manager_host, manager_port, heartbeat_msg)
                    time.sleep(2)
                

            heartbeat_thread = threading.Thread(target=heartbeat_sender, daemon=True)
            heartbeat_thread.start()
        else:
            LOGGER.error("Did not receive register_ack from manager. Halting worker startup.")

    # TODO: Implement handle_tcp_func
    def handle_tcp_func(self, msg_dict, conn):
        if msg_dict["message_type"] == "shutdown":
            self.signals['shutdown'] = True
            # Optionally acknowledge
            conn.send(b'{"status": "shutting_down"}')
            return


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
