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
from utils.network import *

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        # TODO: Is this the right way to do prefix for the temp dir?
        prefix = f"mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)

            # Signals dict for shutdown
            # TODO: Is shutdown the only signal?????
            self.signals = {'shutdown': False}

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

            # Start TCP server thread
            tcp_thread = threading.Thread(
                target=tcp_server,
                args=(host, port, self.signals, self.handle_tcp_func),
                daemon=True
            )
            tcp_thread.start()

            LOGGER.info(
                "Starting manager host=%s port=%s pwd=%s",
                host, port, os.getcwd(),
            )

            # Main TCP server loop
            try:
                while not self.signals.get("shutdown", False):
                    try:
                        conn, addr = server_sock.accept()
                    except KeyboardInterrupt:
                        break
                    LOGGER.debug(f"Accepted connection from {addr}")
                    data = conn.recv(4096)
                    try:
                        msg = json.loads(data.decode())
                        LOGGER.debug("TCP recv\n%s", json.dumps(msg, indent=2))
                        self.handle_tcp_message(msg, conn)
                    except json.JSONDecodeError:
                        LOGGER.warning("Received invalid JSON, ignoring")
                    finally:
                        conn.close()
            except KeyboardInterrupt:
                LOGGER.info("Manager shutdown requested.")
            finally:
                self.signals['shutdown'] = True  # Signal shutdown to threads

            LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    # TODO: handler function is supposed to:
    # Receive incoming messages from the TCP server thread
    # Inspect the message type and contents
    # Dispatch to the correct internal method or logic (like “register worker”, “accept job”, “process results”, etc.)
    # Update Manager state as needed
    # Send any necessary responses back to the sender (optional, if required by protocol)

    # TODO: Implement
    def handle_tcp_func():
        
        
    # TODO: Implement
    def handle_udp_func():
        



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
