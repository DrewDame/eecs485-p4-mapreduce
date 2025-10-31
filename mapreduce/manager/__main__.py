"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import socket

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind((host, port))
        server_sock.listen()

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        while True:
            conn, addr = server_sock.accept()
            LOGGER.debug(f"Accepted connection from {addr}")
            data = conn.recv(4096)
            try:
                msg = json.loads(data.decode())
                LOGGER.debug("TCP recv\n%s", json.dumps(msg, indent=2))
                # handle message here
            except json.JSONDecodeError:
                LOGGER.warning("Received invalid JSON, ignoring")
            conn.close()


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
