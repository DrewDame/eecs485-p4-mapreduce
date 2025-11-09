import socket
import json
import logging
from mapreduce.utils.utils import *

LOGGER = logging.getLogger(__name__)

def tcp_server(host: str, port: int, signals, handle_func):
    """TCP socket server."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()

        sock.settimeout(1)

        while not signals.get("shutdown", False):
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            LOGGER.info("Connection from", address[0])

            clientsocket.settimeout(1)
            
            with clientsocket:
                message_chunks = []
                while True:
                    try:
                        data = clientsocket.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message_chunks.append(data)

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")

            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            handle_func(message_dict)


def tcp_client(host: str, port: int, msg_dict: dict):
    """Example TCP socket client."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        LOGGER.info("TCP client connecting to %s:%d", host, port)
        sock.connect((host, port))

        message = dict_to_json(msg_dict)
        sock.sendall(message.encode('utf-8'))
        LOGGER.info("TCP client sent message to %s:%d, message: %s", host, port, msg_str)


def udp_server(host: str, port: int, signals, handle_func):
    """Establish a UDP server."""
    LOGGER.infO("NOTHING MATTERS IN HERE YET.")


def udp_client(host: str, port: int, signals, handle_func):
    """Send a message over udp client."""
    LOGGER.info("NOTHING MATTERS IN HERE YET.")