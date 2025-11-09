import socket
import json
import logging
from mapreduce.utils.utils import *
LOGGER = logging.getLogger(__name__)


def tcp_server(host, port, signals, handle_func):
    """TCP Socket Server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()
        # if server_ready_event is not None:
        #     server_ready_event.set()
        sock.settimeout(1)
        while not signals.get("shutdown", False):
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            # print("Connection from", address[0])
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

                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json_to_dict(message_str)
                    handle_func(message_dict)
                except json.JSONDecodeError:
                    continue
                # Pass both message and clientsocket
                # handle_func(message_dict)


def udp_server(host, port, signals, handle_func):
    # LOGGER.info("UDP server yay.")
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Bind the UDP socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)

        # No sock.listen() since UDP doesn't establish connections like TCP

        # Receive incoming UDP messages
        while not signals.get("shutdown", False):
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")
            message_dict = json_to_dict(message_str)
            handle_func(message_dict)


def tcp_client(host, port, message_dict):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        msg_str = (dict_to_json(message_dict)).encode('utf-8')
        sock.sendall(msg_str)


def udp_client(host: str, port: int, message_dict: dict):
    """Send dictionary over UDP and (optionally) receive a response."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Connect to the UDP socket on server
        sock.connect((host, port))

        # Send a message
        message = dict_to_json(message_dict)
        sock.sendall(message.encode('utf-8'))
