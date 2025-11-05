import socket
import json
import logging
from mapreduce.utils.utils import *
LOGGER = logging.getLogger(__name__)

def tcp_server(host, port, signals, handle_func, server_ready_event=None):
    try:
        """TCP Socket Server."""
        LOGGER.info("Starting TCP1 server on %s:%d", host, port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            LOGGER.info("Starting TCP2 server on %s:%d", host, port)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            if server_ready_event is not None:
                server_ready_event.set()
            sock.settimeout(1)
            LOGGER.info("TCP server listening on %s:%d", host, port)
            while not signals.get("shutdown", False):
                try:
                    clientsocket, address = sock.accept()
                    LOGGER.info("Accepted connection from %s:%d", address[0], address[1])
                except socket.timeout:
                    continue
                LOGGER.info("Connection established with %s:%d", address[0], address[1])
                print("Connection from", address[0])
                clientsocket.settimeout(1)


                with clientsocket:
                    LOGGER.info("Accepted connection from %s:%d", address[0], address[1])
                    message_chunks = []
                    while True:
                        LOGGER.info("Waiting to receive data from %s:%d", address[0], address[1])
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
                        print("RAW MESSAGE STR:", repr(message_str), flush=True)
                        message_dict = json_to_dict(message_str)
                    except json.JSONDecodeError:
                        print("JSON DECODE ERROR:", e, "on input:", repr(message_str), flush=True)
                        continue
                    # Pass both message and clientsocket
                    handle_func(message_dict)
    except Exception as e:
        LOGGER.error("TCP server encountered an error: %s", e)


def udp_server(host, port, signals, handle_func):
    LOGGER.info("UDP server yay.")
    # with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

    #     # Bind the UDP socket to the server
    #     sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #     sock.bind((host, port))
    #     sock.settimeout(1)

    #     # No sock.listen() since UDP doesn't establish connections like TCP

    #     # Receive incoming UDP messages
    #     while not signals.get("shutdown", False):
    #         try:
    #             message_bytes = sock.recv(4096)
    #         except socket.timeout:
    #             continue
    #         message_str = message_bytes.decode("utf-8")
    #         message_dict = json_to_dict(message_str)
    #         # TODO: HANDLE FUNC NOT YET DEFINED
    #         handle_func(message_dict)


def tcp_client(host, port, message_dict):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        LOGGER.info("TCP client connecting to %s:%d", host, port)
        sock.connect((host, port))
        msg_str = (dict_to_json(message_dict) + "\n").encode('utf-8')
        sock.sendall(msg_str)
        LOGGER.info("TCP client sent message to %s:%d, message: %s", host, port, msg_str)


def udp_client(host: str, port: int, message_dict: dict):
    """Send dictionary over UDP and (optionally) receive a response."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((host, port))
        message_str = dict_to_json(message_dict)
        sock.send(message_str.encode("utf-8"))

        # (Optional) receive a response
        sock.settimeout(2)
        try:
            response_bytes = sock.recv(4096)
            if not response_bytes or not isinstance(response_bytes, (bytes, bytearray)):
                print("No response received.")
                return None
            response_str = response_bytes.decode("utf-8")
            response_dict = json_to_dict(response_str)
            print("Received:", response_dict)
            return response_dict
        except socket.timeout:
            print("No response received.")
            return None