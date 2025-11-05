import socket
import json
from mapreduce.utils.utils import *

def tcp_server(host, port, signals, handle_func):
    """TCP Socket Server."""
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
            print("Connection from", address[0])
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
                except json.JSONDecodeError:
                    continue
                # Pass both message and clientsocket
                handle_func(message_dict)


def udp_server(host, port, signals, handle_func):
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
            # TODO: HANDLE FUNC NOT YET DEFINED
            handle_func(message_dict)


def tcp_client(host: str, port: int, message_dict: dict):
    """Send dictionary over TCP and (optionally) receive a response."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        # Use your utility function for encoding
        message_str = dict_to_json(message_dict)
        sock.sendall(message_str.encode("utf-8"))

        # # (Optional) receive a response
        # sock.settimeout(2)
        # try:
        #     response_bytes = sock.recv(4096)
        #     if not response_bytes or not isinstance(response_bytes, (bytes, bytearray)):
        #         print("No response received.")
        #         return None
        #     response_str = response_bytes.decode("utf-8")
        #     response_dict = json_to_dict(response_str)
        #     print("Received:", response_dict)
        #     return response_dict
        # except socket.timeout:
        #     print("No response received.")
        # return None

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