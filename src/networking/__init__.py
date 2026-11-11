"""networking: Socket-Server und -Client für kleine LAN-Spiele.

Jede Nachricht wird als 4-Byte-Länge (Big Endian) plus Inhalt gesendet.
Der Inhalt ist JSON (nur einfache Datentypen) oder pickle (beliebige Objekte).
pickle führt beim Dekodieren Code aus: nur mit vertrauenswürdigen Gegenstellen verwenden.
"""
from __future__ import annotations

import json
import pickle
import socket
import struct
import threading
import time
from typing import Any, Callable, Literal, Optional

Encoding = Literal["json", "pickle"]
MessageFunction = Callable[[str, Any], None]

_HEADER = struct.Struct(">I")
_POLL_SECONDS = 1.0
_FULL_SERVER_WAIT_SECONDS = 0.1


def onConnect(this: "Server.Client") -> None:
    """Wird aufgerufen, sobald sich ein Client verbunden hat. Zum Ersetzen gedacht."""
    return None


def onDisconnect(this: "Server.Client") -> None:
    """Wird aufgerufen, sobald ein Client getrennt wurde. Zum Ersetzen gedacht."""
    return None


def _recv_all(conn: socket.socket, size: int) -> bytes:
    """Liest genau size Bytes. Ein Timeout nach den ersten Bytes bricht den Rahmen nicht ab."""
    buf = b""
    while len(buf) < size:
        try:
            chunk = conn.recv(size - len(buf))
        except TimeoutError:
            if not buf:
                raise
            continue
        if not chunk:
            raise ConnectionError("Socket closed during frame reception")
        buf += chunk
    return buf


def _recv_frame(conn: socket.socket) -> bytes:
    (length,) = _HEADER.unpack(_recv_all(conn, _HEADER.size))
    return _recv_frame_body(conn, length)


def _recv_frame_body(conn: socket.socket, length: int) -> bytes:
    """Nach dem Kopf kommt der Inhalt bestimmt: Timeouts hier nur abwarten."""
    while True:
        try:
            return _recv_all(conn, length)
        except TimeoutError:
            continue


def _send_frame(conn: socket.socket, data: bytes) -> None:
    conn.sendall(_HEADER.pack(len(data)) + data)


class Message:
    """Nachricht aus Absender, Typ und Inhalt."""

    def __init__(self, sender: str, type: str, content: Any) -> None:
        self.sender = sender
        self.type = type
        self.content = content

    def __str__(self) -> str:
        return f"Message from '{self.sender}': ({self.type})\n {self.content}"


class Server:
    class Client:
        """Ein verbundener Client aus Sicht des Servers."""

        def __init__(self, parent: Server, conn: socket.socket, addr: tuple[str, int]) -> None:
            self.parent = parent
            self.conn = conn
            self.addr = addr
            client_thread = threading.Thread(target=self.handleClient, args=(conn, addr), daemon=True)
            with parent._lock:
                self.name = parent._take_name()
                parent.clients.append(self)
                parent.threads.append(client_thread)
            client_thread.start()

        def handleClient(self, conn: socket.socket, addr: tuple[str, int]) -> None:
            print(f"Connected with {addr}")
            onConnect(self)
            try:
                self.parent.send(conn, Message("server", "setName", self.name))
                while self.parent.on:
                    message = self.parent.receive(conn, addr)
                    function = self.parent.messageFunctions.get(message.type)
                    if function is not None:
                        function(message.sender, message.content)
                    else:
                        print(message)
            except (ConnectionError, OSError) as error:
                if self.parent.on:
                    print(error)
            finally:
                try:
                    conn.close()
                except OSError:
                    pass
                self.parent._release(self)
                print(f"Disconnected {addr}")
                onDisconnect(self)

    def __init__(self, host: str, port: int, dataSize: int, encoding: Encoding = "json",
                 maxConnections: int = 4, console: bool = False) -> None:
        self.threads: list[threading.Thread] = []
        self.on = True
        self.clients: list[Server.Client] = []
        self.host = host
        self.port = port
        self.dataSize = dataSize
        self.encoding: Encoding = encoding
        self.messageFunctions: dict[str, MessageFunction] = {"userMessage": self.userMessage}
        self.unusedNames: list[str] = []
        self.maxConnections = maxConnections
        self.console = console
        self._lock = threading.RLock()
        self._name_count = 0
        self.server_socket: socket.socket
        self.start_server()

    def _take_name(self) -> str:
        with self._lock:
            if self.unusedNames:
                return self.unusedNames.pop(0)
            self._name_count += 1
            return str(self._name_count)

    def _release(self, client: Server.Client) -> None:
        with self._lock:
            if client in self.clients:
                self.clients.remove(client)
            if client.name not in self.unusedNames:
                self.unusedNames.append(client.name)
                self.unusedNames.sort(key=lambda name: (len(name), name))

    def start_server(self) -> None:
        print("Starting server...")
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.maxConnections)
        self.server_socket.settimeout(_POLL_SECONDS)
        print(f"Listening on {self.host}:{self.port}")
        if self.console:
            threading.Thread(target=self.consoleLoop, daemon=True).start()
        connect_thread = threading.Thread(target=self.connectLoop, daemon=True)
        self.threads.append(connect_thread)
        connect_thread.start()

    def close(self) -> None:
        self.on = False
        with self._lock:
            clients = list(self.clients)
        for client in clients:
            try:
                client.conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            client.conn.close()
        for thread in list(self.threads):
            if thread is not threading.current_thread():
                thread.join()
        self.threads.clear()
        self.server_socket.close()
        print("Server closed")

    def connectLoop(self) -> None:
        while self.on:
            with self._lock:
                full = len(self.clients) >= self.maxConnections
            if full:
                time.sleep(_FULL_SERVER_WAIT_SECONDS)
            else:
                self.connect()

    def consoleLoop(self) -> None:
        print("Console activated")
        while self.on:
            try:
                content = input("")
            except EOFError:
                return
            self.sendAll(Message("server", "userMessage", content))
            print(f"server: {content}")

    def connect(self) -> Optional[TimeoutError]:
        try:
            conn, addr = self.server_socket.accept()
        except TimeoutError as error:
            return error
        except OSError:
            return None
        conn.settimeout(None)
        self.Client(self, conn, addr)
        return None

    def receive(self, conn: socket.socket, addr: tuple[str, int]) -> Message:
        return decode(_recv_frame(conn), self.encoding)

    def send(self, conn: socket.socket, message: Message) -> None:
        _send_frame(conn, encode(message, self.encoding))

    def sendAll(self, message: Message) -> None:
        data = encode(message, self.encoding)
        with self._lock:
            clients = list(self.clients)
        for client in clients:
            try:
                _send_frame(client.conn, data)
            except OSError:
                pass

    def userMessage(self, sender: str, content: Any) -> None:
        print(f"{sender}: {content}")
        self.sendAll(Message(sender, "userMessage", content))


class Client:
    def __init__(self, host: str, port: int, dataSize: int, encoding: Encoding = "json",
                 console: bool = False, debug: int = 1) -> None:
        self.on = True
        self.threads: list[threading.Thread] = []
        self.host = host
        self.port = port
        self.dataSize = dataSize
        self.encoding: Encoding = encoding
        self.console = console
        self.name = "unknown client"
        self.debug = int(debug)
        self.messageFunctions: dict[str, MessageFunction] = {"userMessage": self.userMessage,
                                                             "setName": self.setName}
        self.client_socket: socket.socket
        self.connect(host, port)

    def connect(self, host: str, port: int) -> None:
        self.client_socket = socket.create_connection((host, port))
        if self.debug > 0:
            print(f"Connected to host: {self.host}: port {self.port}")
        recv_thread = threading.Thread(target=self.receive_messages, args=(self.client_socket,), daemon=True)
        self.threads.append(recv_thread)
        recv_thread.start()
        if self.console:
            threading.Thread(target=self.consoleLoop, daemon=True).start()

    def close(self) -> None:
        self.on = False
        for thread in list(self.threads):
            if thread is not threading.current_thread():
                thread.join()
        self.threads.clear()
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.client_socket.close()
        print("Disconnected")

    def consoleLoop(self) -> None:
        if self.debug > 0:
            print("Console activated")
        while self.on:
            try:
                content = input("")
            except EOFError:
                return
            self.send(Message(self.name, "userMessage", content))

    def receive_messages(self, sock: socket.socket) -> None:
        if self.debug > 0:
            print("Now receiving messages")
        sock.settimeout(_POLL_SECONDS)
        while self.on:
            try:
                message = self.receive()
            except TimeoutError:
                continue
            except (ConnectionError, OSError) as error:
                if self.debug > 1:
                    print(error)
                break
            function = self.messageFunctions.get(message.type)
            if function is not None:
                function(message.sender, message.content)
            else:
                print(message)
            if self.debug > 1:
                print(f"Executed {message.type} from {message.sender}")

    def receive(self) -> Message:
        message = decode(_recv_frame(self.client_socket), self.encoding)
        if self.debug > 1:
            print(message)
        return message

    def send(self, message: Message) -> None:
        _send_frame(self.client_socket, encode(message, self.encoding))

    def userMessage(self, sender: str, content: Any) -> None:
        print(f"{sender}: {content}")

    def setName(self, sender: str, name: str) -> None:
        self.name = name


def encode(message: Message, type: Encoding) -> bytes:
    if type == "json":
        payload = {"sender": message.sender, "type": message.type, "content": message.content}
        return json.dumps(payload).encode("utf-8")
    if type == "pickle":
        return pickle.dumps(message)
    raise ValueError(f"Unbekannte Kodierung: {type!r}")


def decode(encoded: bytes, type: Encoding) -> Message:
    if type == "json":
        payload = json.loads(encoded.decode("utf-8"))
        return Message(payload["sender"], payload["type"], payload["content"])
    if type == "pickle":
        message = pickle.loads(encoded)
        if not isinstance(message, Message):
            raise TypeError(f"Erwartet Message, bekommen {message.__class__.__name__}")
        return message
    raise ValueError(f"Unbekannte Kodierung: {type!r}")


__all__ = ["Message", "Server", "Client", "encode", "decode", "onConnect", "onDisconnect", "Encoding",
           "MessageFunction"]
