import inspect
import socket
import struct
import time
from typing import Callable, Iterator

import pytest

import networking
from networking import communications


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("localhost", 0))
        return probe.getsockname()[1]


def _wait_for(condition: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


def _params(func: Callable) -> list[str]:
    return [p for p in inspect.signature(func).parameters if p != "self"]


# --- Schnittstelle ------------------------------------------------------------

@pytest.mark.parametrize("name", ["Message", "Server", "Client", "encode", "decode", "onConnect", "onDisconnect"])
def test_public_names(name: str) -> None:
    assert hasattr(networking, name)


@pytest.mark.parametrize("func,expected", [
    (networking.Server.__init__, ["host", "port", "dataSize", "encoding", "maxConnections", "console"]),
    (networking.Client.__init__, ["host", "port", "dataSize", "encoding", "console", "debug"]),
    (networking.Message.__init__, ["sender", "type", "content"]),
    (networking.Server.send, ["conn", "message"]),
    (networking.Server.sendAll, ["message"]),
    (networking.Server.receive, ["conn", "addr"]),
    (networking.Client.send, ["message"]),
    (networking.Client.receive, []),
    (networking.encode, ["message", "type"]),
    (networking.decode, ["encoded", "type"]),
    (communications.CommunicationsHandler.__init__, ["host", "ip", "port", "maxClients", "dataSize", "commands"]),
    (communications.Request.__init__, ["head", "body"]),
    (communications.Event.__init__, ["head", "body"]),
])
def test_signatures(func: Callable, expected: list[str]) -> None:
    assert _params(func) == expected


def test_old_handler_name_is_alias() -> None:
    assert communications.CommunicationHandler is communications.CommunicationsHandler
    assert communications.HOSTNAME == "HOST"


# --- Kodierung ----------------------------------------------------------------

@pytest.mark.parametrize("encoding", ["json", "pickle"])
def test_encode_decode_roundtrip(encoding: str) -> None:
    message = networking.Message("server", "userMessage", {"a": [1, 2]})
    decoded = networking.decode(networking.encode(message, encoding), encoding)
    assert (decoded.sender, decoded.type, decoded.content) == ("server", "userMessage", {"a": [1, 2]})


def test_message_str() -> None:
    assert str(networking.Message("a", "b", "c")) == "Message from 'a': (b)\n c"


def test_request_event_equality() -> None:
    assert communications.Request("x", 1) == communications.Request("x", 1)
    assert communications.Event("x", 1) != communications.Event("x", 2)
    assert str(communications.Request("x", 1)) == "Request[head=x, body=1]"


# --- Verbindung ---------------------------------------------------------------

@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> Iterator[networking.Server]:
    connected: list[object] = []
    disconnected: list[object] = []
    monkeypatch.setattr(networking, "onConnect", connected.append)
    monkeypatch.setattr(networking, "onDisconnect", disconnected.append)
    instance = networking.Server("localhost", _free_port(), 1024, "pickle", 4)
    instance.connected = connected
    instance.disconnected = disconnected
    yield instance
    instance.close()


def test_client_gets_name_and_hooks_fire(server: networking.Server) -> None:
    client = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
    try:
        assert _wait_for(lambda: client.name == "1")
        assert _wait_for(lambda: len(server.connected) == 1)
    finally:
        client.close()
    assert _wait_for(lambda: len(server.disconnected) == 1)
    assert _wait_for(lambda: server.unusedNames == ["1"])


def test_custom_message_function_and_broadcast(server: networking.Server) -> None:
    received: list[tuple[str, object]] = []
    server.messageFunctions["ping"] = lambda sender, content: received.append((sender, content))
    client = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
    echoed: list[object] = []
    client.messageFunctions["pong"] = lambda sender, content: echoed.append(content)
    try:
        assert _wait_for(lambda: client.name == "1")
        client.send(networking.Message(client.name, "ping", "x" * 50_000))
        assert _wait_for(lambda: received == [("1", "x" * 50_000)])
        server.sendAll(networking.Message("server", "pong", 42))
        assert _wait_for(lambda: echoed == [42])
    finally:
        client.close()


def test_wire_format_has_length_prefix(server: networking.Server) -> None:
    with socket.create_connection(("localhost", server.port), timeout=5) as raw:
        header = networking._recv_all(raw, 4)
        length = struct.unpack(">I", header)[0]
        body = networking._recv_all(raw, length)
        message = networking.decode(body, "pickle")
        assert (message.type, message.content) == ("setName", "1")


def test_communications_request_and_event() -> None:
    port = _free_port()
    host = communications.CommunicationsHandler(host=True, port=port)
    client = None
    try:
        client = communications.CommunicationsHandler(host=False, port=port)
        assert _wait_for(lambda: client.getName() == "1")
        client.sendRequest(communications.Request("bewegen", (1, 2)))
        assert _wait_for(lambda: len(host.getRequests()) == 1)
        request = host.getRequests()[0]
        assert (request.head, request.body, request.signature) == ("bewegen", (1, 2), "1")
        host.resolveRequest(request.id)
        assert host.getRequests() == []
        host.castEvent(communications.Event("start", None))
        assert _wait_for(lambda: len(client.getEvents()) == 1)
        event = client.getEvents()[0]
        client.resolveEvent(event.id)
        assert client.getEvents() == []
        assert isinstance(host.getMainObject(), networking.Server)
    finally:
        if client is not None:
            client.quit()
        host.quit()


def test_set_on_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    hook = lambda this: None
    monkeypatch.setattr(networking, "onConnect", networking.onConnect)
    monkeypatch.setattr(networking, "onDisconnect", networking.onDisconnect)
    communications.setOnConnect(hook)
    communications.setOnDisconnect(hook)
    assert networking.onConnect is hook
    assert networking.onDisconnect is hook
