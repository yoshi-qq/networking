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
    """Positionelle Parameter; neue Schlüsselwort-Parameter brechen alte Aufrufe nicht."""
    return [name for name, p in inspect.signature(func).parameters.items()
            if name != "self" and p.kind is not inspect.Parameter.KEYWORD_ONLY]


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


# --- behobene Fehler ------------------------------------------------------------

def test_server_close_with_connected_client_returns(server: networking.Server) -> None:
    client = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
    assert _wait_for(lambda: client.name == "1")
    started = time.monotonic()
    server.close()
    assert time.monotonic() - started < 5
    assert _wait_for(lambda: not client.threads[0].is_alive())
    client.close()
    server.close = lambda: None  # type: ignore[method-assign]


def test_send_all_skips_dead_clients(server: networking.Server) -> None:
    client = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
    assert _wait_for(lambda: len(server.clients) == 1)
    client.close()
    server.sendAll(networking.Message("server", "userMessage", "noch da?"))
    assert _wait_for(lambda: server.clients == [])


def test_full_server_does_not_spin(monkeypatch: pytest.MonkeyPatch) -> None:
    accepted: list[int] = []
    server = networking.Server("localhost", _free_port(), 1024, "pickle", 0)
    monkeypatch.setattr(server, "connect", lambda: accepted.append(1))
    time.sleep(0.5)
    server.close()
    assert accepted == []


def test_names_are_reused_lowest_first(server: networking.Server) -> None:
    first = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
    second = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
    try:
        assert _wait_for(lambda: {first.name, second.name} == {"1", "2"})
        first.close()
        assert _wait_for(lambda: server.unusedNames == ["1"])
        third = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
        assert _wait_for(lambda: third.name == "1")
        third.close()
    finally:
        second.close()


@pytest.mark.parametrize("encoding", ["xml", ""])
def test_unknown_encoding_raises(encoding: str) -> None:
    with pytest.raises(ValueError):
        networking.encode(networking.Message("a", "b", "c"), encoding)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        networking.decode(b"{}", encoding)  # type: ignore[arg-type]


def test_pickle_rejects_non_message() -> None:
    import pickle
    with pytest.raises(TypeError):
        networking.decode(pickle.dumps({"kein": "Message"}), "pickle")


def test_frame_survives_timeout_in_the_middle() -> None:
    left, right = socket.socketpair()
    try:
        right.settimeout(0.05)
        payload = networking.encode(networking.Message("a", "b", "x" * 10), "json")
        left.sendall(struct.pack(">I", len(payload)) + payload[:5])

        def rest() -> None:
            time.sleep(0.2)
            left.sendall(payload[5:])

        import threading
        threading.Thread(target=rest).start()
        assert networking._recv_frame(right) == payload
    finally:
        left.close()
        right.close()


def test_role_methods_raise_on_wrong_side() -> None:
    port = _free_port()
    host = communications.CommunicationsHandler(host=True, port=port)
    try:
        with pytest.raises(RuntimeError):
            host.sendRequest(communications.Request("x", 1))
        assert host.getName() is None
    finally:
        host.quit()


def test_request_compares_only_with_request() -> None:
    assert communications.Request("x", 1) != "x"
    assert communications.Event("x", 1) != communications.Request("x", 1)


# --- Verzögerung --------------------------------------------------------------------

def test_sockets_disable_nagle(server: networking.Server) -> None:
    client = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
    try:
        assert _wait_for(lambda: len(server.clients) == 1)
        assert client.client_socket.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) != 0
        assert server.clients[0].conn.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) != 0
    finally:
        client.close()


def test_parallel_sends_keep_frames_intact(server: networking.Server) -> None:
    import threading
    received: list[str] = []
    server.messageFunctions["teil"] = lambda sender, content: received.append(content)
    client = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
    try:
        assert _wait_for(lambda: client.name == "1")
        senders = [threading.Thread(target=lambda tag=tag: [client.send(networking.Message("1", "teil", tag * 20_000))
                                                             for _ in range(20)]) for tag in "abcd"]
        for sender in senders:
            sender.start()
        for sender in senders:
            sender.join()
        assert _wait_for(lambda: len(received) == 80)
        assert all(len(set(text)) == 1 and len(text) == 20_000 for text in received)
    finally:
        client.close()


def test_close_is_fast(server: networking.Server) -> None:
    client = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
    assert _wait_for(lambda: client.name == "1")
    started = time.monotonic()
    client.close()
    server.close()
    assert time.monotonic() - started < 0.5
    server.close = lambda: None  # type: ignore[method-assign]


# --- Phase 2b -------------------------------------------------------------------

def test_oversized_frame_is_rejected_without_reading() -> None:
    left, right = socket.socketpair()
    try:
        left.sendall(struct.pack(">I", 2_000_000_000))
        with pytest.raises(networking.FrameTooLargeError):
            networking._recv_frame(right, 1024)
        assert issubclass(networking.FrameTooLargeError, ConnectionError)
    finally:
        left.close()
        right.close()


def test_max_frame_none_means_no_limit() -> None:
    left, right = socket.socketpair()
    try:
        payload = b"x" * 5000
        left.sendall(struct.pack(">I", len(payload)) + payload)
        assert networking._recv_frame(right, None) == payload
    finally:
        left.close()
        right.close()


def test_server_drops_client_announcing_huge_frame() -> None:
    server = networking.Server("localhost", _free_port(), 1024, "pickle", 4, max_frame=1024)
    try:
        assert server.max_frame == 1024
        with socket.create_connection(("localhost", server.port), timeout=5) as raw:
            assert _wait_for(lambda: len(server.clients) == 1)
            raw.sendall(struct.pack(">I", 10_000_000))
            assert _wait_for(lambda: len(server.clients) == 0)
    finally:
        server.close()


def test_client_max_frame_default_and_keyword(server: networking.Server) -> None:
    client = networking.Client("localhost", server.port, 1024, "pickle", debug=0, max_frame=None)
    try:
        assert client.max_frame is None
        assert networking.DEFAULT_MAX_FRAME == 16 * 1024 * 1024
        assert server.max_frame == networking.DEFAULT_MAX_FRAME
    finally:
        client.close()


def test_instance_hooks_replace_module_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    module_calls: list[object] = []
    monkeypatch.setattr(networking, "onConnect", module_calls.append)
    monkeypatch.setattr(networking, "onDisconnect", module_calls.append)
    first: list[str] = []
    second: list[str] = []
    a = networking.Server("localhost", _free_port(), 1024, "pickle", 4,
                          on_connect=lambda c: first.append("an"), on_disconnect=lambda c: first.append("ab"))
    b = networking.Server("localhost", _free_port(), 1024, "pickle", 4,
                          on_connect=lambda c: second.append("an"))
    try:
        ca = networking.Client("localhost", a.port, 1024, "pickle", debug=0)
        cb = networking.Client("localhost", b.port, 1024, "pickle", debug=0)
        assert _wait_for(lambda: first == ["an"] and second == ["an"])
        ca.close()
        cb.close()
        assert _wait_for(lambda: first == ["an", "ab"])
        assert _wait_for(lambda: len(module_calls) == 1)  # b ohne on_disconnect: Modul-Hook
    finally:
        a.close()
        b.close()


def test_failing_hook_warns_and_keeps_server_running() -> None:
    def broken(client: object) -> None:
        raise ValueError("kaputt")

    server = networking.Server("localhost", _free_port(), 1024, "pickle", 4, on_connect=broken)
    try:
        with pytest.warns(RuntimeWarning, match="kaputt"):
            client = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
            assert _wait_for(lambda: client.name == "1")
        client.close()
    finally:
        server.close()


def test_communications_callbacks() -> None:
    port = _free_port()
    requests: list[object] = []
    events: list[object] = []
    host = communications.CommunicationsHandler(host=True, port=port, on_request=requests.append)
    client = None
    try:
        client = communications.CommunicationsHandler(host=False, port=port, on_event=events.append)
        assert _wait_for(lambda: client.getName() == "1")
        client.sendRequest(communications.Request("frage", 1))
        assert _wait_for(lambda: len(requests) == 1)
        assert requests[0] == communications.Request("frage", 1)
        assert len(host.getRequests()) == 1
        host.castEvent(communications.Event("antwort", 2))
        assert _wait_for(lambda: len(events) == 1)
        assert len(client.getEvents()) == 1
    finally:
        if client is not None:
            client.quit()
        host.quit()
