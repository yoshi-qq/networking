"""Misst Verzögerung und Durchsatz von networking auf localhost.

Aufruf: python bench/bench_latency.py
"""
from __future__ import annotations

import socket
import statistics
import threading
import time
from typing import Any

import networking
from networking import communications

ROUNDS = 300


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("localhost", 0))
        return int(probe.getsockname()[1])


def _wait(condition: Any, timeout: float = 5) -> None:
    deadline = time.perf_counter() + timeout
    while not condition():
        if time.perf_counter() > deadline:
            raise TimeoutError
        time.sleep(0.0005)


def ping_pong(encoding: networking.Encoding, payload: Any) -> float:
    """Client sendet, Server antwortet sofort; gemessen wird die Rundreise."""
    server = networking.Server("localhost", _free_port(), 1024, encoding, 4)
    answered = threading.Event()
    server.messageFunctions["ping"] = lambda sender, content: server.sendAll(networking.Message("server", "pong", content))
    client = networking.Client("localhost", server.port, 1024, encoding, debug=0)
    client.messageFunctions["pong"] = lambda sender, content: answered.set()
    _wait(lambda: client.name != "unknown client")
    times: list[float] = []
    for _ in range(ROUNDS):
        answered.clear()
        start = time.perf_counter()
        client.send(networking.Message(client.name, "ping", payload))
        if not answered.wait(2):
            raise TimeoutError("keine Antwort")
        times.append(time.perf_counter() - start)
    client.close()
    server.close()
    return statistics.median(times) * 1000


def request_to_event() -> float:
    """communications: Request vom Client bis zum Event zurück, Host fragt jede Millisekunde ab."""
    port = _free_port()
    host = communications.CommunicationsHandler(host=True, port=port)
    client = communications.CommunicationsHandler(host=False, port=port)
    _wait(lambda: client.getName() not in (None, "unknown client"))
    stop = threading.Event()

    def host_loop() -> None:
        while not stop.is_set():
            for request in host.getRequests():
                host.resolveRequest(request.id or 0)
                host.castEvent(communications.Event("antwort", request.body))
            time.sleep(0.001)

    worker = threading.Thread(target=host_loop)
    worker.start()
    times: list[float] = []
    for index in range(ROUNDS // 3):
        start = time.perf_counter()
        client.sendRequest(communications.Request("frage", index))
        _wait(lambda: any(event.body == index for event in client.getEvents()))
        for event in client.getEvents():
            client.resolveEvent(event.id or 0)
        times.append(time.perf_counter() - start)
    stop.set()
    worker.join()
    client.quit()
    host.quit()
    return statistics.median(times) * 1000


def throughput(encoding: networking.Encoding, count: int = 20_000) -> float:
    """Nachrichten pro Sekunde vom Client zum Server, kleine Nachrichten ohne Antwort."""
    server = networking.Server("localhost", _free_port(), 1024, encoding, 4)
    received = [0]
    done = threading.Event()

    def count_message(sender: str, content: Any) -> None:
        received[0] += 1
        if received[0] >= count:
            done.set()

    server.messageFunctions["tick"] = count_message
    client = networking.Client("localhost", server.port, 1024, encoding, debug=0)
    _wait(lambda: client.name != "unknown client")
    payload = {"x": 1.5, "y": 2.5, "angle": 90}
    start = time.perf_counter()
    for _ in range(count):
        client.send(networking.Message(client.name, "tick", payload))
    if not done.wait(30):
        raise TimeoutError("nicht alle Nachrichten angekommen")
    elapsed = time.perf_counter() - start
    client.close()
    server.close()
    return count / elapsed


def close_time() -> float:
    server = networking.Server("localhost", _free_port(), 1024, "pickle", 4)
    client = networking.Client("localhost", server.port, 1024, "pickle", debug=0)
    _wait(lambda: client.name != "unknown client")
    start = time.perf_counter()
    client.close()
    server.close()
    return (time.perf_counter() - start) * 1000


def main() -> None:
    small = {"x": 1.5, "y": 2.5, "angle": 90}
    big = list(range(20_000))
    print(f"Rundreise pickle, klein   {ping_pong('pickle', small):8.3f} ms")
    print(f"Rundreise json, klein     {ping_pong('json', small):8.3f} ms")
    print(f"Rundreise pickle, 20k     {ping_pong('pickle', big):8.3f} ms")
    print(f"Rundreise pickle, 2 MB    {ping_pong('pickle', bytes(2_000_000)):8.3f} ms")
    print(f"Durchsatz pickle          {throughput('pickle'):8.0f} Nachrichten/s")
    print(f"Durchsatz json            {throughput('json'):8.0f} Nachrichten/s")
    print(f"Request -> Event          {request_to_event():8.3f} ms")
    print(f"Schließen Client+Server   {close_time():8.1f} ms")


if __name__ == "__main__":
    main()
