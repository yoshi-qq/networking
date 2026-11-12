"""Request/Event-Schicht über networking.

Der Host sammelt Requests der Clients in einer Warteschlange und verteilt Events an alle Clients.
"""
from __future__ import annotations

from enum import Enum
import warnings
from threading import Lock
from time import time as now
from typing import Any, Callable, Optional, Union

import networking as net
from networking import Client, Encoding, Message, MessageFunction, Server, decode, encode

HOSTNAME = "HOST"

Head = Union[str, Enum]


class Request:
    def __init__(self, head: Head, body: Any) -> None:
        self.signature: str = "unsigned"
        self.head = head
        self.body = body
        self.id: Optional[int] = None
        self.sentTime: float = now()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Request):
            return NotImplemented
        return bool(self.head == other.head and self.body == other.body)

    def __str__(self) -> str:
        return f"Request[head={self.head}, body={self.body}]"


class Event:
    def __init__(self, head: Head, body: Any) -> None:
        self.head = head
        self.body = body
        self.id: Optional[int] = None
        self.sentTime: float = now()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Event):
            return NotImplemented
        return bool(self.head == other.head and self.body == other.body)

    def __str__(self) -> str:
        return f"Event[head={self.head}, body={self.body}]"


def _notify(callback: Callable[[Any], None], item: Any) -> None:
    try:
        callback(item)
    except Exception as error:
        warnings.warn(f"Rückruf {getattr(callback, '__name__', callback)!r} fehlgeschlagen: {error!r}",
                      RuntimeWarning, stacklevel=2)


class CommunicationsHandler:
    def __init__(self, host: bool = False, ip: str = "localhost", port: int = 54321, maxClients: int = 4,
                 dataSize: int = 1024, commands: Optional[dict[str, MessageFunction]] = None, *,
                 on_request: Optional[Callable[[Request], None]] = None,
                 on_event: Optional[Callable[[Event], None]] = None) -> None:
        """on_request (Host) und on_event (Client) werden im Empfangs-Thread gerufen, sobald etwas in der
        Warteschlange liegt. Die Warteschlange bleibt; getRequests/getEvents funktionieren weiter.
        """
        self.on_request = on_request
        self.on_event = on_event
        self.DATA_SIZE = dataSize
        self.__commands: dict[str, MessageFunction] = {} if commands is None else commands
        self._host = host
        self._ip = ip
        self._port = port
        self._maxClients = maxClients
        self.__idCount = 1
        self.__queueLock = Lock()
        self.__requestQueue: list[Request] = []
        self.__eventQueue: list[Event] = []
        self.__mainObject: Union[Server, Client]
        if self._host:
            self.__initHost()
        else:
            self.__initClient()

    def __getId(self) -> int:
        self.__idCount += 1
        return self.__idCount

    def __addRequest(self, sender: str, request: Request) -> None:
        with self.__queueLock:
            request.id = self.__getId()
            request.signature = sender
            self.__requestQueue.append(request)
        if self.on_request is not None:
            _notify(self.on_request, request)

    def __addEvent(self, sender: str, event: Event) -> None:
        if sender != HOSTNAME:
            return
        with self.__queueLock:
            event.id = self.__getId()
            self.__eventQueue.append(event)
        if self.on_event is not None:
            _notify(self.on_event, event)

    def __initHost(self) -> None:
        server = Server(self._ip, self._port, self.DATA_SIZE, "pickle", self._maxClients)
        server.messageFunctions = server.messageFunctions | self.__commands
        server.messageFunctions["Request"] = self.__addRequest
        self.__mainObject = server

    def __initClient(self) -> None:
        client = Client(self._ip, self._port, self.DATA_SIZE, "pickle", False, debug=1)
        client.messageFunctions = client.messageFunctions | self.__commands
        client.messageFunctions["Event"] = self.__addEvent
        self.__mainObject = client

    def __server(self, method: str) -> Server:
        if not isinstance(self.__mainObject, Server):
            raise RuntimeError(f"{method}() gibt es nur beim Host")
        return self.__mainObject

    def __client(self, method: str) -> Client:
        if not isinstance(self.__mainObject, Client):
            raise RuntimeError(f"{method}() gibt es nur beim Client")
        return self.__mainObject

    # Host
    def getRequests(self) -> list[Request]:
        self.__server("getRequests")
        with self.__queueLock:
            return list(self.__requestQueue)

    def resolveRequest(self, requestId: int) -> None:
        self.__server("resolveRequest")
        with self.__queueLock:
            self.__requestQueue[:] = [request for request in self.__requestQueue if request.id != requestId]

    def castEvent(self, event: Event) -> None:
        self.__server("castEvent").sendAll(Message(sender=HOSTNAME, type="Event", content=event))

    # Client
    def getEvents(self) -> list[Event]:
        self.__client("getEvents")
        with self.__queueLock:
            return list(self.__eventQueue)

    def resolveEvent(self, eventId: int) -> None:
        self.__client("resolveEvent")
        with self.__queueLock:
            self.__eventQueue[:] = [event for event in self.__eventQueue if event.id != eventId]

    def sendRequest(self, request: Request) -> None:
        client = self.__client("sendRequest")
        client.send(Message(sender=client.name, type="Request", content=request))

    # Beide
    def getName(self) -> Optional[str]:
        if isinstance(self.__mainObject, Client):
            return self.__mainObject.name
        return None

    def getMainObject(self) -> Union[Server, Client]:
        return self.__mainObject

    def quit(self) -> None:
        self.__mainObject.close()


def setOnConnect(func: Callable[[Server.Client], None]) -> None:
    net.onConnect = func  # type: ignore[assignment]


def setOnDisconnect(func: Callable[[Server.Client], None]) -> None:
    net.onDisconnect = func  # type: ignore[assignment]


CommunicationHandler = CommunicationsHandler

__all__ = ["HOSTNAME", "Request", "Event", "CommunicationsHandler", "CommunicationHandler", "setOnConnect",
           "setOnDisconnect", "Message", "Server", "Client", "encode", "decode", "Encoding", "Head"]
