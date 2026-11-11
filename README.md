# networking

Socket-Server und -Client für kleine LAN-Spiele. Nachrichten werden als JSON oder pickle kodiert und mit einem 4-Byte-Längenpräfix (Big Endian) gesendet.

## Installation

```bash
pip install "networking @ git+https://github.com/yoshi-qq/networking.git"
```

Zum Mitentwickeln:

```bash
pip install -e ".[test]"
```

## Benutzung

```python
import networking

server = networking.Server("0.0.0.0", 54321, 1024, "pickle", 4)
client = networking.Client("localhost", 54321, 1024, "pickle")
client.send(networking.Message(client.name, "userMessage", "hallo"))
```

`networking.onConnect` und `networking.onDisconnect` lassen sich durch eigene Funktionen ersetzen; sie bekommen den `Server.Client` übergeben.

## Request/Event-Schicht

```python
from networking.communications import CommunicationsHandler, Request, Event
```

Der Host sammelt `Request`s der Clients und verteilt `Event`s an alle. `CommunicationHandler` ist ein Zweitname für ältere Aufrufer.

## Achtung: Protokoll

Ältere Fassungen (bruno vor der Umstellung, Game/networking) sendeten ohne Längenpräfix. Beide Seiten einer Verbindung müssen dieses Paket verwenden.

## Tests

```bash
pytest
```
