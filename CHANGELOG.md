# Changelog

Versionen nach [SemVer](https://semver.org/lang/de/). Datum TT.MM.JJJJ.

## 1.2.0 – 12.11.2026

Alles Neue ist ein Schlüsselwort-Parameter; alte Aufrufe verhalten sich gleich.

### Neu
- `max_frame` für `Server` und `Client` (Standard `DEFAULT_MAX_FRAME` = 16 MiB, `None` = keine Grenze). Kündigt die Gegenstelle einen größeren Rahmen an, wird nichts davon gelesen: `FrameTooLargeError` (eine `ConnectionError`), die Verbindung wird getrennt. Vorher konnte eine Gegenstelle im LAN mit einem großen Längenkopf den Speicher füllen.
- `Server(on_connect=..., on_disconnect=...)`: Rückrufe für diesen Server allein. Ohne sie gelten wie bisher `networking.onConnect`/`onDisconnect`. Fehler in diesen Rückrufen werden als `RuntimeWarning` gemeldet und beenden den Empfangs-Thread nicht.
- `CommunicationsHandler(on_request=..., on_event=...)`: wird im Empfangs-Thread gerufen, sobald ein Request (Host) oder Event (Client) ankommt. Die Warteschlangen bleiben; Fehler werden gewarnt.

## 1.1.1 – 11.11.2026

Weniger Verzögerung, Verhalten unverändert.

### Schneller
- `TCP_NODELAY` auf beiden Seiten: kleine Nachrichten gehen sofort raus, statt auf weitere Daten zu warten. Wirkt im echten Netz; auf localhost nicht messbar.
- `Client.close()` und `Server.close()` wecken die wartenden Threads sofort, statt auf die 1-Sekunden-Abfrage zu warten.
- Empfang ohne wiederholtes Aneinanderhängen von Bytes; pickle mit dem schnellsten Protokoll.
- `bench/bench_latency.py` misst Rundreise, Durchsatz, Request→Event und Schließen.

### Behoben
- Gleichzeitiges Senden aus mehreren Threads über denselben Socket konnte Rahmen vermischen; jetzt eine Sperre je Socket.

Messung auf localhost (Median):

| | 1.1.0 | 1.1.1 |
|---|---|---|
| Rundreise pickle, klein | 0,091 ms | 0,096 ms |
| Rundreise json, klein | 0,113 ms | 0,108 ms |
| Rundreise pickle, 2 MB | 7,2 ms | 8,0 ms |
| Durchsatz pickle | 33 200 /s | 31 500 /s |
| Request → Event | 1,6 ms | 2,0 ms |
| Schließen Client+Server | 2025 ms | 0,8 ms |

Auf localhost liegen Rundreise und Durchsatz im Rauschen; Request → Event wird von der 1-ms-Abfrage des Messskripts bestimmt.

## 1.1.0 – 11.11.2026

Vollständig typisiert (`mypy --strict`), bekannte Fehler behoben: `Server.close()` hing bei verbundenen Clients, voller Server lastete einen Kern aus, Nachrichten mit Pause gingen verloren, getrennte Clients ließen `sendAll()` abstürzen, Client-Liste threadsicher, klare Fehler bei unbekannter Kodierung. `getRequests()`/`getEvents()` geben Kopien zurück.

## 1.0.0 – 11.11.2026

Erste Fassung als Paket, aus der val2d-Kopie (Längenpräfix) und communications 1.2.
