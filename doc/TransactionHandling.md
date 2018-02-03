
# Transaktionen im Zusammenhang mit MMapBTree bzw. MMapDbFile

## Anforderungen

Benutzer sollen Anfang und Ende von Transaktionen markieren können mit folgenden Eigenschaften

* Das Ende soll mit commit abgeschlossen werden können, was bedeutet, dass alle Änderungen stabil persistiert sind
* Das Ende soll mit rollback abgeschlossen werden können, was bedeutet, dass alle Änderungen zwischen Anfang 
und rollback rückgängig gemacht werden.
* Änderungen, die in Transaktionen durchgeführt werden, sollen für andere Transaktionen erst sichtbar werden, wenn commit 
erfolgreich durchgeführt werden konnte. Das bedeutet, dass nur Auswirkungen von vollständig durchgeführten Transaktionen
für andere Benutzer sichtbar sind.
* Änderungen, die in Transaktionen durchgeführt werden, sollen für die aktuelle Transaktion sichtbar sein. 
* Nach dem Begin einer Transaktion soll die Datenbank für den Benutzer bis zum commit unverändert bleiben.