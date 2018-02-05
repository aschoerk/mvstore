
# MMapBTree, MMapDbFile Transactional

## Requirements

To support isolation and multiuser-possibilities, a concept of transactions is implemented.
To support that the client can define the beginning and end of transactions. The following conditions will be met.

* Successfully ending the transaction using commit, means that all changes are persisted. It is possible that because of 
unique constraints the commit can not end successfully.
* It will be possible to end transcations with rollback. That means, that all changes since the start of the current 
transactions can be undone
* Changes done during a transaction will not be visible to other transactions until the commit has been successfully completed. 

* Änderungen, die in Transaktionen durchgeführt werden, sollen für die aktuelle Transaktion sichtbar sein. 
* Nach dem Begin einer Transaktion soll die Datenbank für den Benutzer bis zum commit unverändert bleiben.