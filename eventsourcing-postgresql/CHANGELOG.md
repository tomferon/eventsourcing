# Revision history for eventsourcing-postgresql

## 1.0.0 -- ??

* Removed Pipes.Consumer from helper functions.
* Add `TrackedSQLQuery` to run a SQL query while fetching the state from a
  tracking table at the same time. This can be useful with `TopUpReadModel`.
* Add helpers for migrations: `lockTable` and `swapTables`.

## 0.9.0 -- 2020-08-16

* First version.
