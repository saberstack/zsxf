# `ZSXF`
An incremental query engine

### What problem is ZSXF solving?

We are solving a problem that every growing database is going to have:
as the amount of data in a database grows, the time to answer queries is guaranteed to to increase.
Many databases will stop working.

Our solution is called ZSXF: an incremental view maintenance query engine based on [DBSP](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf), written in Clojure. Once a query is declared, ZSXF incrementally and efficiently re-computes a correct query result on every write. The query result is always accessible, typically within microseconds.
### Are we there yet (database edition)?

*[Are we there yet? -- Rich Hickey](https://github.com/matthiasn/talk-transcripts/blob/master/Hickey_Rich/AreWeThereYet.md)*

Most databases since the beginning of time have tried to answer queries in the same fashion: by starting from scratch every time we ask a question! Some of them come with a number of creative solutions to make that problem [less bad](https://duckdb.org). And yet, the fact remains: more data means slower queries, especially JOIN queries.

Databases with well-separated query and transaction processing ([Datomic](http://datomic.com)) have the great property of reads not slowing down writes.

Widely-used databases ([Postgres](http://postgresql.org), [MySQL](https://www.mysql.com)) will experience significant degradation in both queries (reads) and writes to the point of total unresponsiveness of the entire system. In both cases, query times often increase significantly. Once a query response time exceeds a few seconds, it becomes unsuitable for many use cases.

Arguably, the need to answer queries in at least *some fashion* without crashing the entire database gave rise to batch-oriented [warehouses](http://snowflake.com) . Unfortunately, most warehouse setups have latencies of minutes or even hours if we include the time it takes to synchronize each batch of data.

### Goals
- Queries are always correct
- Queries are always fast
- Queries are always available
- Available as a library 
- Simple (and easy!) to integrate with the bare minimum steps required

### Current limitations
- The current implementation is in-memory only.
    - Reason: we are focusing on correctness first, before building a persistent/on-disk implementation
- Query languages supported: only [Datomic Datalog](https://docs.datomic.com/query/query-data-reference.html)
    - We are planning to support SQL Postgres
- Work in progress


## Roadmap

### Datalog support
- [x] Datascript
- [x] Implicit joins via `:find` and `:where` clauses, i.e. `[?e :attr1 v] [?e :attr2 v2]`
- [x] Aggregates: sum, count
- [ ] Pull API, i.e. `(pull ?e [...])`
- [ ] Efficient rolling window queries
- [ ] Rules
- [ ] Recursive queries
- [ ] Aggregates: avg, min, max
- [ ] Datomic

### Postgres/SQL support
- [ ] JOINS


## What does `zsxf` stand for?
A (zs) zset (xf) transducer, a transducer which takes and returns zsets.
