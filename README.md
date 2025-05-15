# `ZSXF`
An incremental query engine

## Roadmap

### Datalog support
- [x] Datascript
- [x] Implicit joins via `:find` and `:where` clauses, i.e. `[?e :attr1 v] [?e :attr2 v2]`
- [ ] Aggregates: sum, count
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
