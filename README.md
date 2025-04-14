# `zsxf `
An incremental query engine

## Roadmap

### Datalog support
- [x] Implicit joins via `:find` and `:where` clauses, i.e. `[?e :attr1 v] [?e :attr2 v2]`
- [x] Aggregates: sum, count
- [ ] Pull API, i.e. `(pull ?e [...])`
- [ ] Efficient rolling window queries
- [ ] Rules
- [ ] Recursive queries
- [ ] Aggregates: avg, min, max


## What does `zsxf` stand for?
A (zs) zset (xf) transducer, a transducer which takes and returns zsets.
