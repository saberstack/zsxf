**General Structure (alpha)**

- **Source code** – All implementation lives under `src/org/zsxf/`. Key namespaces are:

    - `query.cljc` – defines how queries are created, updated with transaction data, and read via functions such as `create-query` and `input`.

    - `zset.cljc` – utility functions for “zsets,” weighted sets that represent data and deltas (e.g., `zset-pos+`, `zset-negate`, `index` etc.).

    - `datascript.cljc` – integration with the Datascript database; it sets up listeners and feeds transactions through the query engine.

    - `datom.cljc` – helper functions for working with datoms and converting them into zset items.

    - `datalog/compiler.clj` and `datalog/parser.clj` – compile Datalog queries into composed transducer using macros; runtime compilation is possible but should be used only with trusted sources.

    - `xf.cljc` – a large set of transducers for joins, differences, unions, cartesian products, and more. These transducers operate on zsets.

    - The `type` directory includes custom data structures (e.g., `one_item_set.cljc` for optimized single-item sets).


- **Tests** – Found under `test/org/zsxf/`, covering utilities, zsets, the Datalog parser, query functionality, and specialized types. They demonstrate how to compose queries and integrate with Datascript.


**Important Concepts**

- **zsets**: Weighted sets where each item carries a weight in metadata. They are combined using functions such as `zset+`, `zset-negate`, `zset*`, `indexed-zset+`, etc.

- **Queries**: Created with `create-query`, executed by sending transactions via `input`. The query result is kept incrementally updated.

- **Transducers**: Most processing logic (joins, filters, cartesian products) is implemented via transducers in `xf.cljc`.

- **Datascript integration**: Functions in `datascript.cljc` show how to load existing data from a Datascript connection and keep the query updated via listeners.

- **Datalog compilation**: `datalog/compiler.clj` macros analyze a Datalog query, build a graph of clauses, and produce a composite transducer that applies joins and predicates efficiently.
