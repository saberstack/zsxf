PRAGMA
foreign_keys = ON;
PRAGMA
journal_mode = WAL;
PRAGMA
synchronous = NORMAL;
PRAGMA
mmap_size = 134217728; -- 128 megabytes
PRAGMA
journal_size_limit = 67108864; -- 64 megabytes
PRAGMA
cache_size = 2000;

-- datomic schema
CREATE TABLE datomic_kvs
(
    id  TEXT NOT NULL,
    rev INTEGER,
    map TEXT,
    val BYTEA,
    CONSTRAINT pk_id PRIMARY KEY (id)
);
