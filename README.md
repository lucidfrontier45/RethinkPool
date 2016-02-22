# RethinkDB Connection Pool for Python

`rethinkpool` is a Python library for pooling RethinkDB connections.
This is usefull for multi-thread application since a connection object Python driver provides is not thread-safe

## Installation

```bash
$ pip install rethinkpool
```

## Usage

Connection Pool can be created by instantiating `RethinkPool`.

```python
from rethinkpool import RethinkPool

pool = RethinkPool()
```

The arguments for `RethinkPool` are as follows.

- max_conns: maximum number of connections
- initial_conns: number of initial connections to establish
- get_timeout: timeout for obtaining a connection from the queue
- host, port, ...: same as `r.connect`

The pooled `ConnectionResource` object can be obtained by `pool.get_resource()`
Used connection resource can be returned to the pool as follows.

```python
res = pool.get_resource()
cur = r.table("data").run(res.conn)
for obj in cur:
    print(obj)
res.release()
```


It also supports `with` statement for automatically restore used connection back to the pool.

```python
with pool.get_resource() as res:
    cur = r.table("data").run(res.conn)
    for obj in cur:
        print(obj)
```