from __future__ import absolute_import

import threading

import rethinkdb as r
from future.builtins import range
from future.moves.queue import Queue


class ConnectionResource(object):
    def __init__(self, queue, conn, host='localhost', port=28015, db=None, auth_key="", timeout=20, ssl=None, **kwargs):
        self._queue = queue
        if conn:
            self._conn = conn
        else:
            self._conn = r.connect(host, port, db, auth_key, timeout, ssl, **kwargs)

    @property
    def conn(self):
        return self._conn

    def release(self):
        self._queue.put_nowait(self._conn)

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class RethinkPool(object):
    def __init__(self, max_conn=10, get_timeout=10, host='localhost', port=28015, db=None, auth_key="", timeout=20,
                 ssl=None,
                 **kwargs):
        self.get_timeout = get_timeout
        if ssl is None:
            ssl = dict()

        self._ready_conns = Queue(max_conn)
        self._uncleaned_conns = Queue(max_conn)
        for _ in range(max_conn):
            self._ready_conns.put(r.connect(host, port, db, auth_key, timeout, ssl, **kwargs))

        self._internal_thread = threading.Thread(target=self.prepare_conns)
        self._internal_thread.setDaemon(True)
        self._internal_thread.start()

    def prepare_conns(self):
        while True:
            conn = self._uncleaned_conns.get()
            conn.reconnect(noreply_wait=False)
            self._ready_conns.put(conn)

    def get_connection(self):
        conn = self._ready_conns.get(True, self.get_timeout)
        return ConnectionResource(self._uncleaned_conns, conn)
