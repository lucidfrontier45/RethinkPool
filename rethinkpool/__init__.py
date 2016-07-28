from __future__ import absolute_import

from logging import getLogger

import rethinkdb as r
from future.builtins import range
from future.moves.queue import Queue

logger = getLogger("RethinkPool")


class ConnectionResource(object):

    def __init__(self, queue, conn, **kwds):
        self._queue = queue
        if conn:
            self._conn = conn
        else:
            self._conn = r.connect(**kwds)

    @property
    def conn(self):
        return self._conn

    def release(self):
        if self._conn:
            logger.info("release a connection")
            self._queue.put_nowait(self._conn)
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def __del__(self):
        self.release()


class RethinkPool(object):

    def __init__(self, max_conns=10, initial_conns=0, get_timeout=10, **kwds):
        """
        :param max_conns: maximum number of connections
        :param initial_conns: number of connections to be initially establish
        :param get_timeout: timeout for obtaining a connection from the queue
        :param host, port, ...: same as r.connect
        """

        self._current_conns = 0
        self.get_timeout = get_timeout

        self._connection_info = kwds

        self._queue = Queue(max_conns)
        for _ in range(min(max_conns, min(initial_conns, max_conns))):
            self._queue.put(self._create_connection())

    def _create_connection(self):
        conn = r.connect(**self._connection_info)
        self._current_conns += 1
        return conn

    @property
    def current_conns(self):
        return self._current_conns

    def get_resource(self):
        """
        obtain a connection resource from the queue
        :return: ConnectionResource object
        """
        if self._queue.empty() and self.current_conns < self._queue.maxsize:
            logger.info("create a new connection")
            conn = self._create_connection()
        else:
            logger.info("reuse a connection")
            conn = self._queue.get(True, self.get_timeout)
        return ConnectionResource(self._queue, conn)
