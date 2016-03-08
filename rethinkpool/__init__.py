from __future__ import absolute_import

import threading
import time
from logging import getLogger

import rethinkdb as r
from future.builtins import range
from future.moves.queue import Queue
from rethinkdb.errors import ReqlDriverError

logger = getLogger("RethinkPool")


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


def connect_to_rethinkdb(info):
    return r.connect(
        info["host"], info["port"], info["db"], info["auth_key"], info["timeout"], info["ssl"], **info["other"]
    )


class RethinkPool(object):
    def __init__(self, max_conns=10, initial_conns=0, get_timeout=10, host='localhost', port=28015, db=None,
                 auth_key="", timeout=20, ssl=None, reconnect_interval=20, **kwargs):
        """
        :param max_conns: maximum number of connections
        :param initial_conns: number of connections to be initially establish
        :param get_timeout: timeout for obtaining a connection from the queue
        :param host, port, ...: same as r.connect
        """

        self.reconnect_interval = reconnect_interval
        self.get_timeout = get_timeout
        if ssl is None:
            ssl = dict()

        self._connection_info = {
            "host": host,
            "port": port,
            "db": db,
            "auth_key": auth_key,
            "timeout": timeout,
            "ssl": ssl,
            "other": kwargs
        }

        self._ready_conns = Queue(max_conns)
        self._uncleaned_conns = Queue(max_conns)
        for _ in range(min(max_conns, initial_conns)):
            self._ready_conns.put(connect_to_rethinkdb(self._connection_info))

        self._internal_thread = threading.Thread(target=self._prepare_conns)
        self._internal_thread.setDaemon(True)
        self._internal_thread.start()

    @property
    def current_conns(self):
        return self._ready_conns.qsize() + self._uncleaned_conns.qsize()

    def _prepare_conns(self):
        while True:
            conn = self._uncleaned_conns.get()
            try:
                conn.reconnect(noreply_wait=False)
                self._ready_conns.put(conn)
                logger.info("reconnected, current ready conns = {}".format(self._ready_conns.qsize()))
            except ReqlDriverError:
                logger.warn("connection failed")
                self._uncleaned_conns.put(conn)
                time.sleep(self.reconnect_interval)

    def get_resource(self):
        """
        obtain a connection resource from the queue
        :return: ConnectionResource object
        """
        if self._ready_conns.empty() and self.current_conns < self._ready_conns.maxsize:
            logger.info("create a new connection")
            conn = connect_to_rethinkdb(self._connection_info)
        else:
            logger.info("reuse a connection")
            conn = self._ready_conns.get(True, self.get_timeout)
        return ConnectionResource(self._uncleaned_conns, conn)
