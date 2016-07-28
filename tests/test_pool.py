import rethinkdb as r
from future.moves.queue import Empty
from nose.tools import assert_raises
from rethinkpool import RethinkPool


def test_pool_create():
    max_conns = 50
    initial_conns = 10
    rp = RethinkPool(max_conns=max_conns, initial_conns=initial_conns)
    assert rp.current_conns == initial_conns


def test_create_connection():
    initial_conns = 0
    rp = RethinkPool(max_conns=10, initial_conns=initial_conns)
    res = rp.get_resource()
    assert rp.current_conns == (initial_conns + 1)
    assert rp._queue.empty()

    res.release()
    assert not rp._queue.empty()

    rp.get_resource()
    assert not rp._queue.empty()


def test_pool_full():
    n_conns = 10
    rp = RethinkPool(max_conns=n_conns, initial_conns=n_conns, get_timeout=0.5)
    assert rp._queue.full()

    bussy_resources = [rp.get_resource() for _ in range(n_conns)]
    assert rp._queue.empty()

    with assert_raises(Empty):
        res = rp.get_resource()

    bussy_resources[0].release()
    rp.get_resource()

    [res.release() for res in bussy_resources]
