import time
from rethinkdb import r
from rethinkpool import RethinkPool

pool = RethinkPool()

with pool.get_resource() as res2:
    print(list(r.table("trx").run(res2.conn)))

time.sleep(1)
