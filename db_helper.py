from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import atexit
from urllib.parse import quote_plus as urlquote
from psycopg2.pool import ThreadedConnectionPool
from threading import Semaphore
 
class ReallyThreadedConnectionPool(ThreadedConnectionPool):
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self._semaphore = Semaphore(maxconn)
        super().__init__(minconn, maxconn, *args, **kwargs)

    def getconn(self, *args, **kwargs):
        self._semaphore.acquire()
        return super().getconn(*args, **kwargs)

    def putconn(self, *args, **kwargs):
        super().putconn(*args, **kwargs)
        self._semaphore.release()

 
class DBHelper:
    def __init__(self):
        self._connection_pool = None
 
    def initialize_connection_pool(self):
        pwd = "Xr6!g9I@p5"
        db_dsn = f"postgresql://dbspider:{urlquote(pwd)}@172.31.6.162:5432/bidata?connect_timeout=5"
        self._connection_pool = ReallyThreadedConnectionPool(1, 10, db_dsn)
 
    @contextmanager
    def get_resource(self, autocommit=True):
        if self._connection_pool is None:
            self.initialize_connection_pool()
 
        conn = None
        try:
            conn = self._connection_pool.getconn()
        except:
            try:
                conn = self._connection_pool.getconn()
            except:
                conn = self._connection_pool.getconn()
        conn.autocommit = autocommit
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor, conn
        finally:
            cursor.close()
            self._connection_pool.putconn(conn)
 
    def shutdown_connection_pool(self):
        if self._connection_pool is not None:
            self._connection_pool.closeall()
 
 
db_helper = DBHelper()
 
 
@atexit.register
def shutdown_connection_pool():
    db_helper.shutdown_connection_pool()