import asyncio
import logging
import weakref

from .connection import connect

logger = logging.getLogger(__name__)


def create_pool(init_count=2, max_count=10, **connect_kwargs):
    return Pool(init_count, max_count, **connect_kwargs)


class PoolAcquireContextManager:
    __slots__ = ("connection", "timeout", "done", "pool")

    def __init__(self, pool, timeout):
        self.pool = pool
        self.timeout = timeout
        self.done = False
        self.connection = None

    async def __aenter__(self):
        if self.connection is not None or self.done:
            raise Exception("Connection is already acquired")
        self._connection = await self.pool._acquire(self.timeout)
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        self.done = True
        connection = self.connection
        self.connection = None
        self.pool.release(connection)

    async def __await__(self):
        self.done = True
        return self.pool._acquire(self.timeout).__await__()


class Pool:
    def __init__(self, init_count, max_count, **connect_kwargs):
        if not isinstance(init_count, int) or init_count < 0:
            raise ValueError("init_count should be positive integer")
        if not isinstance(max_count, int) or max_count < init_count:
            raise ValueError("max_count should be not less than init_count")
        self._init_count = init_count
        self._max_count = max_count
        self._connect_kwargs = connect_kwargs
        self._queue = None
        self._used = weakref.WeakSet()

        self._closing = False
        self._closed = False
        self._initializing = False
        self._initialized = False

    def __repr__(self):
        return "<{} [size:[{}:{}], free:{}]>".format(
            self.__class__.__name__,
            self._init_count,
            self._max_count,
            self.freesize
        )

    def __await__(self):
        return self._async__init__().__await__()

    async def __aenter__(self):
        await self._async__init__()
        return self

    async def __aexit__(self, *exc):
        self.close()
        await self.ensure_closed()

    async def _async__init__(self):
        if self._initialized:
            return
        if self._initializing:
            raise RuntimeError("The pool is being initialized in another task")
        if self.closed:
            raise RuntimeError("The pool is already closed")
        self._initializing = True
        try:
            await self._initialize()
            return self
        finally:
            self._initializing = False
            self._initialized = True

    async def _initialize(self):
        self._queue = asyncio.LifoQueue(maxsize=self._max_count)
        tasks = []
        logger.debug("Fill the pool with %d connections", self._init_count)
        for _ in range(self._init_count):
            tasks.append(self._open_connection())
        await asyncio.gather(*tasks)

    @property
    def init_count(self):
        return self._init_count

    @property
    def max_count(self):
        return self._max_count

    @property
    def free_count(self):
        return self._queue.qsize()

    @property
    def size(self) -> int:
        return self.freesize + len(self._used)

    def close(self):
        if not self._close_state.is_set():
            self._close_state.set()

    @property
    def closed(self):
        return self._close_state.is_set()

    async def ensure_closed(self) -> None:
        await self._close_state.wait()

    async def _do_close(self):
        tasks = []
        while not self._queue.empty():
            connection = self._queue.get_nowait()
            connection.close()
            tasks.append(connection.ensure_closed())
        for connection in self._used:
            connection.close()
            tasks.append(connection.ensure_closed())
        await asyncio.gather(*tasks)

    async def _open_connection(self):
        connection = await connect(**self._connect_kwargs)
        self._queue.put_nowait(connection)

    def acquire(self, timeout=None):
        return PoolAcquireContextManager(self, timeout)

    async def _acquire(self, timeout):
        if self.closed:
            raise RuntimeError("Cannot acquire connection, the pool is closed")
        try:
            connection = await asyncio.wait_for(
                self._queue.get(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            raise RuntimeError("Timed out acquiring connection")
        assert not connection.closed, connection
        assert connection not in self._used, (connection, seld._used)
        self._used.add(connection)
        return connection

    def release(self, connection):
        assert connection in self._used, (connection, self._used)
        logger.debug("Remove connection from used")
        self._used.remove(connection)

        if connection.closed:
            logger.debug("Connection is already closed, discard it")
        else:
            if connection.in_transaction:
                logger.warning(
                    "Connection %r is in transaction, closing it",
                    connection
                )
                connection.close()
            else:
                if self.free_count < self.max_count:
                    logger.debug("Put open connection back to the pool")
                    self._queue.put_nowait(connection)
                else:
                    logger.debug("The pool is full, closing connection")
                    connection.close()
