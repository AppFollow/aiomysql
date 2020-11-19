import asyncio
from collections.abc import Coroutine


class _ContextManager(Coroutine):

    __slots__ = ('_coro', '_obj')

    def __init__(self, coro):
        self._coro = coro
        self._obj = None

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self):
        return self._coro.close()

    @property
    def gi_frame(self):
        return self._coro.gi_frame

    @property
    def gi_running(self):
        return self._coro.gi_running

    @property
    def gi_code(self):
        return self._coro.gi_code

    def __next__(self):
        return self.send(None)

    def __iter__(self):
        return self._coro.__await__()

    def __await__(self):
        return self._coro.__await__()

    async def __aenter__(self):
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb):
        await self._obj.close()
        self._obj = None


class _ConnectionContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self._obj.close()
        else:
            await self._obj.ensure_closed()
        self._obj = None


class _SAConnectionContextManager(_ContextManager):
    async def __aiter__(self):
        result = await self._coro
        return result


class _TransactionContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        if exc_type:
            await self._obj.rollback()
        else:
            if self._obj.is_active:
                await self._obj.commit()
        self._obj = None


class PoolContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        self._obj.close()
        await self._obj.wait_closed()
        self._obj = None


class PoolAcquireContextManager:

    __slots__ = ('_connection', '_done', '_pool')

    def __init__(self, pool):
        self._pool = pool
        self._done = False
        self._connection = None

    async def __aenter__(self):
        if self._connection is not None or self._done:
            raise Exception('Connection is already acquired')
        self._connection = await self._pool._acquire()
        return self._connection

    async def __aexit__(self, exc_type, exc, tb):
        self._done = True
        connection = self._connection
        self._connection = None
        self._pool.release(connection)

    async def __await__(self):
        self._done = True
        return self._pool._acquire().__await__()


class _PoolConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from pool) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with pool:
            <block>
    """

    __slots__ = ('_pool', '_conn')

    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn

    def __enter__(self):
        assert self._conn
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None

    async def __aenter__(self):
        assert not self._conn
        self._conn = await self._pool.acquire()
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class CloseEvent:
    def __init__(self, on_close):
        self._close_init = asyncio.Event()
        self._close_done = asyncio.Event()
        self._on_close = on_close

    async def wait(self):
        await self._close_init.wait()
        await self._close_done.wait()

    def is_set(self):
        return self._close_done.is_set() or self._close_init.is_set()

    def set(self):
        if self._close_init.is_set():
            return

        task = asyncio.create_task(self._on_close())
        task.add_done_callback(self._cleanup)
        self._close_init.set()

    def _cleanup(self, task):
        self._on_close = None
        self._close_done.set()
