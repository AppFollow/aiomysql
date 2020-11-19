import logging
import asyncio
import collections

from .connection import connect
from .utils import (
    CloseEvent,
    PoolContextManager,
    PoolAcquireContextManager
)

logger = logging.getLogger(__name__)


def create_pool(minsize=1, maxsize=10, pool_recycle=-1, **kwargs):
    coro = _create_pool(minsize=minsize, maxsize=maxsize,
                        pool_recycle=pool_recycle, **kwargs)
    return PoolContextManager(coro)


async def _create_pool(minsize=1, maxsize=10, pool_recycle=-1, **kwargs):
    pool = Pool(minsize=minsize, maxsize=maxsize,
                pool_recycle=pool_recycle, **kwargs)
    if minsize > 0:
        # TODO: try/except block
        async with pool._cond:
            await pool._fill_free(override_min=False)
    return pool


class Pool:
    def __init__(self, minsize, maxsize, pool_recycle, **kwargs):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize:
            raise ValueError("maxsize should be not less than minsize")
        self._minsize = minsize
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._free = collections.deque(maxlen=maxsize)
        self._cond = asyncio.Condition()
        self._used = set()
        self._close_state = CloseEvent(self._do_close)
        self._recycle = pool_recycle

    def __repr__(self):
        return "<{} [size:[{}:{}], free:{}]>".format(
            self.__class__.__name__,
            self.minsize,
            self.maxsize,
            self.freesize
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.ensure_closed()

    @property
    def echo(self):
        return self._echo

    @property
    def minsize(self):
        return self._minsize

    @property
    def maxsize(self):
        return self._free.maxlen

    @property
    def size(self):
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        return len(self._free)

    async def clear(self):
        async with self._cond:
            await self._do_clear()
            self._cond.notify()

    async def _do_clear(self):
        coroutines = []
        while self._free:
            connection = self._free.popleft()
            connection.close()
            coroutines.append(connection.ensure_closed())
        await asyncio.gather(*coroutines)

    def close(self):
        if not self._close_state.is_set():
            self._close_state.set()

    @property
    def closed(self):
        return self._close_state.is_set()

    async def ensure_closed(self):
        await self._close_state.wait()

    async def _do_close(self):
        async with self._cond:
            assert not self._acquiring, self._acquiring
            coroutines = []
            while self._free:
                connection = self._free.popleft()
                connection.close()
                coroutines.append(connection.ensure_closed())
            for connection in self._used:
                connection.close()
                coroutines.append(connection.ensure_closed())
            await asyncio.gather(*coroutines)

    # TODO: add timeout
    def acquire(self):
        return PoolAcquireContextManager(self)

    async def _acquire(self):
        if self.closed:
            raise RuntimeError('Cannot acquire connection after closing pool')
        async with self._cond:
            while True:
                await self._fill_free(override_min=True)
                if self.freesize:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    async def _fill_free(self, *, override_min):
        # free_size = len(self._free)
        # n = 0
        # while n < free_size:
        #     conn = self._free[-1]
        #     if conn._reader.at_eof() or conn._reader.exception():
        #         self._free.pop()
        #         conn.close()

        #     elif (self._recycle > -1 and
        #           self._loop.time() - conn.last_usage > self._recycle):
        #         self._free.pop()
        #         conn.close()

        #     else:
        #         self._free.rotate()
        #     n += 1

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                connection = await connect(**self._conn_kwargs)
                self._free.append(connection)
                self._cond.notify()
            finally:
                self._acquiring -= 1

        if self.freesize:
            return

        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                connection = await connect(**self._conn_kwargs)
                self._free.append(connection)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    def release(self, connection):
        assert connection in self._used, (connection, self._used)
        self._used.remove(connection)

        if not connection.closed:
            if connection.in_transaction:
                logger.warning(
                    "Connection %r is in transaction, closing it.",
                    connection
                )
                connection.close()
            else:
                if self.maxsize and self.freesize < self.maxsize:
                    self._free.append(connection)
                else:
                    connection.close()
        asyncio.create_task(self._wakeup())
