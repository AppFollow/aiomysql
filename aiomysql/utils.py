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
