import aiosqlite
import sqlite3
import asyncio

TIMEOUT = 5

class Database:
    def __init__(self, db: aiosqlite.Connection, lock: asyncio.Lock):
        self.db: aiosqlite.Connection = db
        self.lock: asyncio.Lock = lock

    async def run_(self, coro):
        return await asyncio.wait_for(coro, timeout=5.0)

    async def execute(self, query: str, args: tuple = None, commit=True):
        if not args:
            args = tuple()
        async with self.lock:
            try:
                cursor = self.db.execute(query, args)
                cursor = await asyncio.wait_for(cursor, timeout=TIMEOUT)
                if commit:
                    await asyncio.wait_for(self.db.commit(), timeout=TIMEOUT)
                return cursor
            except sqlite3.IntegrityError:
                await self.db.rollback()
                return
            except asyncio.TimeoutError as e:
                await self.db.rollback()
                print('timeouted', e, query, args)
                return

    async def execute_many(self, queries):
        async with self.lock:
            try:
                for query, args in queries:
                    await self.execute(query, args, False)
                await asyncio.wait_for(self.db.commit(), timeout=1.0)
            except sqlite3.IntegrityError:
                await self.db.rollback()
                return
            except asyncio.TimeoutError as e:
                await self.db.rollback()
                print('timeouted', e, query, args)
                return
