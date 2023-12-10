import aiosqlite
import sqlite3
import asyncio


class Database:
    def __init__(self, db: aiosqlite.Connection, lock: asyncio.Lock):
        self.db: aiosqlite.Connection = db
        self.lock: asyncio.Lock = lock

    async def execute(self, query: str, args: tuple = None):
        if not args:
            args = tuple()
        try:
            result = (await asyncio.wait_for(self.execute_(query, args), timeout=5))
            return result
        except asyncio.TimeoutError:
            await self.db.rollback()
            print('timeouted')
            return

    async def execute_(self, query: str, args: tuple):
        async with self.lock:
            try:
                cursor = await self.db.execute(query, args)
                await self.db.commit()
                return cursor
            except sqlite3.IntegrityError:
                await self.db.rollback()
                print('integrity')
                return
