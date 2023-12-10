import aiohttp
import aiosqlite

from constants import DATABASE
from parser.get_page import get_page


async def get_site(url: str, lock):
    async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=50)) as session:
        root = await get_page(session, url, lock)

        async with aiosqlite.connect(DATABASE) as db:
            async with lock:
                await db.execute(
                    'INSERT INTO sites VALUES(?, ?, ?, ?)',
                    (None, url, root[1], root[0]))
                await db.commit()
