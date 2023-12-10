import sqlite3

import aiohttp
import aiosqlite
import asyncio
import validators
from bs4 import BeautifulSoup

from constants import DATABASE
from parser.proccess import make_links_absolute, get_plain_text
from parser.utils import check_binary, get_headers


async def get_page(session: aiohttp.ClientSession, url: str,
                   lock: asyncio.Lock, root=0):
    try:
        url = url.strip('/')
        if not validators.url(url):
            return

        db = await aiosqlite.connect(DATABASE)
        binary = check_binary(url)
        table = 'files' if binary else 'pages'
        async with lock:
            query = await (await db.execute(f'SELECT * FROM {table} WHERE url = ?',
                                        (url,))).fetchone()

        if query:
            if not binary and root:
                async with lock:
                    await db.execute('INSERT INTO children VALUES (?, ?, ?)',
                                     (None, root, query[0]))
                    await db.commit()
            return

        if not binary:
            async with lock:
                try:
                    index = (await db.execute('INSERT INTO pages VALUES (?, ?, ?, ?, ?)',
                                              (None, url, None, None, None))).lastrowid
                    await db.commit()
                except sqlite3.IntegrityError:
                    await db.rollback()
                    return

        await asyncio.sleep(0.1)
        async with session.get(url, headers=get_headers()) as response:
            print(url)
            await asyncio.sleep(0.1)

            if binary:
                if root > 0:
                    filename = url[url.rfind('/') + 1:].lower()
                    try:
                        async with lock:
                            await db.execute(
                                'INSERT INTO files VALUES(?, ?, ?, ?, ?, ?)',
                                (None, url, filename[:filename.rfind('.')],
                                 filename[filename.rfind('.') + 1:],
                                 await response.content.read(), root))
                            await db.commit()
                    except sqlite3.IntegrityError:
                        await db.rollback()
                        return
                return

            if response.status != 200:
                await asyncio.sleep(1)
                response = await session.get(url, headers=get_headers())

                if response.status != 200:
                    return

            text = await response.text(errors='replace')
            while '503' in text and 'Service' in text:
                await asyncio.sleep(1)
                response = await session.get(url, headers=get_headers())
                text = await response.text(errors='replace')

            text = make_links_absolute(text, url)
            text = text[:text.rfind('<!-- copyright (t2) -->')]

            soup = BeautifulSoup(text, features="html.parser")
            plain_text = await get_plain_text(soup)
            title = soup.title.string if soup.title else ""
            async with lock:
                await db.execute(
                    'UPDATE pages SET page_name = ?, pure_html = ?, '
                    'plain_text = ? WHERE id = ?',
                    (title, text, plain_text, index))

                if root > 0:
                    await db.execute(
                        'INSERT INTO children VALUES (?, ?, ?)',
                        (None, root, index))
                await db.commit()

            site = url[url.find('://') + 3:]
            site = site[:site.find('/')]
            tasks = []
            for link in soup.findAll('a'):
                link = link.get('href')
                if link and site in link:
                    task = asyncio.create_task(get_page(session, link, lock, index))
                    tasks.append(task)

            await asyncio.gather(*tasks)

            return index
    except aiohttp.ClientConnectorError:
        print('Client error:', url)
    except aiohttp.ServerDisconnectedError:
        print('Server error:', url)
    except aiohttp.ClientPayloadError:
        print('Payload error:', url)
    except asyncio.TimeoutError:
        print('Time error:', url)

