import aiohttp
import aiosqlite
import asyncio
import validators
from bs4 import BeautifulSoup

from constants import DATABASE
from database import Database
from parser.proccess import make_links_absolute, get_plain_text
from parser.utils import check_binary, get_headers


async def get_page(session: aiohttp.ClientSession, url: str,
                   lock: asyncio.Lock, root=0):
    try:
        url = url.strip('/')
        if not validators.url(url):
            return

        db = Database(await aiosqlite.connect(DATABASE), lock)
        binary = check_binary(url)
        table = 'files' if binary else 'pages'
        query = await (await db.execute(f'SELECT * FROM {table} WHERE url = ?',
                                        (url,))).fetchone()

        if query:
            if not binary and root:
                await db.execute('INSERT INTO children VALUES (?, ?, ?)',
                                 (None, root, query[0]))
            return

        if not binary:
            cursor = (await db.execute(
                'INSERT INTO pages VALUES (?, ?, ?, ?, ?)',
                (None, url, None, None, None)))
            if cursor:
                index = cursor.lastrowid
            else:
                return

        await asyncio.sleep(0.1)
        async with session.get(url, headers=get_headers()) as response:
            print(url)
            await asyncio.sleep(0.1)

            if binary:
                if root > 0:
                    filename = url[url.rfind('/') + 1:].lower()
                    await db.execute(
                        'INSERT INTO files VALUES(?, ?, ?, ?, ?, ?)',
                        (None, url, filename[:filename.rfind('.')],
                         filename[filename.rfind('.') + 1:],
                         await response.content.read(), root))
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
            await db.execute(
                'UPDATE pages SET page_name = ?, pure_html = ?, '
                'plain_text = ? WHERE id = ?',
                (title, text, plain_text, index))

            if root > 0:
                await db.execute(
                    'INSERT INTO children VALUES (?, ?, ?)',
                    (None, root, index))

            site = url[url.find('://') + 3:]
            site = site[:site.find('/')]
            tasks = []
            for link in soup.findAll('a'):
                link = link.get('href')
                if link and site in link:
                    task = asyncio.create_task(
                        get_page(session, link, lock, index))
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
