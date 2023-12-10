import random

import aiohttp
import aiosqlite
import asyncio
import validators
from bs4 import BeautifulSoup

from constants import DATABASE
from database import Database
from parser.proccess import make_links_absolute, get_plain_text
from parser.utils import check_binary, get_headers
from parser.data import *


async def wait_for_page(url):
    while len([i for i in pages if i[1] == url]) == 0:
        await asyncio.sleep(1)
    return


async def get_page(session: aiohttp.ClientSession, url: str,
                   lock: asyncio.Lock, root=0):
    url = url.strip('/')
    if url.rfind('/') < url.rfind('#'):
        url = url[:url.rfind('#')]
    binary = check_binary(url)
    if not validators.url(url) or 'panel' in url:
        return

    if url in links:
        if not binary:
            try:
                await asyncio.wait_for(wait_for_page(url), timeout=30)
                relations.add((None, root, [i for i in pages if i[1] == url][0][0]))
            except asyncio.TimeoutError:
                print('timeout', url)

        return
    links.add(url)
    print('good', url)

    try:

        await asyncio.sleep(0.1)
        async with session.get(url, headers=get_headers()) as response:
            await asyncio.sleep(0.1)

            if response.status != 200:
                await asyncio.sleep(1)
                response = await session.get(url, headers=get_headers())

                if response.status != 200:
                    return

            if binary:
                if root > 0:
                    filename = url[url.rfind('/') + 1:].lower()
                    files.add((None, url, filename[:filename.rfind('.')],
                               filename[filename.rfind('.') + 1:],
                               await response.content.read(), root))
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

            index = len(pages) + 1
            pages.add((index, url, str(title), text, plain_text))

            if root > 0:
                relations.add((None, root, index))

            site = url[url.find('://') + 3:]
            site = site[:site.find('/')]
            tasks = []
            for link in set(soup.findAll('a')):
                link = link.get('href')
                if link and site in link:
                    task = asyncio.create_task(
                        get_page(session, link, lock, index))
                    tasks.append(task)

            if tasks:
                await asyncio.gather(*tasks)

            return index, title

    except aiohttp.ClientConnectorError:
        print('Client error:', url)
    except aiohttp.ServerDisconnectedError:
        print('Server error:', url)
    except aiohttp.ClientPayloadError:
        print('Payload error:', url)
    except asyncio.TimeoutError:
        print('Time error:', url)
