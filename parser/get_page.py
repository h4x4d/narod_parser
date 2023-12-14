import aiohttp
import asyncio
import validators
from bs4 import BeautifulSoup

from parser.proccess import make_links_absolute, get_plain_text
from parser.utils import check_binary, get_headers
from parser.data import pages, files, relations, links


async def wait_for_page(url):
    while len([i for i in pages if i[1] == url]) == 0:
        await asyncio.sleep(1)


async def process_page(session, url, root, binary, semaphore):
    await asyncio.sleep(0.1)

    async with session.get(url, headers=get_headers()) as response:
        await asyncio.sleep(0.1)

        if response.status != 200:
            await asyncio.sleep(1)
            response = await session.get(url, headers=get_headers())

            if response.status != 200:
                semaphore.release()
                return

        if binary:
            if root > 0:
                filename = url[url.rfind('/') + 1:].lower()
                files.add((None, url, filename[:filename.rfind('.')],
                           filename[filename.rfind('.') + 1:],
                           await response.content.read(), root))
            semaphore.release()
            return

        text = await response.text(errors='replace')

        while '503' in text and 'Service' in text:
            await asyncio.sleep(1)
            response = await session.get(url, headers=get_headers())
            text = await response.text(errors='replace')

        text = make_links_absolute(text, url)
        text = text[:text.rfind('<!-- copyright (t2) -->')]

        try:
            soup = BeautifulSoup(text, features='html.parser')
        except AssertionError:
            semaphore.release()
            return
        plain_text = await get_plain_text(soup)
        title = soup.title.string if soup.title else ''

        index = len(pages) + 1
        pages.add((index, url, str(title), text, plain_text))

        if root > 0:
            relations.add((None, root, index))

        site = url[url.find('://') + 3:]
        if '/' in site:
            site = site[:site.find('/')]
        tasks = []
        '''
        for link in set(soup.findAll('a')):
            link = link.get('href')
            if link and site in link:
                task = asyncio.create_task(
                    get_page(session, link, semaphore, index))
                tasks.append(task)
        '''
        semaphore.release()

        if tasks:
            await asyncio.gather(*tasks)

        return index, title


async def safe_run(coro, url, semaphore: asyncio.BoundedSemaphore):
    try:
        await semaphore.acquire()
        print('start', url)
        return await coro
    except aiohttp.ClientConnectorError:
        print('Client error:', url)
    except aiohttp.ClientOSError:
        print('ClientOSError error:', url)
    except aiohttp.ServerDisconnectedError:
        print('Server error:', url)
    except aiohttp.ClientPayloadError:
        print('Payload error:', url)
    except asyncio.TimeoutError:
        print('Time error:', url)
    semaphore.release()


async def process_existing(root, url):
    try:
        await asyncio.wait_for(wait_for_page(url), timeout=100)
        relations.add(
            (None, root, [i for i in pages if i[1] == url][0][0]))

    except asyncio.TimeoutError:
        print('timeout', url)


async def get_page(session: aiohttp.ClientSession, url: str,
                   semaphore, root=0):
    url = url.strip('/')
    if url.rfind('/') < url.rfind('#'):
        url = url[:url.rfind('#')]
    binary = check_binary(url)

    if not validators.url(url) or 'panel' in url:
        return

    if url in links:
        if not binary:
            await process_existing(root, url)
        return

    links.add(url)

    return await safe_run(process_page(session, url, root, binary,
                                       semaphore), url, semaphore)
