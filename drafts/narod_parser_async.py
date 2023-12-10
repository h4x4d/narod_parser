# db
import aiosqlite

# web
import aiohttp
import asyncio

# analysis
import fake_useragent
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import validators

# system
import os
import sys

# sys.stdout = open('logs.txt', 'w', encoding='utf-8')

DATABASE_NAME = 'sites.db'


async def fill_database(name=DATABASE_NAME, rewrite=True):
    print(sites)
    if rewrite and os.path.isfile(name):
        os.remove(name)
    async with aiosqlite.connect(DATABASE_NAME) as db:
        await db.execute('''
        CREATE TABLE IF NOT EXISTS pages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT UNIQUE,
          page_name TEXT,
          
          pure_html TEXT,
          plain_text TEXT
        )
        ''')

        await db.execute('''
        CREATE TABLE IF NOT EXISTS children (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          root_id INTEGER,
          child_id INTEGER,
          FOREIGN KEY (root_id)  REFERENCES pages(id),
          FOREIGN KEY (child_id)  REFERENCES pages(id)
        )
        ''')

        await db.execute('''
        CREATE TABLE IF NOT EXISTS sites (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT UNIQUE,
          site_name TEXT,
          root_id INTEGER,
          FOREIGN KEY (root_id)  REFERENCES pages(id)
        )
        ''')

        await db.execute('''
        CREATE TABLE IF NOT EXISTS files (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT UNIQUE,
          filename TEXT,
          extension TEXT,
          file BLOB,
          page_id INTEGER,
          FOREIGN KEY (page_id)  REFERENCES pages(id)
        )
        ''')

        for page in pages:
            await db.execute('INSERT INTO pages VALUES(?, ?, ?, ?, ?)', page)
        for site in sites:
            await db.execute('INSERT INTO sites VALUES(?, ?, ?, ?)', site)
        for rel in children:
            await db.execute('INSERT INTO children VALUES(?, ?, ?)', rel)
        for file in files:
            await db.execute('INSERT INTO files VALUES(?, ?, ?, ?, ?, ?)', file)

        await db.commit()


pages = []
links = set()
children = []
sites = []
files = []

bad_ending = ['png', 'jpg', 'pdf', 'css', 'js', 'zip', 'rar', 'docx', 'doc',
              'pptx', 'xlsx', 'wmv', 'mp4', 'mp3', 'wma', 'jpeg', 'gif']
bad_ending += [i.upper() for i in bad_ending]

headers_ = {
    'Accept-Language': 'ru,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
}
ua = fake_useragent.UserAgent()


def get_headers():
    headers = headers_.copy()
    headers['User-Agent'] = ua.random
    return headers


def make_links_absolute(html, url):
    absolutize = lambda m: ' src="' + urljoin(url, m.group(1)) + '"'
    html = re.sub(r' src="([^"]+)"', absolutize, html)
    absolutize2 = lambda m: ' href="' + urljoin(url, m.group(1)) + '"'
    html = re.sub(r' href="([^"]+)"', absolutize2, html)
    return html


async def get_plain_text(soup):
    for script in soup(["script", "style"]):
        script.extract()

    text = soup.get_text()

    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    return '\n'.join(chunk for chunk in chunks if chunk)


async def get_page(session: aiohttp.ClientSession, url: str, root=0):
    url = url.strip('/')
    try:
        db = await aiosqlite.connect(DATABASE_NAME)
        if not validators.url(url):
            return

        await asyncio.sleep(0.1)
        if url in links:
            if root > 0:
                while len([i for i in pages if i[1] == url]) == 0:
                    await asyncio.sleep(1)
                ch_i = len(children) + 1
                children.append(
                    (ch_i, root, [i for i in pages if i[1] == url][0][0]))
            return
        links.add(url)
        async with session.get(url, headers=get_headers()) as response:
            print(url)
            await asyncio.sleep(0.1)
            if any(url.endswith(i) for i in bad_ending):
                if root > 0:
                    file_id = len(files) + 1
                    filename = url[url.rfind('/') + 1:].lower()
                    await db.execute(
                        'INSERT INTO files VALUES(?, ?, ?, ?, ?, ?)',
                        (file_id, url, filename[:filename.rfind('.')],
                         filename[filename.rfind('.') + 1:],
                         await response.content.read(), root))
                return

            if response.status != 200:
                await asyncio.sleep(1)
                response = await session.get(url, headers=get_headers())

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
            pages.append((index, url, str(title), text, plain_text))

            if root > 0:
                ch_i = len(children) + 1
                children.append((ch_i, root, index))

            site = url[url.find('://') + 3:]
            site = site[:site.find('/')]
            tasks = []
            for link in soup.findAll('a'):
                link = link.get('href')
                if link and site in link and link.strip(
                        '/') not in links and link.startswith('http'):
                    task = asyncio.create_task(get_page(session, link, index))
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


async def get_site(url: str):
    async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=20)) as session:
        root = await get_page(session, url)
        index = len(sites) + 1
        print(index)
        sites.append((index, url, pages[root - 1][2], root))


async def gather_sites(sites):
    # sem = asyncio.Semaphore(5)
    tasks = []

    for site_link in sites:
        task = asyncio.create_task(get_site(site_link))
        tasks.append(task)

    await asyncio.gather(*tasks)


async def main():
    sites = [i.strip() for i in open('../Narod_cut.txt')]
    await gather_sites(sites)
    # for i in sites:
    #     await gather_sites([i])
    #     print('next')
    #     await asyncio.sleep(30)

    await fill_database()


asyncio.run(main())
