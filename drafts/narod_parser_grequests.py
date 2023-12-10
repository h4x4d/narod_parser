from gevent import monkey as curious_george
curious_george.patch_all(thread=False, select=False)

# web
import requests
import grequests
import fake_useragent

# db
import aiosqlite


# analysis
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

# system
import os
import sys
import shutil

DATABASE_NAME = 'sites.db'
pages = []
children = []
sites = []


async def fill_database(name=DATABASE_NAME, rewrite=True):
    if rewrite and os.path.isfile(name):
        os.remove(name)
    async with aiosqlite.connect(DATABASE_NAME) as db:
        await db.execute('''
        CREATE TABLE pages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT,
          page_name TEXT,

          pure_html TEXT,
          plain_text TEXT
        )
        ''')

        await db.execute('''
        CREATE TABLE children (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          root_id INTEGER,
          child_id INTEGER,
          FOREIGN KEY (root_id)  REFERENCES pages(id),
          FOREIGN KEY (child_id)  REFERENCES pages(id)
        )
        ''')

        await db.execute('''
        CREATE TABLE sites (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT,
          site_name TEXT,
          root_id INTEGER,
          FOREIGN KEY (root_id)  REFERENCES pages(id)
        )
        ''')

        for page in pages:
            await db.execute('INSERT INTO pages VALUES(?, ?, ?, ?, ?)', page)
        for site in sites:
            await db.execute('INSERT INTO sites VALUES(?, ?, ?, ?)', site)
        for rel in children:
            await db.execute('INSERT INTO childern VALUES(?, ?, ?)', rel)

        await db.commit()

def exception_handler(request, exception):
    print("Request failed", request.url, exception)


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


def get_plain_text(soup):
    for script in soup(["script", "style"]):
        script.extract()

    text = soup.get_text()

    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    return '\n'.join(chunk for chunk in chunks if chunk)


def get_links(soup):
    for script in soup(["script", "style"]):
        script.extract()

    text = soup.get_text()

    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    return '\n'.join(chunk for chunk in chunks if chunk)


async def process_page(response: requests.models.Response, root=0):
    url = response.url
    text = response.text
    text = make_links_absolute(text, url)
    text = text[:text.rfind('<!-- copyright (t2) -->')]

    soup = BeautifulSoup(text, features="html.parser")
    plain_text = get_plain_text(soup)
    title = soup.title.string if soup.title else ""

    index = len(pages) + 1
    pages.append((index, url, str(title), text, plain_text))

    if root > 0:
        ch_i = len(children) + 1
        children.append((ch_i, root, index))

    site = url[url.find('://') + 3:]
    site = site[:site.find('/')]
    rs = []
    for link in soup.findAll('a'):
        link = link.get('href')
        if link and site in link and link.startswith('http'):
            rs.append(grequests.get(link, headers=get_headers()))

    for r in grequests.imap(rs, size=4, exception_handler=exception_handler):
        await process_page(r, index)
    return index