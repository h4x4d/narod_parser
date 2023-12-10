import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup


def make_links_absolute(html, root_url):
    html = re.sub(r' src="([^"]+)"',
                  lambda m: ' src="' + urljoin(root_url, m.group(1)) + '"',
                  html)
    html = re.sub(r' href="([^"]+)"',
                  lambda m: ' href="' + urljoin(root_url, m.group(1)) + '"',
                  html)

    return html


async def get_plain_text(soup: BeautifulSoup):
    for script in soup(["script", "style"]):
        script.extract()

    text = soup.get_text()

    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    return '\n'.join(chunk for chunk in chunks if chunk)