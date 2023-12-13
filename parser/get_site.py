from parser.data import sites
from parser.get_page import get_page


async def get_site(url: str, session, semaphore):

    root = await get_page(session, url, semaphore)

    sites.add((None, url, root[1], root[0]))
