from parser.data import sites, troubles
from parser.get_page import get_page


async def get_site(url: str, session, semaphore):

    root = await get_page(session, url, semaphore)
    if not root:
        print(f'BIG BIG TROUBLES {url}')
        troubles.add(url)
    else:
        sites.add((None, url, root[1], root[0]))
