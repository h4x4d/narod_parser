import asyncio

import aiohttp

from parser.get_site import get_site
from constants import LIMIT


async def gather_sites(sites):
    tasks = []
    semaphore = asyncio.Semaphore(LIMIT)

    async with aiohttp.ClientSession() as session:
        for site_link in sites:
            task = asyncio.create_task(get_site(site_link, session, semaphore))
            tasks.append(task)

        await asyncio.gather(*tasks)
    return
