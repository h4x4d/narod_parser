import asyncio

import aiohttp

from database import fill_database
from parser.get_site import get_site
from constants import LIMIT, GROUP_SIZE

from parser.data import troubles


async def gather_sites(sites):
    tasks = []
    semaphore = asyncio.Semaphore(LIMIT)

    async with aiohttp.ClientSession() as session:
        for site_link in sites:
            task = asyncio.create_task(get_site(site_link, session, semaphore))
            tasks.append(task)

        await asyncio.gather(*tasks)
    return


async def split_and_gather_sites(sites, database):
    global troubles
    for i in range(0, len(sites), GROUP_SIZE):
        try:
            await gather_sites(sites[i:i + GROUP_SIZE])
            print(f'GROUP {i} finished parsing')
            await fill_database(database)
            print(f'GROUP {i} finished writing to db')
        except Exception as e:
            print(f'GROUP {sites[i:i + GROUP_SIZE]} failed. Reason: {e}. Adding to troubles')
            troubles |= set(sites[i:i + GROUP_SIZE])