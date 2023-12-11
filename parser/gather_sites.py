import asyncio

import aiohttp

from parser.get_site import get_site


async def gather_sites(sites):
    tasks = []

    async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=50)) as session:
        for site_link in sites:
            task = asyncio.create_task(get_site(site_link, session))
            tasks.append(task)

        await asyncio.gather(*tasks)
    return
