import asyncio

from parser.get_site import get_site


async def gather_sites(sites):
    tasks = []
    lock = asyncio.Lock()

    for site_link in sites:
        task = asyncio.create_task(get_site(site_link, lock))
        tasks.append(task)

    await asyncio.gather(*tasks)
    return
