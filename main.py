from constants import *
from parser import gather_sites
from database import create_database

import asyncio


async def main(filename=FILENAME, database=DATABASE):
    await create_database(database)

    sites = [i.strip() for i in open(filename)]
    await gather_sites(sites)


if __name__ == '__main__':
    asyncio.run(main(FILENAME))
