import datetime

from constants import DATABASE, FILENAME
from parser import gather_sites
from database import create_database, fill_database

import asyncio

from parser.data import troubles

# sys.stdout = open('out.txt', 'w', encoding='utf-8')


async def main(filename=FILENAME, database=DATABASE):
    start = datetime.datetime.now()

    await create_database(database)

    sites = [i.strip() for i in open(filename)]
    await gather_sites(sites)
    while troubles:
        tr = troubles.copy()
        troubles.clear()
        await gather_sites(list(tr))

    print('END PARSING')

    await fill_database(database)
    print('END:', datetime.datetime.now() - start)


if __name__ == '__main__':
    asyncio.run(main())
