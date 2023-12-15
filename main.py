import datetime

from constants import DATABASE, FILENAME
from parser import split_and_gather_sites
from database import create_database, fill_database

import asyncio

from parser.data import troubles, links, sites as st

# sys.stdout = open('out.txt', 'w', encoding='utf-8')


async def main(filename=FILENAME, database=DATABASE):
    start = datetime.datetime.now()

    await create_database(database)

    sites = [i.strip() for i in open(filename)]
    await split_and_gather_sites(sites, database)

    print('END PARSING')
    print('NOW PARSING ERRORED')
    count = 0

    while troubles and count < 4:
        tr = troubles.copy()
        tr = [i for i in tr if all(j[1] != i for j in st)]
        troubles.clear()
        links.clear()
        await split_and_gather_sites(list(tr), database)
        count += 1

    print('ERRORED PARSED')
    print('END OF PARSING:', datetime.datetime.now() - start)


if __name__ == '__main__':
    asyncio.run(main())
