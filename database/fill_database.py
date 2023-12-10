from parser.data import *

import aiosqlite


async def fill_database(name):
    async with aiosqlite.connect(name) as db:
        for page in pages:
            await db.execute('INSERT INTO pages VALUES(?, ?, ?, ?, ?)', page)
        for site in sites:
            await db.execute('INSERT INTO sites VALUES(?, ?, ?, ?)', site)
        for rel in relations:
            await db.execute('INSERT INTO children VALUES(?, ?, ?)', rel)
        for file in files:
            await db.execute('INSERT INTO files VALUES(?, ?, ?, ?, ?, ?)', file)

        await db.commit()