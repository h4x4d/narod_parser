import os

import aiosqlite


async def create_database(name, rewrite=True):
    if rewrite and os.path.isfile(name):
        os.remove(name)

    async with aiosqlite.connect(name) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS pages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT UNIQUE,
          page_name TEXT,

          pure_html TEXT,
          plain_text TEXT
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS children (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          root_id INTEGER,
          child_id INTEGER,
          FOREIGN KEY (root_id)  REFERENCES pages(id),
          FOREIGN KEY (child_id)  REFERENCES pages(id)
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS sites (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT UNIQUE,
          site_name TEXT,
          root_id INTEGER,
          FOREIGN KEY (root_id)  REFERENCES pages(id)
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS files (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT UNIQUE,
          filename TEXT,
          extension TEXT,
          file BLOB,
          page_id INTEGER,
          FOREIGN KEY (page_id)  REFERENCES pages(id)
        )
        """)
