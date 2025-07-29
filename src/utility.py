import os
import logging
from bs4 import BeautifulSoup as bs
import aiohttp
import settings

from psycopg import AsyncConnection, AsyncCursor


from sql import populateFoodDatabase, populateBiospecimenMemo, populateClassMemo, populateFoodCatMemo

import json
from psycopg import AsyncConnection
from typing import Dict, List

DATABASES = {
    "food_category":
        """
        CREATE TABLE food_category (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL UNIQUE
        )
        """,
    "food":
        """
        CREATE TABLE food (
        id SERIAL PRIMARY KEY,
        category_id INT,
        name VARCHAR(50) NOT NULL UNIQUE,
        CONSTRAINT fk_category_id FOREIGN KEY (category_id)
            REFERENCES food_category (id)
            ON DELETE SET NULL
        )
        """,
    "compound_class":
        """
        CREATE TABLE compound_class (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL UNIQUE
        )
        """,
    "compound": 
        """
        CREATE TABLE compound (
        id SERIAL PRIMARY KEY,
        class_id INT,
        name VARCHAR(100) NOT NULL UNIQUE,
        hmdb_id CHAR(11) UNIQUE,
        foodb_id CHAR(9) UNIQUE,
        CONSTRAINT fk_class_id FOREIGN KEY (class_id)
            REFERENCES compound_class (id)
            ON DELETE CASCADE
        )
        """,
    "food_compounds": 
        """
        CREATE TABLE food_compounds (
        compound_id INT NOT NULL,
        food_id INT NOT NULL,
        average_value REAL,
        max_value REAL,
        min_value REAL,
        PRIMARY KEY(compound_id, food_id),
        CONSTRAINT fk_compound_id FOREIGN KEY (compound_id)
            REFERENCES compound (id)
            ON DELETE CASCADE,
        CONSTRAINT fk_food_id FOREIGN KEY (food_id)
            REFERENCES food (id)
            ON DELETE CASCADE
        )
        """,
    "biospecimen": 
        """
        CREATE TABLE biospecimen (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL UNIQUE
        )
        """,
    "compound_biospecimens": 
        """
        CREATE TABLE compound_biospecimens (
        compound_id INT NOT NULL,
        biospecimen_id INT,
        PRIMARY KEY(compound_id, biospecimen_id),
        CONSTRAINT fk_compound_id FOREIGN KEY (compound_id)
            REFERENCES compound (id)
            ON DELETE CASCADE,
        CONSTRAINT fk_biospecimen_id FOREIGN KEY (biospecimen_id)
            REFERENCES biospecimen (id)
            ON DELETE CASCADE
        )
        """,
    "concentration": 
        """
        CREATE TABLE concentration (
        id SERIAL PRIMARY KEY,
        compound_id INT NOT NULL,
        biospecimen_id INT NOT NULL,
        value CHAR(50) NOT NULL,
        units CHAR(35) NOT NULL,
        age CHAR(35),
        sex CHAR(15),
        condition TEXT,
        comment TEXT,
        CONSTRAINT fk_compound_id FOREIGN KEY (compound_id)
            REFERENCES compound (id)
            ON DELETE CASCADE,
        CONSTRAINT fk_biospecimen_id FOREIGN KEY (biospecimen_id)
            REFERENCES biospecimen (id)
            ON DELETE CASCADE
        )
        """,
    "reference": 
        """
        CREATE TABLE reference (
        id SERIAL PRIMARY KEY,
        concentration_id INT NOT NULL,
        reference_text TEXT,
        pubmed_id CHAR(10),
        CONSTRAINT fk_concentration_id FOREIGN KEY (concentration_id)
            REFERENCES concentration (id)
            ON DELETE CASCADE
        )
        """,
}

async def check_and_create(cur: AsyncCursor, name, creation_command) -> None:
    await cur.execute("SELECT EXISTS(SELECT * from information_schema.tables WHERE table_name=%s)", (name,))
    row = await cur.fetchone()
    if not row:
        raise ValueError("No row returned")
    if not row[0]:
        await cur.execute(creation_command)
        

async def createDatabases(conn: AsyncConnection) -> None:
    cursor = conn.cursor()
    for database_name, create_command in DATABASES.items():
        await check_and_create(cursor, database_name, create_command)
    await conn.commit()
    await cursor.close()



async def get_page_text(url: str):
    timeout = aiohttp.ClientTimeout(total=240)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            page_text = await resp.text()
            return page_text
        

async def getFoodMap() -> Dict[str, List[str]]:
    food_map: Dict[str, List[str]] = {}
    for page_num in range(1, settings.FOODB_FOOD_TOTAL_PAGES+1):
        url = settings.FOODDB_FOOD_CATALOG_URL + str(page_num)
        page_text = await get_page_text(url)
        soup = bs(page_text, "html.parser")
        food_links = soup.find_all("a", class_="btn-show")
        for food_link in food_links:
            row = food_link.parent.parent
            row_elements = row.find_all(True, recursive=False)
            category = row_elements[4].string
            if category not in food_map:
                food_map[category] = []
            food_map[category].append(row_elements[1].string)
    return food_map


async def populate_databases(conn: AsyncConnection, repopulate_foodmap: bool):
    await createDatabases(conn)
    if repopulate_foodmap:
        os.makedirs("cache", exist_ok=True)
        with open("cache/food_map", "w") as file:
            food_map = await getFoodMap()
            json.dump(food_map, file, indent=2)
        await populateFoodDatabase(conn, food_map)
    await populateBiospecimenMemo(conn)
    await populateClassMemo(conn)
    await populateFoodCatMemo(conn)
