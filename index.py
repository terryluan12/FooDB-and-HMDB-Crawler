
import os
import psycopg2
import json

from dotenv import load_dotenv
from HMDB import crawlHMDB, parseHMDBId
from FooDB import crawlFooDB
from FooDB import getFoodMap
from sql import populateFoodDatabase, populateBiospecimenMemo, populateClassMemo, populateFoodCatMemo
from utility import createDatabases
from logger import logger

repopulate_foodmap = False

if __name__ == "__main__":
    load_dotenv()
    conn = psycopg2.connect(database = os.getenv('PSQL_DATABASE'), 
                        user = os.getenv('PSQL_USERNAME'), 
                        host= os.getenv('PSQL_HOST'),
                        password = os.getenv('PSQL_PASSWORD'),
                        port = 5432)
    createDatabases(conn)
    if repopulate_foodmap:
        os.makedirs("cache", exist_ok=True)
        with open("cache/food_map", "w") as file:
            food_map = getFoodMap()
            json.dump(food_map, file, indent=2)
        populateFoodDatabase(conn, food_map)
    try:
        populateBiospecimenMemo(conn)
        populateClassMemo(conn)
        populateFoodCatMemo(conn)
        crawlFooDB(conn)
        logger.info(" Finished crawling FooDB. Crawling HMDB")
        crawlHMDB(conn)
    finally:
        conn.close()
    