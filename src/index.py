
import os
import psycopg
import json

from dotenv import load_dotenv
from HMDB import crawlHMDB
from FooDB import crawlFooDB
from utility import populate_databases
from logger import logger
import asyncio

repopulate_foodmap = False

async def main():
    
    load_dotenv()
    
    async with await psycopg.AsyncConnection.connect(dbname = os.getenv('PSQL_DATABASE'), 
                        user = os.getenv('PSQL_USERNAME'), 
                        host= os.getenv('PSQL_HOST'),
                        password = os.getenv('PSQL_PASSWORD'),
                        port = 5432) as conn:
        
        
        populate_databases(conn, repopulate_foodmap)
        
        await crawlFooDB(conn)
        await logger.info(" Finished crawling FooDB. Crawling HMDB")
        await crawlHMDB(conn)

if __name__ == "__main__":
    asyncio.run(main())
    