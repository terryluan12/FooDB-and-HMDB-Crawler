from bs4 import BeautifulSoup as bs, Tag
from FooDB import parseFooDBId
from logger import logger
from psycopg import AsyncConnection, OperationalError, DatabaseError
from sql import insertCompoundDatabase, insertBioSpecDatabase, insertConcentrationDatabase, getCompoundIdFromFooDBId, updateHmdbId, getCompoundIdAndFooDBIdFromName
import psycopg
from dotenv import load_dotenv
import pandas as pd
import os
from utility import get_page_text, populate_databases
from typing import cast
import asyncio

from typing import List
import settings

semaphore = asyncio.Semaphore(10)

def getName(soup: bs) -> str:
    name: Tag = cast(Tag, soup.find("name"))
    if not name or not name.string:
        logger.warning(" No name")
        return ""
    return name.string

def getBiospecimens(soup: bs):
    location_tags = soup.find("biospecimen_locations")
    if not location_tags:
        logger.warning(" No biospecimen locations")
        return
    locations = [loc.text for loc in location_tags.find_all("biospecimen")]
    return locations

def getConcentrations(soup: bs, normal = True):
    conc_table = soup.find("normal_concentrations") if normal else soup.find("abnormal_concentrations")
    
    if not conc_table:
        logger.warning(f" {'Normal Concentrations' if normal else 'Abnormal Concentrations'} missing")
        return
    concs = conc_table.find_all("concentration", recursive=False)
    if not concs:
        logger.warning(f" {'Abnormal' if not normal else ''} Concentrations missing")
        return
        
    concentrations = []
    col_map = {
        "subject_age": "age",
        "patient_age": "age",
        "subject_sex": "sex",
        "patient_sex": "sex",
        "subject_condition": "condition",
        "patient_information": "condition",
        "concentration_value": "value",
        "concentration_units": "units"
    }
    
    for conc in concs:
        concentration = {}
        isQuantified = True
        unquantified_values = []
        
        for tag in conc.find_all(True, recursive=False):
            name = tag.name
            if name == "references":
                concentration["references"] = []
                references = tag.find_all("reference", recursive=False)
                for reference in references:
                    ref = {}
                    for ref_col in reference.find_all(True, recursive=False):
                        ref[ref_col.name] = ref_col.text
                    concentration["references"].append(ref)
            else:
                tag_value = None
                if name in col_map:
                    name = col_map[name]
                
                if tag.text == "Not Specified" or tag.text == "Not Quantified" or not tag.string:
                    if name == "value":
                        isQuantified = False
                        break
                    else:
                        if name != "age" and name != "sex":
                            unquantified_values.append(name)
                        tag_value = None
                else:
                    tag_value = tag.string
                
                concentration[name] = tag_value
        if isQuantified:
            concentrations.append(concentration)
            if unquantified_values:
                logger.warning(f" {','.join(unquantified_values)} is not specified/quantified")
                    
    return concentrations
    

async def parseHMDBId(conn: AsyncConnection, id: str):
    async with conn.cursor() as cur:
        async with semaphore:
            logger.info(f" Parsing HMDB with id: {id}")
            compound_id = None
            
            url = settings.HMDB_MET_PAGE + id + ".xml"
            print(f"With Url {url}")
            page_text = await get_page_text(url)
            soup = bs(page_text, features="xml")

            name = getName(soup)
            biospecimens = getBiospecimens(soup)
            concentrations = getConcentrations(soup, True)
            abconcentrations = getConcentrations(soup, False)
            
            result = await getCompoundIdAndFooDBIdFromName(conn, cur, name)
            if result:
                compound_id, db_foodb_id = result
            foodb_id_tag = soup.find("foodb_id")

            if foodb_id_tag and foodb_id_tag.string:
                parsed_foodb_id = foodb_id_tag.string
                if not compound_id:
                    logger.info(f" Couldn't find from name {name}. Using parsed foodb_id {parsed_foodb_id}")
                    compound_id = await getCompoundIdFromFooDBId(conn, cur, parsed_foodb_id)
                    if not compound_id:
                        compound_id = await parseFooDBId(conn, parsed_foodb_id)
                elif db_foodb_id != parsed_foodb_id:
                    logger.error(f" {id}: Issue aligning metabolite {name} with id { db_foodb_id } in database and parsed foodb_id {parsed_foodb_id}")
                    return
            
            if not compound_id:
                logger.warning(f" {id}: No fooDB ID. Creating compound without fooDB ID")
                compound_id = await insertCompoundDatabase(conn, cur, id, name, None, True, False)
            else:
                await updateHmdbId(conn, cur, compound_id, id)

            if not biospecimens:
                logger.warning(f"{id}: No Biospecimens")
            else: 
                try:
                    for biospec in biospecimens:
                        await insertBioSpecDatabase(conn, cur, compound_id, biospec)
                    
                except (OperationalError, DatabaseError) as e:
                    logger.error(f" {id}: Error inserting biospecimen: {e}")
                    await conn.rollback()
            
            if not concentrations:
                logger.info(" Concentrations missing")
            else:
                for concentration in concentrations:
                    await insertConcentrationDatabase(conn, cur, compound_id, concentration)
            
            if not abconcentrations:
                logger.info(" Abnormal Concentrations missing")
            else:
                for abconcentration in abconcentrations:
                    await insertConcentrationDatabase(conn, cur, compound_id, abconcentration)
    

async def crawlHMDB(conn: AsyncConnection) -> None:
    for page_num in range(settings.HMDB_START_PAGE, settings.HMDB_TOTAL_PAGES+1):
        url = settings.HMDB_CATALOG_PAGE + str(page_num)
        page = await get_page_text(url)
        soup = bs(page.text, "html.parser")
        met_link = soup.find_all("td", class_="metabolite-link")
        ids = [link.a.text for link in met_link]
        
        logger.info(f" Got ID's for page {page_num}")
        logger.info(f" ----------------------------------------------------------")

        
        for id in ids:
            await asyncio.gather(*[parseHMDBId(conn, id) for id in ids])
                      
    
async def main():
    csv = pd.read_csv("data/Missing_HMDB_IDS.csv")
    async with await psycopg.AsyncConnection.connect(dbname = os.getenv('PSQL_DATABASE'), 
                        user = os.getenv('PSQL_USERNAME'), 
                        host= os.getenv('PSQL_HOST'),
                        password = os.getenv('PSQL_PASSWORD'),
                        port = 5432) as conn:
        # print(csv)
        repopulate_foodmap = True
        await populate_databases(conn, repopulate_foodmap)
        await asyncio.gather(*[parseHMDBId(conn, str(id)) for id in csv["hmdb_id"]])

if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())
    
    