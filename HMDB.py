from bs4 import BeautifulSoup as bs
import requests
from FooDB import parseFooDBId
from logger import logger
from psycopg2.extensions import connection, cursor
from psycopg2 import OperationalError, DatabaseError
from sql import insertCompoundDatabase, insertBioSpecDatabase, insertConcentrationDatabase, getCompoundIdFromFooDBId, updateHmdbId, getCompoundIdAndFooDBIdFromName

from typing import List
import settings

def getName(soup: bs) -> str:
    name = soup.find("name")
    if not name or not name.string:
        logger.warning(" No name")
        return
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
        
def parseHMDBId(conn: connection, cur: cursor, id: str):
    logger.info(f" Parsing HMDB with id: {id}")
    compound_id = None
    page = requests.get(settings.HMDB_MET_PAGE + id + ".xml")
    soup = bs(page.text, features="xml")

    name = getName(soup)
    biospecimens = getBiospecimens(soup)
    concentrations = getConcentrations(soup, True)
    abconcentrations = getConcentrations(soup, False)
    
    result = getCompoundIdAndFooDBIdFromName(conn, cur, name)
    if result:
        compound_id, db_foodb_id = result
    foodb_id_tag = soup.find("foodb_id")

    if foodb_id_tag and foodb_id_tag.string:
        parsed_foodb_id = foodb_id_tag.string
        if not compound_id:
            logger.info(f" Couldn't find from name {name}. Using parsed foodb_id {parsed_foodb_id}")
            compound_id = getCompoundIdFromFooDBId(conn, cur, parsed_foodb_id)
            if not compound_id:
                compound_id = parseFooDBId(conn, cur, parsed_foodb_id)
        elif db_foodb_id != parsed_foodb_id:
            logger.error(f" {id}: Issue aligning metabolite {name} with id { db_foodb_id } in database and parsed foodb_id {parsed_foodb_id}")
            return
    
    if not compound_id:
        logger.warning(f" {id}: No fooDB ID. Creating compound without fooDB ID")
        compound_id = insertCompoundDatabase(conn, cur, id, name, None, True, False)
    else:
        updateHmdbId(conn, cur, compound_id, id)

    if not biospecimens:
        logger.warning(" No Biospecimens")
        
    try:
        for biospec in biospecimens:
            insertBioSpecDatabase(conn, cur, compound_id, biospec)
        
    except (OperationalError, DatabaseError) as e:
        logger.error(f" {id}: Error inserting biospecimen: {e}")
        conn.rollback()
    
    if not concentrations:
        logger.info(" Concentrations missing")
    else:
        for concentration in concentrations:
            insertConcentrationDatabase(conn, cur, compound_id, concentration)
    
    if not abconcentrations:
        logger.info(" Abnormal Concentrations missing")
    else:
        for abconcentration in abconcentrations:
            insertConcentrationDatabase(conn, cur, compound_id, abconcentration)
    

def crawlHMDB(conn: connection) -> None:
    for page_num in range(settings.HMDB_START_PAGE, settings.HMDB_TOTAL_PAGES+1):
        page = requests.get(settings.HMDB_CATALOG_PAGE + str(page_num))
        soup = bs(page.text, "html.parser")
        met_link = soup.find_all("td", class_="metabolite-link")
        ids = [link.a.text for link in met_link]
        
        logger.info(f" Got ID's for page {page_num}")
        logger.info(f" ----------------------------------------------------------")

        cur = conn.cursor()
        
        try:
            for id in ids:
                parseHMDBId(conn, cur, id)
                        
        finally: 
            cur.close()
    