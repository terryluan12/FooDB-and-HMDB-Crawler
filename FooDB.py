from bs4 import BeautifulSoup as bs
import requests
from psycopg2 import OperationalError, DatabaseError
from typing import Dict, List
from psycopg2.extensions import connection, cursor
from sql import insertCompoundDatabase, connectFoodDatabase
import settings

from logger import logger

def getName(soup):
    name = soup.find("name")
    if not name or not name.string:
        logger.warning(" No name")
        return
    return name.string.strip()

def getClass(soup):
    met_class = soup.find("class")
    if not met_class or not met_class.string:
        logger.warning(" No class")
        return
    return met_class.string.strip()

def getFoods(soup):
    foods = {}
    food_table = soup.find("foods")
    
    no_data_foods = []
    zeroed_foods = []
    for row in food_table.find_all("food"):
        name_tag = row.find("name")
        if not name_tag or not name_tag.string:
            logger.warning(" No Food Name")
            continue
        name = name_tag.string.strip()
        average_value = row.find("average_value")
        max_value = row.find("max_value")
        min_value = row.find("min_value")
        
        if  (not average_value or not average_value.string) \
        and (not max_value or not max_value.string) \
        and (not min_value or not min_value.string):
            no_data_foods.append(name)
            continue
        
        foods[name] = {}
        try:
            foods[name]["average_value"] = float(average_value.string)
            foods[name]["max_value"] = float(max_value.string)
            foods[name]["min_value"] = float(min_value.string)
            if foods[name]["min_value"] == 0.0 and foods[name]["max_value"] == 0.0:
                zeroed_foods.append(name)
                del foods[name]
        except ValueError as ve:
            logger.warning(f" Value error with {ve}")
            del foods[name]
    if no_data_foods:
        logger.info(f" Foods with no data: {no_data_foods}")
    if zeroed_foods:
        logger.info(f" Foods with 0 min/max/average: {zeroed_foods}")
        
    return foods

def getFoodMap() -> Dict[str, List[str]]:
    food_map: Dict[str, List[str]] = {}
    for page_num in range(1, settings.FOODB_FOOD_TOTAL_PAGES+1):
        page = requests.get(settings.FOODDB_FOOD_CATALOG_URL + str(page_num))
        soup = bs(page.text, "html.parser")
        food_links = soup.find_all("a", class_="btn-show")
        for food_link in food_links:
            row = food_link.parent.parent
            row_elements = row.find_all(True, recursive=False)
            category = row_elements[4].string
            if category not in food_map:
                food_map[category] = []
            food_map[category].append(row_elements[1].string)
    return food_map

        
def parseFooDBId(conn: connection, cur: cursor, id: str) -> int:
    logger.info(f" Parsing FooDB with id: {id}")
    page = requests.get(settings.FOODB_MET_PAGE + id)
    soup = bs(page.text, features="xml")
    try:
        name = getName(soup)
        met_class = getClass(soup)
        foods = getFoods(soup)
        
    #     logger.debug(f"""
    # Information for {id}:
    # fooDB Name: {name}
    # Class: {met_class}
    # Associated Foods: {foods}
    #         """)
    except Exception as e:
        logger.error(f" {id}: {e}")
        return
    try:
        compound_id = insertCompoundDatabase(conn, cur, id, name, met_class, False, True)
        for food, value in foods.items():
            connectFoodDatabase(conn, cur, compound_id, food, value["average_value"], value["max_value"], value["min_value"])
        return compound_id
    except (OperationalError, DatabaseError) as e:
        logger.error(f" {id}: {e}")
        conn.rollback()

def crawlFooDB(conn: connection):
    for page_num in range(settings.FOODB_START_PAGE, settings.FOODB_TOTAL_PAGES+1):
        page = requests.get(settings.FOODB_CATALOG_PAGE + str(page_num))
        soup = bs(page.text, "html.parser")
        
        rows = soup.find_all("a", class_="btn-show")
        ids = [link.text for link in rows]

        logger.info(f" Got ID's for page {page_num}")
        logger.info(f" ----------------------------------------------------------")
    
        cur = conn.cursor()
        try:
            for id in ids:
                parseFooDBId(conn, cur, id)
        finally:
            cur.close()