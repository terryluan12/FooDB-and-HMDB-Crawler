from bs4 import BeautifulSoup as bs
import requests
import psycopg2
from psycopg2 import OperationalError, DatabaseError
from utility import check_and_create
from dotenv import load_dotenv
import os
import logging

os.makedirs('logs', exist_ok=True)

logger = logging.getLogger(__name__)
logging.basicConfig(filename='logs/foodb.log', level=logging.INFO)

error_handler = logging.FileHandler("logs/error.log")
error_handler.setLevel(logging.ERROR)

logger.addHandler(error_handler)

food_memo = {}
class_memo = {None: None}


CATALOG_PAGE = "https://foodb.ca/compounds?filter=true&quantified=1&page="
MET_PAGE = "https://foodb.ca/compounds/"

TOTAL_PAGES = 151
START_PAGE = 1

def parseWebpage(conn):
    for page_num in range(START_PAGE, TOTAL_PAGES+1):
        page = requests.get(CATALOG_PAGE + str(page_num))
        soup = bs(page.text, "html.parser")
        
        rows = soup.find_all("a", class_="btn-show")
        ids = [link.text for link in rows]

        logger.info(f" Got ID's for page {page_num}")
        logger.info(f" ----------------------------------------------------------")
    
        cursor = conn.cursor()
        for id in ids:
            logger.info(f" Working with id: {id}")
            page = requests.get(MET_PAGE + id)
            soup = bs(page.text, features="xml")
            try:
                fooDB_name = getName(soup)
                met_class = getClass(soup)
                food_map = getAssociatedFoods(soup)
            except Exception as e:
                logger.error(f" {id}: {e}")
                continue
            
            try:
                foodb_insert_query = """
                    INSERT INTO foodb_compound (id, name, class_id)
                    VALUES (%s, %s, %s)
                """
                compound_class_insert_query = """
                    INSERT INTO compound_class (name)
                    VALUES (%s)
                    RETURNING id
                """
                compound_class_select_query = """
                    SELECT id FROM compound_class
                    WHERE name = %s
                """
                
                if met_class not in class_memo:
                    cursor.execute(compound_class_select_query, (met_class,))
                    class_id = cursor.fetchone()
                    if class_id:
                        class_memo[met_class] = class_id
                    else:
                        cursor.execute(compound_class_insert_query, (met_class,))
                        class_memo[met_class] = cursor.fetchone()[0]
                        conn.commit()
                foodb_data = (id, fooDB_name, class_memo[met_class])
                cursor.execute(foodb_insert_query, foodb_data)
                conn.commit()
        
                associated_food_insert_query = """
                    INSERT INTO associated_food (compound_id, food_id, average_value, max_value, min_value)
                    VALUES (%s, %s, %s, %s, %s)
                """
                food_item_insert_query = """
                    INSERT INTO food_item (name)
                    VALUES (%s)
                    RETURNING id
                """
                food_item_select_query = """
                    SELECT id FROM food_item
                    WHERE name = %s
                """
                for key, value in food_map.items():
                    if key not in food_memo:
                        
                        cursor.execute(food_item_select_query, (key,))
                        food_id = cursor.fetchone()
                        if food_id:
                            food_memo[key] = food_id
                        else:
                            cursor.execute(food_item_insert_query, (key,))
                            food_memo[key] = cursor.fetchone()[0]
                            conn.commit()
                        
                    associated_food_data = (id, food_memo[key], value["average_value"], value["max_value"], value["min_value"])
                    cursor.execute(associated_food_insert_query, associated_food_data)
                conn.commit()
            except (OperationalError, DatabaseError) as e:
                logger.error(f" {id}: {e}")
                conn.rollback()
        cursor.close()
                
            

            
        #     print(f"""
        # Information for {id}:
        # fooDB Name: {fooDB_name}
        # Class: {met_class}
        # Associated Foods: {food_map}
        #         """)
        
def parseId(conn, id, insertCompound=True):
    logger.info(f" Working with id: {id}")
    page = requests.get(MET_PAGE + id)
    soup = bs(page.text, features="xml")
    try:
        fooDB_name = getName(soup)
        met_class = getClass(soup)
        food_map = getAssociatedFoods(soup)
    except Exception as e:
        logger.error(f" {id}: {e}")
        return
    cursor = conn.cursor()
    try:
        foodb_insert_query = """
            INSERT INTO foodb_compound (id, name, class_id)
            VALUES (%s, %s, %s)
        """
        compound_class_insert_query = """
            INSERT INTO compound_class (name)
            VALUES (%s)
            RETURNING id
        """
        compound_class_select_query = """
            SELECT id FROM compound_class
            WHERE name = %s
        """
        if insertCompound:
            if met_class not in class_memo:
                cursor.execute(compound_class_select_query, (met_class,))
                class_id = cursor.fetchone()
                if class_id:
                    class_memo[met_class] = class_id
                else:
                    cursor.execute(compound_class_insert_query, (met_class,))
                    class_memo[met_class] = cursor.fetchone()[0]
                    conn.commit()
            foodb_data = (id, fooDB_name, class_memo[met_class])
            cursor.execute(foodb_insert_query, foodb_data)
            conn.commit()

        associated_food_insert_query = """
            INSERT INTO associated_food (compound_id, food_id, average_value, max_value, min_value)
            VALUES (%s, %s, %s, %s, %s)
        """
        food_item_insert_query = """
            INSERT INTO food_item (name)
            VALUES (%s)
            RETURNING id
        """
        food_item_select_query = """
            SELECT id FROM food_item
            WHERE name = %s
        """
        for key, value in food_map.items():
            if key not in food_memo:
                
                cursor.execute(food_item_select_query, (key,))
                food_id = cursor.fetchone()
                if food_id:
                    food_memo[key] = food_id
                else:
                    cursor.execute(food_item_insert_query, (key,))
                    food_memo[key] = cursor.fetchone()[0]
                    conn.commit()
                
            associated_food_data = (id, food_memo[key], value["average_value"], value["max_value"], value["min_value"])
            cursor.execute(associated_food_insert_query, associated_food_data)
        conn.commit()
    except (OperationalError, DatabaseError) as e:
        logger.error(f" {id}: {e}")
        conn.rollback()
    cursor.close()
    
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

def getAssociatedFoods(soup):
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
        logger.warning(f" Foods with no data: {no_data_foods}")
    if zeroed_foods:
        logger.warning(f" Foods with 0 min/max/average: {zeroed_foods}")
        
    return foods

        
def createDatabases(conn):
    cursor = conn.cursor()
    check_and_create(cursor, "compound_class", 
                        """
                        CREATE TABLE compound_class (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(50) NOT NULL UNIQUE
                        )
                        """)
    check_and_create(cursor, "food_item", 
                        """
                        CREATE TABLE food_item (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(50) NOT NULL UNIQUE
                        )
                        """)
    check_and_create(cursor, "foodb_compound", 
                        """
                        CREATE TABLE foodb_compound (
                        id CHAR(9) PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        class_id INT,
                        CONSTRAINT fk_class_id FOREIGN KEY (class_id)
                            REFERENCES compound_class (id)
                            ON DELETE CASCADE
                        )
                        """)
    check_and_create(cursor, "associated_food", 
                        """
                        CREATE TABLE associated_food (
                        compound_id CHAR(9) NOT NULL,
                        food_id INT NOT NULL,
                        average_value REAL NOT NULL,
                        max_value REAL NOT NULL,
                        min_value REAL NOT NULL,
                        PRIMARY KEY(compound_id, food_id),
                        CONSTRAINT fk_compound_id FOREIGN KEY (compound_id)
                            REFERENCES foodb_compound (id)
                            ON DELETE CASCADE,
                        CONSTRAINT fk_food_id FOREIGN KEY (food_id)
                            REFERENCES food_item (id)
                            ON DELETE CASCADE
                        )
                        """)
    conn.commit()
    cursor.close()
    

if __name__ == "__main__":
    load_dotenv()
    conn = psycopg2.connect(database = os.getenv('PSQL_DATABASE'), 
                        user = os.getenv('PSQL_USERNAME'), 
                        host= os.getenv('PSQL_HOST'),
                        password = os.getenv('PSQL_PASSWORD'),
                        port = 5432)
    createDatabases(conn)
    
    try:
        parseWebpage(conn)
    finally:
        if conn:
            conn.close()