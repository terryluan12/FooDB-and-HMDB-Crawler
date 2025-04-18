from psycopg2.extensions import connection, cursor
from typing import Dict, List
import settings
from logger import logger
from psycopg2.extensions import AsIs

def insertFoodCategoryDatabase(conn: connection, cur: cursor, name: str) -> int:
    cat_insert =   """
    INSERT INTO food_category (name)
    VALUES (%s)
    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
    RETURNING id
    """
    cur.execute(cat_insert, (name,))
    conn.commit()
    return cur.fetchone()[0]
    
def insertFoodDatabase(conn: connection, cur: cursor, cat_id: int, food_name: str) -> int:
    food_insert = """
    INSERT INTO food (category_id, name)
    VALUES (%s, %s)
    returning id
    """
    cur.execute(food_insert, (cat_id, food_name))
    conn.commit()
    return cur.fetchone()[0]

def populateFoodDatabase(conn: connection, food_map : Dict[str, List[str]]):
    cur = conn.cursor()
    
    for category, foods in food_map.items():
        if category not in settings.foodcat_memo:
            cat_id = insertFoodCategoryDatabase(conn, cur, category)
            settings.foodcat_memo[category] = cat_id
            

        for food in foods:
            insertFoodDatabase(conn, cur, settings.foodcat_memo[category], food)
    insertFoodCategoryDatabase(conn, cur, "UNKNOWN")
    conn.commit()
    cur.close()
    
def getFoodCatDatabase(conn: connection, cur: cursor, cat_name: str) -> int:
    food_get = """
        SELECT id FROM food_category
        WHERE name= %s
    """
    
    cur.execute(food_get, (cat_name,))
    return cur.fetchone()
    
def getCompoundDatabase(conn: connection, cur: cursor, id: str, is_hmdb: None):
    if is_hmdb is None:
        logger.error(" Must input isHMDB")
        raise
    id_type = "hmdb_id" if is_hmdb else "foodb_id" 
    
    compound_select = f"""
        SELECT id FROM compound
        WHERE {id_type} = %s
    """
    cur.execute(compound_select, (id,))
    return cur.fetchone()
    
    
def insertClassDatabase(conn: connection, cur: cursor, met_class: str):
    compound_class_insert = """
        INSERT INTO compound_class (name)
        VALUES (%s)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
    """
    if met_class not in settings.class_memo:
        cur.execute(compound_class_insert, (met_class,))
        settings.class_memo[met_class] = cur.fetchone()[0]
        conn.commit()
            
def insertCompoundDatabase(conn: connection, cur: cursor, met_id: str, name: str, met_class: str, isHMDB=None, insertClass=True) -> int:
    if isHMDB == None:
        logger.error(" Must input isHMDB")
        raise
    id_type = "hmdb_id" if isHMDB else "foodb_id" 
    compound_insert = f"""
        INSERT INTO compound (class_id, name, {id_type})
        VALUES (%s, %s, %s)
        returning id
    """
    if insertClass:
        insertClassDatabase(conn, cur, met_class)
    data = (settings.class_memo[met_class], name, met_id)
    cur.execute(compound_insert, data)
    conn.commit()
    return cur.fetchone()[0]

def insertBioSpecDatabase(conn: connection, cur: cursor, compound_id: int, biospec: str):
    biospec_insert = """
        INSERT INTO biospecimen (name)
        VALUES (%s)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
    """
    if biospec not in settings.biospec_memo:
        cur.execute(biospec_insert, (biospec,))
        settings.biospec_memo[biospec] = cur.fetchone()[0]
        conn.commit()
    
    biospec_connect = """
        INSERT INTO compound_biospecimens (compound_id, biospecimen_id)
        VALUES (%s, %s)
    """
    cur.execute(biospec_connect, (compound_id, settings.biospec_memo[biospec]))
    conn.commit()
    
def insertConcentrationDatabase(conn: connection, cur: cursor, compound_id: int, concentration: Dict):
    concentration_insert = f"""
        INSERT INTO concentration (%s)
        VALUES %s
        returning id
    """
    reference_insert = f"""
        INSERT INTO reference (%s)
        VALUES %s
    """
    references = None
    if "references" in concentration:
        references = concentration["references"]
        del concentration["references"]
        
    concentration["biospecimen_id"] = settings.biospec_memo[concentration["biospecimen"]]
    del concentration["biospecimen"]
    concentration["compound_id"] = compound_id
    
    
    cols = list(concentration.keys())
    values = [concentration[col] for col in cols]
    
    cur.execute(concentration_insert, (AsIs(','.join(cols)), tuple(values)))
    conc_id = cur.fetchone()[0]
    conn.commit()
    
    if references:
        for reference in references:
            reference["concentration_id"] = conc_id
            cols = list(reference.keys())
            values = [reference[col] for col in cols]
            cur.execute(reference_insert, (AsIs(','.join(cols)), tuple(values)))
            conn.commit()
            
def connectFoodDatabase(conn: connection, cur: cursor, compound_id: int, food: str, average_value, max_value, min_value):
    food_select = """
        SELECT id FROM food
        WHERE name = %s
    """
    food_connect = """
        INSERT INTO food_compounds (compound_id, food_id, average_value, max_value, min_value)
        VALUES (%s, %s, %s, %s, %s)
    """
    if food not in settings.food_memo:
        cur.execute(food_select, (food,))
        food_id = cur.fetchone()
        if not food_id:
            unknown_cat_id = getFoodCatDatabase(conn, cur, "UNKNOWN")
            food_id = insertFoodDatabase(conn, cur, unknown_cat_id, food)
            logger.warning(f" food_id for {food} does not exist. Item created with category UNKNOWN.")
            
        settings.food_memo[food] = food_id
    
    food_data = (compound_id, settings.food_memo[food], average_value, max_value, min_value)
    
    cur.execute(food_connect, food_data)
    
    conn.commit()
    
def getCompoundIdAndFooDBIdFromName(conn: connection, cur: cursor, name: str) -> int:
    compound_select = """
        SELECT id, foodb_id FROM compound
        WHERE name = %s
    """
    cur.execute(compound_select, (name,))
    return cur.fetchone()

def getCompoundIdFromFooDBId(conn: connection, cur: cursor, foodb_id: str) -> int:
    compound_select = """
        SELECT id FROM compound
        WHERE foodb_id = %s
    """
    cur.execute(compound_select, (foodb_id,))
    return cur.fetchone()

def updateHmdbId(conn: connection, cur: cursor, compound_id: int, hmdb_id: str):
    compound_update = """
        UPDATE compound
        SET hmdb_id = %s
        WHERE id= %s
    """
    
    cur.execute(compound_update, (hmdb_id, compound_id))
    conn.commit()
    
    
def populateBiospecimenMemo(conn: connection):
    biospecimen_select = """
        SELECT id, name FROM biospecimen
    """
    cur = conn.cursor()
    cur.execute(biospecimen_select)
    
    for id, name in cur:
        settings.biospec_memo[name] = id
    
    cur.close()
    
def populateFoodCatMemo(conn: connection):
    foodcat_select = """
        SELECT id, name FROM food_category
    """
    cur = conn.cursor()
    cur.execute(foodcat_select)
    
    for id, name in cur:
        settings.foodcat_memo[name] = id
    
    cur.close()
    
def populateClassMemo(conn: connection):
    class_select = """
        SELECT id, name FROM compound_class
    """
    cur = conn.cursor()
    cur.execute(class_select)
    
    for id, name in cur:
        settings.class_memo[name] = id
    
    cur.close()