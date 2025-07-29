from psycopg import AsyncConnection, AsyncCursor, sql
from typing import Dict, List
import settings
from logger import logger

async def insertFoodCategoryDatabase(conn: AsyncConnection, cur: AsyncCursor, name: str) -> int:
    cat_insert =   """
    INSERT INTO food_category (name)
    VALUES (%s)
    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
    RETURNING id
    """
    await cur.execute(cat_insert, (name,))
    await conn.commit()
    row = await cur.fetchone()
    
    if row is None:
        raise ValueError("No row returned")
    return row[0]
    
async def insertFoodDatabase(conn: AsyncConnection, cur: AsyncCursor, cat_id: int, food_name: str) -> int:
    food_insert = """
    INSERT INTO food (category_id, name)
    VALUES (%s, %s)
    returning id
    """
    await cur.execute(food_insert, (cat_id, food_name))
    await conn.commit()
    row = await cur.fetchone()
    
    if row is None:
        raise ValueError("No row returned")
    return row[0]

async def populateFoodDatabase(conn: AsyncConnection, food_map : Dict[str, List[str]]):
    cur = conn.cursor()
    
    for category, foods in food_map.items():
        if category not in settings.foodcat_memo:
            cat_id = await insertFoodCategoryDatabase(conn, cur, category)
            settings.foodcat_memo[category] = cat_id
            

        for food in foods:
            await insertFoodDatabase(conn, cur, settings.foodcat_memo[category], food)
    await insertFoodCategoryDatabase(conn, cur, "UNKNOWN")
    await conn.commit()
    await cur.close()
    
async def getFoodCatDatabase(conn: AsyncConnection, cur: AsyncCursor, cat_name: str) -> int:
    food_get = """
        SELECT id FROM food_category
        WHERE name= %s
    """
    
    await cur.execute(food_get, (cat_name,))
    row = await cur.fetchone()
    
    if row is None:
        raise ValueError("No row returned")
    return row[0]
    
async def getCompoundDatabase(conn: AsyncConnection, cur: AsyncCursor, id: str, is_hmdb: None):
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
    
    
async def insertClassDatabase(conn: AsyncConnection, cur: AsyncCursor, met_class: str):
    compound_class_insert = """
        INSERT INTO compound_class (name)
        VALUES (%s)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
    """
    if met_class not in settings.class_memo:
        await cur.execute(compound_class_insert, (met_class,))
        row = await cur.fetchone()
        if row is None:
            raise ValueError("No row returned")
        settings.class_memo[met_class] = row[0]
        await conn.commit()
            
async def insertCompoundDatabase(conn: AsyncConnection, cur: AsyncCursor, met_id: str, name: str, met_class: str, isHMDB=None, insertClass=True) -> int:
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
        await insertClassDatabase(conn, cur, met_class)
    data = (settings.class_memo[met_class], name, met_id)
    await cur.execute(compound_insert, data)
    await conn.commit()
    row = await cur.fetchone()
    if row is None:
        raise ValueError("No row returned")
    return row[0]

async def insertBioSpecDatabase(conn: AsyncConnection, cur: AsyncCursor, compound_id: int, biospec: str):
    biospec_insert = """
        INSERT INTO biospecimen (name)
        VALUES (%s)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
    """
    if biospec not in settings.biospec_memo:
        await cur.execute(biospec_insert, (biospec,))
        row = await cur.fetchone()
        
        if row is None:
            raise ValueError("No row returned")
        settings.biospec_memo[biospec] = row[0]
        await conn.commit()
    
    biospec_connect = """
        INSERT INTO compound_biospecimens (compound_id, biospecimen_id)
        VALUES (%s, %s)
    """
    await cur.execute(biospec_connect, (compound_id, settings.biospec_memo[biospec]))
    await conn.commit()
    
async def insertConcentrationDatabase(conn: AsyncConnection, cur: AsyncCursor, compound_id: int, concentration: Dict):
    concentration_insert = """
        INSERT INTO concentration ({fields})
        VALUES ({placeholders})
        returning id
    """
    reference_insert = """
        INSERT INTO reference ({fields})
        VALUES ({placeholders})
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
    
    query = sql.SQL(concentration_insert).format(
        fields=sql.SQL(', ').join(map(sql.Identifier, cols)),
        placeholders=sql.SQL(', ').join(sql.Placeholder() * len(values))
    )
    await cur.execute(query, values)
    row = await cur.fetchone()
    
    if row is None:
        raise ValueError("No row returned")
    conc_id = row[0]
    await conn.commit()
    
    if references:
        for reference in references:
            reference["concentration_id"] = conc_id
            cols = list(reference.keys())
            values = [reference[col] for col in cols]
            
            query = sql.SQL(reference_insert).format(
                fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
                placeholders=sql.SQL(", ").join(sql.Placeholder() * len(values)),
            )
            await cur.execute(query, values)
            await conn.commit()
            
async def connectFoodDatabase(conn: AsyncConnection, cur: AsyncCursor, compound_id: int, food: str, average_value, max_value, min_value):
    food_select = """
        SELECT id FROM food
        WHERE name = %s
    """
    food_connect = """
        INSERT INTO food_compounds (compound_id, food_id, average_value, max_value, min_value)
        VALUES (%s, %s, %s, %s, %s)
    """
    if food not in settings.food_memo:
        await cur.execute(food_select, (food,))
        row = await cur.fetchone()
        if row is None:
            raise ValueError("No row returned")
        food_id = row[0]
        if not food_id:
            unknown_cat_id = await getFoodCatDatabase(conn, cur, "UNKNOWN")
            food_id = await insertFoodDatabase(conn, cur, unknown_cat_id, food)
            logger.warning(f" food_id for {food} does not exist. Item created with category UNKNOWN.")
            
        settings.food_memo[food] = food_id
    
    food_data = (compound_id, settings.food_memo[food], average_value, max_value, min_value)
    
    await cur.execute(food_connect, food_data)
    
    await conn.commit()
    
async def getCompoundIdAndFooDBIdFromName(conn: AsyncConnection, cur: AsyncCursor, name: str) -> int:
    compound_select = """
        SELECT id, foodb_id FROM compound
        WHERE name = %s
    """
    await cur.execute(compound_select, (name,))
    return await cur.fetchone()

async def getCompoundIdFromFooDBId(conn: AsyncConnection, cur: AsyncCursor, foodb_id: str) -> int:
    compound_select = """
        SELECT id FROM compound
        WHERE foodb_id = %s
    """
    await cur.execute(compound_select, (foodb_id,))
    return await cur.fetchone()

async def updateHmdbId(conn: AsyncConnection, cur: AsyncCursor, compound_id: int, hmdb_id: str):
    compound_update = """
        UPDATE compound
        SET hmdb_id = %s
        WHERE id= %s
    """
    
    await cur.execute(compound_update, (hmdb_id, compound_id))
    await conn.commit()
    
    
async def populateBiospecimenMemo(conn: AsyncConnection):
    biospecimen_select = """
        SELECT id, name FROM biospecimen
    """
    cur = conn.cursor()
    await cur.execute(biospecimen_select)
    
    async for id, name in cur:
        settings.biospec_memo[name] = id
    
    await cur.close()
    
async def populateFoodCatMemo(conn: AsyncConnection):
    foodcat_select = """
        SELECT id, name FROM food_category
    """
    cur = conn.cursor()
    await cur.execute(foodcat_select)
    
    async for id, name in cur:
        settings.foodcat_memo[name] = id
    
    await cur.close()
    
async def populateClassMemo(conn: AsyncConnection):
    class_select = """
        SELECT id, name FROM compound_class
    """
    cur = conn.cursor()
    await cur.execute(class_select)
    
    async for id, name in cur:
        settings.class_memo[name] = id
    
    await cur.close()