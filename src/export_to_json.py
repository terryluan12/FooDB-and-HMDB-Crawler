import os
import pandas as pd
from sqlalchemy import create_engine, Engine
import json
from dotenv import load_dotenv

load_dotenv()

def get_engine() -> Engine:
        user = os.getenv('PSQL_USERNAME')
        password = os.getenv('PSQL_PASSWORD')
        host = os.getenv('PSQL_HOST')
        database = os.getenv('PSQL_DATABASE')

        engine = create_engine(f"postgresql+psycopg://{user}:{password}@{host}/{database}")
        
        return engine

def export_to_json(engine: Engine):
        
        select_query = "SELECT reference.id, compound.name, COALESCE(compound.hmdb_id, '') AS hmdb_id, COALESCE(compound.foodb_id, '') AS foodb_id, value, units, age, COALESCE(sex, '') AS sex, COALESCE(condition, '') AS condition, COALESCE(comment, '') AS comment, COALESCE(reference.reference_text, '') AS reference_text, COALESCE(reference.pubmed_id, '') AS pubmed_id FROM compound_biospecimens AS cbs \
        LEFT JOIN compound ON compound_id=compound.id \
        LEFT JOIN concentration ON concentration.compound_id=compound.id \
        LEFT JOIN reference ON concentration.id=reference.concentration_id \
        WHERE cbs.biospecimen_id = 1"

        df = pd.read_sql_query(select_query, con=engine)
        df = df.apply(lambda x: x.str.strip() if x.dtype == object else x)
        result = df.to_json(orient="records")
        parsed = json.loads(result)
        with open("json.json", "w") as f:
                json.dump(parsed, f, indent=4)

def main():
        engine = get_engine()
        with engine.connect() as conn, conn.begin():
                export_to_json(conn)
                
if __name__ == "__main__":
        main()