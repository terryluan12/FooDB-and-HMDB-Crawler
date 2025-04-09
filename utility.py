from psycopg2 import sql
import psycopg2

def database_exists(db_name):
    query = sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = {}").format(sql.Identifier(db_name))
    conn = psycopg2.connect(
        dbname="postgres",  # default database to connect to
        user="your_username", 
        password="your_password", 
        host="localhost", 
        port="5432"
    )
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone() is not None
    
def check_and_create(cur, name, creation_command):
    cur.execute("SELECT EXISTS(SELECT * from information_schema.tables WHERE table_name=%s)", (name,))
    if not cur.fetchone()[0]:
        cur.execute(creation_command)
