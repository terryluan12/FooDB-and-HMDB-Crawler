import os
import logging
import requests
from bs4 import BeautifulSoup as bs

from logging import Logger
from typing import Dict

DATABASES = {
    "food_category":
        """
        CREATE TABLE food_category (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL UNIQUE
        )
        """,
    "food":
        """
        CREATE TABLE food (
        id SERIAL PRIMARY KEY,
        category_id INT,
        name VARCHAR(50) NOT NULL UNIQUE,
        CONSTRAINT fk_category_id FOREIGN KEY (category_id)
            REFERENCES food_category (id)
            ON DELETE SET NULL
        )
        """,
    "compound_class":
        """
        CREATE TABLE compound_class (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL UNIQUE
        )
        """,
    "compound": 
        """
        CREATE TABLE compound (
        id SERIAL PRIMARY KEY,
        class_id INT,
        name VARCHAR(100) NOT NULL UNIQUE,
        hmdb_id CHAR(11) UNIQUE,
        foodb_id CHAR(9) UNIQUE,
        CONSTRAINT fk_class_id FOREIGN KEY (class_id)
            REFERENCES compound_class (id)
            ON DELETE CASCADE
        )
        """,
    "food_compounds": 
        """
        CREATE TABLE food_compounds (
        compound_id INT NOT NULL,
        food_id INT NOT NULL,
        average_value REAL,
        max_value REAL,
        min_value REAL,
        PRIMARY KEY(compound_id, food_id),
        CONSTRAINT fk_compound_id FOREIGN KEY (compound_id)
            REFERENCES compound (id)
            ON DELETE CASCADE,
        CONSTRAINT fk_food_id FOREIGN KEY (food_id)
            REFERENCES food (id)
            ON DELETE CASCADE
        )
        """,
    "biospecimen": 
        """
        CREATE TABLE biospecimen (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL UNIQUE
        )
        """,
    "compound_biospecimens": 
        """
        CREATE TABLE compound_biospecimens (
        compound_id INT NOT NULL,
        biospecimen_id INT,
        PRIMARY KEY(compound_id, biospecimen_id),
        CONSTRAINT fk_compound_id FOREIGN KEY (compound_id)
            REFERENCES compound (id)
            ON DELETE CASCADE,
        CONSTRAINT fk_biospecimen_id FOREIGN KEY (biospecimen_id)
            REFERENCES biospecimen (id)
            ON DELETE CASCADE
        )
        """,
    "concentration": 
        """
        CREATE TABLE concentration (
        id SERIAL PRIMARY KEY,
        compound_id INT NOT NULL,
        biospecimen_id INT NOT NULL,
        value CHAR(50) NOT NULL,
        units CHAR(35) NOT NULL,
        age CHAR(35),
        sex CHAR(15),
        condition TEXT,
        comment TEXT,
        CONSTRAINT fk_compound_id FOREIGN KEY (compound_id)
            REFERENCES compound (id)
            ON DELETE CASCADE,
        CONSTRAINT fk_biospecimen_id FOREIGN KEY (biospecimen_id)
            REFERENCES biospecimen (id)
            ON DELETE CASCADE
        )
        """,
    "reference": 
        """
        CREATE TABLE reference (
        id SERIAL PRIMARY KEY,
        concentration_id INT NOT NULL,
        reference_text TEXT,
        pubmed_id CHAR(10),
        CONSTRAINT fk_concentration_id FOREIGN KEY (concentration_id)
            REFERENCES concentration (id)
            ON DELETE CASCADE
        )
        """,
}

def check_and_create(cur, name, creation_command) -> None:
    cur.execute("SELECT EXISTS(SELECT * from information_schema.tables WHERE table_name=%s)", (name,))
    if not cur.fetchone()[0]:
        cur.execute(creation_command)
        

def createDatabases(conn) -> None:
    cursor = conn.cursor()
    for database_name, create_command in DATABASES.items():
        check_and_create(cursor, database_name, create_command)
    conn.commit()
    cursor.close()
