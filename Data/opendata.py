# -*- coding: utf-8 -*-
"""
Created on Mon Nov 10 13:47:07 2025

@author: PhiRu
"""
import sqlite3
import panda as pd

#putting the name of the file in one variable
db_file = "impactdb.v1.0.2.dg_filled.db"  # por ejemplo "data.db"

#connecting to the database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

#cursor is showing/listing all tables in the database
#sqlite_master is an internal table that stores the structure of the database; 
#cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#tables = cursor.fetchall()

import panda as pd
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
all_tables = [t[0] for t in cursor.fetchall()]
print("All tables in DB:", all_tables)


total_tables = [t for t in all_tables if t.startswith("Total")]

# Por simplicidad, si hay más de una tabla Total, podemos concatenarlas
total_tables = [t for t in all_tables if t.startswith("Total")]
L1 = pd.concat([pd.read_sql_query(f"SELECT * FROM {t}", conn) for t in total_tables], ignore_index=True)
print("L1 DataFrame loaded. Shape:",L1.shape)

#closing the connection
conn.close()
