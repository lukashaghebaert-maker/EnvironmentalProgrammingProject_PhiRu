# -*- coding: utf-8 -*-
"""
Created on Mon Nov 10 13:47:07 2025

@author: PhiRu
"""
import sqlite3
import pandas as pd

#Saving the database name in a variable
db_file = "impactdb.v1.0.2.dg_filled.db"

#connecting to the database & creating a cursor
db_connection = sqlite3.connect(db_file)
cursor = db_connection.cursor()

#cursor is showing/listing all tables in the database

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#all_tables = [t[0] for t in cursor.fetchall()]
#print("All tables in DB:", all_tables)
all_tables = []
for table in cursor.fetchall():
    all_tables.append(table[0])
print("All tables in DB:", all_tables)


#total_tables = [t for t in all_tables if t.startswith("Total")]'
total_tables = []
for table in all_tables:
    if table.startswith("Total"):
        total_tables.append(table)

dataframes = []
for table_name in total_tables:
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, db_connection)
    dataframes.append(df)

L1 = pd.concat(dataframes, ignore_index=True)
print("L1 DataFrame loaded. Shape:",L1.shape)

#closing the connection
db_connection.close()
