# -*- coding: utf-8 -*-
"""
Created on Mon Nov 10 13:47:07 2025

@author: kristine espejo
"""
import sqlite3

#giving the file
db_file = "impactdb.v1.0.2.dg_filled.db"  # por ejemplo "data.db"

#connecting to the database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

#listing of all tables in the database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print("tables found in the database:")
for t in tables:
    print("-", t[0])

#closing the connection
conn.close()