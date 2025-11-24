# -*- coding: utf-8 -*-
"""
Created on Mon Nov 24 13:07:54 2025

@author: kristine espejo
"""

import sqlite3
import pandas as pd

db_path = "impactdb.v1.0.2.dg_filled.db"  # <-- your path

conn = sqlite3.connect(db_path)

# List all tables
tables = pd.read_sql(
    "SELECT name FROM sqlite_master WHERE type='table';", conn
)
print(tables)

total_tables = tables[tables["name"].str.startswith("Total")]["name"].tolist()

# Concatenate to one big L1 dataframe
L1_list = []
for tname in total_tables:
    df = pd.read_sql(f"SELECT * FROM {tname};", conn)
    df["source_table"] = tname
    L1_list.append(df)

L1 = pd.concat(L1_list, ignore_index=True)

spec_tables = tables[tables["name"].str.startswith("Specific")]["name"].tolist()
print(spec_tables)

L3 = {}  # dictionary of category -> dataframe

for tname in spec_tables:
    # simple heuristic, adapt to real names
    if "Deaths" in tname:
        cat = "Deaths"
    elif "Injuries" in tname:
        cat = "Injuries"
    elif "Damage" in tname:
        cat = "Damage"
    else:
        continue

    df = pd.read_sql(f"SELECT * FROM {tname};", conn)
    df["source_table"] = tname
    L3.setdefault(cat, []).append(df)

# Concatenate per category
for cat in L3:
    L3[cat] = pd.concat(L3[cat], ignore_index=True)

L3_Deaths = L3.get("Deaths")
L3_Injuries = L3.get("Injuries")
L3_Damage = L3.get("Damage")
