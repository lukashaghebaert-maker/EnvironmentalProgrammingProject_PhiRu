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
for table_name in total_tables:
    df = pd.read_sql(f"SELECT * FROM {table_name};", conn)
    df["source_table"] = table_name
    L1_list.append(df)

L1 = pd.concat(L1_list, ignore_index=True)

spec_tables = tables[tables["name"].str.startswith("Specific")]["name"].tolist()
print(spec_tables)

L3 = {}  # dictionary of category -> dataframe

for table_name in spec_tables: #for each table that starts with specific
    #classifyinging tables into three impacts deaths, injuries & damage
    if "Deaths" in table_name:
        category = "Deaths"
    elif "Injuries" in table_name:
        category = "Injuries"
    elif "Damage" in table_name:
        category = "Damage"
    else:
        continue

    df = pd.read_sql(f"SELECT * FROM {table_name};", conn)
    df["source_table"] = table_name
    L3.setdefault(category, []).append(df)

# Concatenate per category
for cat in L3:
    L3[cat] = pd.concat(L3[cat], ignore_index=True)

L3_Deaths = L3.get("Deaths")
L3_Injuries = L3.get("Injuries")
L3_Damage = L3.get("Damage")

L1_TC = L1[L1["Main_Event"].str.contains("Tropical", case=False, na=False)].copy()
tc_events = L1_TC["Event_ID"].unique()

def filter_L3_tc(df):
    return df[df["Event_ID"].isin(tc_events)].copy()

L3_Deaths_TC = filter_L3_tc(L3_Deaths)
L3_Injuries_TC = filter_L3_tc(L3_Injuries)
L3_Damage_TC  = filter_L3_tc(L3_Damage)
