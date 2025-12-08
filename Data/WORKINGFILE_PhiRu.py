import sqlite3
import pandas as pd
import numpy as np

#1-------
db_path = "impactdb.v1.0.2.dg_filled.db"  # <-- your path
conn = sqlite3.connect(db_path)

#2------- 
# List all tables
tables = pd.read_sql(
    "SELECT name FROM sqlite_master WHERE type='table';", conn)

total_tables = tables[tables["name"].str.startswith("Total")]["name"]

#2(L1)-------
# Concatenate to one big L1 dataframe
L1_list = []
for table_name in total_tables:
    df = pd.read_sql(f"SELECT * FROM {table_name};", conn)
    df["source_table"] = table_name
    L1_list.append(df)

L1 = pd.concat(L1_list, ignore_index=True)

#2(L3)--------
spec_tables = tables[tables["name"].str.startswith("Specific")]["name"].tolist()

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

# Get only Deaths, Injuries and Damage
for cat in L3:
    L3[cat] = pd.concat(L3[cat], ignore_index=True)

L3_Deaths = L3.get("Deaths")
L3_Injuries = L3.get("Injuries")
L3_Damage = L3.get("Damage")


#3-----
L1_TC = L1[L1["Main_Event"] == "Tropical Storm/Cyclone"].copy()
tc_events = L1_TC["Event_ID"].unique()

def filter_L3_tc(df):
    return df[df["Event_ID"].isin(tc_events)].copy()

L3_Deaths_TC = filter_L3_tc(L3_Deaths)
L3_Injuries_TC = filter_L3_tc(L3_Injuries)
L3_Damage_TC  = filter_L3_tc(L3_Damage)
date_cols = [
    "Start_Date_Year", "Start_Date_Month", "Start_Date_Day",
    "End_Date_Year", "End_Date_Month", "End_Date_Day"
]

L1_TC_dates = L1_TC[["Event_ID"] + date_cols].drop_duplicates()

def fill_dates(L3_tc):
    merged = L3_tc.merge(L1_TC_dates, on="Event_ID", how="left", suffixes=("", "_L1"))
    for col in date_cols:
        # If L3 has missing, fill with L1 value
        merged[col] = merged[col].fillna(merged[f"{col}_L1"])
        merged.drop(columns=[f"{col}_L1"], inplace=True)
    return merged

L3_Deaths_TC = fill_dates(L3_Deaths_TC)
L3_Injuries_TC = fill_dates(L3_Injuries_TC)
L3_Damage_TC = fill_dates(L3_Damage_TC)



#4---------- Aggregate by Administrative Area
def filter_year(df, year):
    return df[df["Start_Date_Year"]>year].copy()

L3_Deaths_TC_1900 = filter_year(L3_Deaths_TC, 1900)
L3_Injuries_TC_1900 = filter_year(L3_Injuries_TC, 1900)
L3_Damage_TC_1900 = filter_year(L3_Damage_TC, 1900)

#----------5
#I AM STILL FIXING THIS CODE BECAUSE THIS IS ADDING UP YEARS (currently clarifying with Ni if we would consider min or max)
import ast  # This library turns string "[...]" into list [...]

def get_single_valid_gid(gid_entry):
    #1 Handle NaNs (NO DATA)
    if pd.isna(gid_entry): 
        return np.nan

    #2 Fix "String that look like Lists"
    # If it looks like a list but is a string "['CHN']", convert it.
    if isinstance(gid_entry, str) and gid_entry.startswith('[') and gid_entry.endswith(']'):
        try:
            gid_entry = ast.literal_eval(gid_entry) #ast.literal_eval converts it to a list
        except (ValueError, SyntaxError):
            pass # Keep it as is if conversion fails

    #3 Standardize to List
    if not isinstance(gid_entry, list):
        elements = [str(gid_entry)]
    else:
        elements = [str(e) for e in gid_entry if pd.notna(e)]

    valid_codes = []
    for e in elements:
        # Clean formatting: remove whitespace, take first 3 chars, uppercase
        # 'AUS.10' -> 'AUS'
        code = e.strip()[:3]
            #e.strip removes accidental spaces
            #:3 chops the string at the third letter
        
        # Validation Rule: 
        # Must be exactly 3 letters. 
        # numeric_only=True logic handles the 'Z03' exclusion (digits make isalpha False)
        if len(code) == 3 and code.isalpha(): #
            valid_codes.append(code)
    
    # 4. Enforce "Single Valid GID"
    if len(valid_codes) == 1:
        return valid_codes[0]
    else:
        return np.nan

def process_step_5(df):
    df_clean = df.copy()
    
    # Debug: Print before cleaning to see what we are dealing with
    print(f"Rows before cleaning: {len(df_clean)}")
    
    df_clean['Administrative_Area_GID'] = df_clean['Administrative_Area_GID'].apply(get_single_valid_gid)
    
    # Filter out the NaNs
    df_clean = df_clean.dropna(subset=['Administrative_Area_GID'])
    
    # Debug: Print after cleaning
    print(f"Rows after cleaning: {len(df_clean)}")

    # Aggregate
    df_agg = df_clean.groupby(['Event_ID', 'Administrative_Area_GID']).sum(numeric_only=True).reset_index()
    
    return df_agg

# --- Run Again ---
L3_Deaths_TC_1900_aggregated = process_step_5(L3_Deaths_TC_1900)
L3_Damage_TC_1900_aggregated = process_step_5(L3_Damage_TC_1900)
L3_Injuries_Damage_TC_1900_aggregated = process_step_5(L3_Injuries_TC_1900)

#5------
