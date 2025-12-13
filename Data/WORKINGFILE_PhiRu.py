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
import ast          # This library turns string "[...]" into list [...]

#1.GID CLEANING FUNCTION (Applied to one cell at a time)
def get_single_valid_gid(gid_entry): # Checks every single GID at a time
    
    # Handle no data cells and returns it as NaNs
    if pd.isna(gid_entry): 
        return np.nan # Returns NaN if the cell is truly empty

# Currently the data that is GID is considered a string, we use this to fix strings and convert it to python list
    if isinstance(gid_entry, str) and gid_entry.startswith('[') and gid_entry.endswith(']'):
        try:
            gid_entry = ast.literal_eval(gid_entry) # ast.literal_eval safely converts the text into a real Python list
        except (ValueError, SyntaxError):
            pass # If the string cannot be converted, ignore the error and proceed

# Make sure all variable elements is a list of strings
    if not isinstance(gid_entry, list): # If the entry is NOT a list (ex: a single string like 'USA'), execute this block
        elements = [str(gid_entry)] # Wrap the single item in a list so we can loop over it
    else: # If the entry is a list, execute this block
        elements = [str(e) for e in gid_entry if pd.notna(e)] #Ensure every item in the list is a string and ignore any NaNs inside the list

    valid_codes = [] # Start an empty list to store valid country codes
    
    for e in elements: # Loop through every item in the cleaned list (e.g., 'Z03', 'CHN')
        # Clean formatting: remove whitespace, take first 3 chars, force UPPERCASE
        # 'AUS.10' -> 'AUS', 'chn' -> 'CHN'
        code = e.strip()[:3].upper() # Apply the cleaning and standardization
        
        # Validation Rule: 
        # Must be exactly 3 letters AND contain only letters (this excludes codes like 'Z03')
        if len(code) == 3 and code.isalpha(): 
            valid_codes.append(code) # If it passes the test, add it to our "Good List"
    
    # 4. Enforce "Single Valid GID"
    if len(valid_codes) == 1: # Check if we found exactly one valid country code
        return valid_codes[0] # If yes, return the code (e.g., 'CHN')
    else:
        return np.nan # If zero or multiple valid codes were found, return NaN (Discard the row)

# --- 2. THE MAIN PROCESSING AND AGGREGATION FUNCTION ---
def process_step_5(df):
    df_clean = df.copy() # Create a copy of the input data to work on safely
    
    # Debug: Print before cleaning to see what we are dealing with
    print(f"Rows before cleaning: {len(df_clean)}")
    
    # A. Clean the GID column
    # Apply the complex cleaning function to every row in the 'Administrative_Area_GID' column
    df_clean['Administrative_Area_GID'] = df_clean['Administrative_Area_GID'].apply(get_single_valid_gid) 
    
    # B. Filter out the NaNs
    # Remove any row where the GID cleaning process returned NaN (discarding bad/multiple GID rows)
    df_clean = df_clean.dropna(subset=['Administrative_Area_GID']) 
    
    # Debug: Print after cleaning
    print(f"Rows after cleaning: {len(df_clean)}")

    # --- C. FIXED AGGREGATION LOGIC (Prevents adding years) ---
    
    # 1. Define the columns we are grouping by
    group_cols = ['Event_ID', 'Administrative_Area_GID'] # The keys that must be identical to form a group
    
    # 2. Create the "Rule Book" for aggregation
    agg_rules = {} # This dictionary tells Pandas what math to do for each column
    
    # Loop through every column to decide what to do with it
    for col in df_clean.columns:
        if col in group_cols:
            continue # Skip the grouping keys—they are handled automatically by groupby
            
        # If it is a Numerical Impact column -> SUM it
        if col in ['Num_Min', 'Num_Max', 'Num_Approx']:
            agg_rules[col] = 'sum' # Add the numbers together
            
        # For Dates and everything else -> KEEP FIRST value
        # (This prevents adding 1992 + 1992)
        else:
            agg_rules[col] = 'first' # Just take the first value found in the group

    # 3. Apply the rules
    # Groups the rows, applies the specific SUM/FIRST rules, and flattens the result
    df_agg = df_clean.groupby(group_cols).agg(agg_rules).reset_index()
    
    return df_agg

# --- Run Again ---
# Execute the process on each of your filtered dataframes:
L3_Deaths_TC_1900_aggregated = process_step_5(L3_Deaths_TC_1900)
L3_Damage_TC_1900_aggregated = process_step_5(L3_Damage_TC_1900)
L3_Injuries_Damage_TC_1900_aggregated = process_step_5(L3_Injuries_TC_1900)
#5------
