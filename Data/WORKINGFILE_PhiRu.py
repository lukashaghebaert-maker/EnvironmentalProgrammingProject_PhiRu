import sqlite3
import pandas as pd
import numpy as np
import ast # This library turns string "[...]" into list [...]

#1-------
db_path = "impactdb.v1.0.2.dg_filled.db"  # <-- your path
conn = sqlite3.connect(db_path)

#2------- 
# List all tables
tables = pd.read_sql(
    "SELECT name FROM sqlite_master WHERE type='table';", conn)

all_total_tables = tables[tables["name"].str.startswith("Total")]["name"]

#2(L1)-------
# Concatenate to one big L1 dataframe
L1_list = []
for table_name in all_total_tables:
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
for category in L3:
    L3[category] = pd.concat(L3[category], ignore_index=True)

L3_Deaths = L3.get("Deaths")
L3_Injuries = L3.get("Injuries")
L3_Damage = L3.get("Damage")


#3-----

filter_criteria = L1["Main_Event"] == "Tropical Storm/Cyclone"
L1_TC = L1[filter_criteria].copy() #Copy is very imprtant to ensure original data isn't altered


tc_events = L1_TC["Event_ID"].unique()

def filter_L3_tc(df):
    return df[df["Event_ID"].isin(tc_events)].copy() #compact versin of boolean mask above

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
    
    ''' Filters the data frame according to the year you input. 
    The filter keeps everything after the year specified 
    (e.g. x>1900) '''
    
    if type(year) == int:
        year_mask = df["Start_Date_Year"]>year
        return df[year_mask].copy()
    else:
        print ("Year must be an int data type")
        
year_to_filter = 1900
L3_Deaths_TC_1900 = filter_year(L3_Deaths_TC, year_to_filter)
L3_Injuries_TC_1900 = filter_year(L3_Injuries_TC, year_to_filter)
L3_Damage_TC_1900 = filter_year(L3_Damage_TC, year_to_filter)

#----------5

#1.GID CLEANING FUNCTION (Applied to one cell at a time)
#def get_single_valid_gid(gid_entry):  # Checks every single GID at a time
    
    # 1. Handle empty or missing cells → return NaN
#    if pd.isna(gid_entry):
#        return np.nan  # Returns NaN if the cell is truly empty

def get_single_valid_gid(gid_entry):

    # 1. Handle empty or missing cells → return NaN
    if gid_entry is None or (isinstance(gid_entry, float) and np.isnan(gid_entry)):
        return np.nan

    # 2. Convert strings that LOOK like lists into real Python lists
    #    Examples:
    #    "['USA']"      → ['USA']
    #    "[['USA']]"    → [['USA']]
    #    "USA"          → stays as "USA"
    if isinstance(gid_entry, str):
        try:
            parsed = ast.literal_eval(gid_entry)  # Safely convert string → Python object
            # If literal_eval returns a list, use it
            if isinstance(parsed, list):
                gid_entry = parsed
        except (ValueError, SyntaxError):
            # If literal_eval fails, treat the string as a single element
            gid_entry = [gid_entry]

    # 3. Ensure the entry is ALWAYS treated as a list of strings
    #    Cases handled:
    #    - gid_entry = "USA"        → ['USA']
    #    - gid_entry = ['USA']      → ['USA']
    #    - gid_entry = [['USA']]    → ['USA']
    if isinstance(gid_entry, str):
        elements = [gid_entry]  # Wrap single string in a list
    else:
        # If it's a list, flatten and ensure all elements are strings
        # Example: [['USA']] → ['USA']
        flat_list = []
        for e in gid_entry:
            if isinstance(e, list):
                flat_list.extend(e)  # Flatten nested lists
            else:
                flat_list.append(e)
        # Convert all elements to strings and remove NaNs
        elements = [str(e) for e in flat_list if pd.notna(e)]

    # 4. Extract valid 3-letter country codes
    valid_codes = []  # Start an empty list to store valid country codes
    
    for e in elements:  # Loop through every cleaned element
        # Clean formatting: remove whitespace, take first 3 chars, force UPPERCASE
        # Examples:
        #   'AUS.10' → 'AUS'
        #   'chn'    → 'CHN'
        code = e.strip()[:3].upper()

        # Validation rule:
        # Must be exactly 3 letters AND contain only letters
        if len(code) == 3 and code.isalpha():
            valid_codes.append(code)

    # 5. Enforce "Single Valid GID"
    #    Only accept rows with EXACTLY ONE valid country code
    if len(valid_codes) == 1:
        return valid_codes[0]  # Return the clean code (e.g., 'CHN')
    else:
        return np.nan  # If zero or multiple codes found → discard row

# --- MAIN PROCESSING AND AGGREGATION FUNCTION ---
def clean_dataframe(df):
    df_clean = df.copy()

    # 1. IDENTIFY THE COLUMN
    if 'Administrative_Area_GID' in df_clean.columns:
        target_col = 'Administrative_Area_GID'

        print("TARGET COLUMN:", target_col)
        print("FIRST VALUES:\n", df_clean[target_col].head())
        print("COLUMN DTYPE:", df_clean[target_col].dtype)
        print("PYTHON TYPE OF VALUE:", type(df_clean[target_col].iloc[2]))

    elif 'Administrative_Areas_GID' in df_clean.columns:
        target_col = 'Administrative_Areas_GID'

        #Step 1: Convert string "[['USA']]" → [['USA']]
        df_clean[target_col] = df_clean[target_col].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )

        #Step 2: Flatten [['USA']] → ['USA']
        df_clean[target_col] = df_clean[target_col].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x
        )

        #Step 3: Convert ['USA'] → "['USA']" (string)
        df_clean[target_col] = df_clean[target_col].apply(
            lambda x: str([x]) if isinstance(x, str) else x
        )

        print("TARGET COLUMN:", target_col)
        print("FIRST VALUES:\n", df_clean[target_col].head())
        print("COLUMN DTYPE:", df_clean[target_col].dtype)
        print("PYTHON TYPE OF VALUE:", type(df_clean[target_col].iloc[2]))

    else:
        print("Error: Neither GID column found.")
        return df_clean

    #return df_clean
    
    # Debug: Confirm which column is being used
    print(f"Detected column: {target_col}")
    
    # Debug: Print before cleaning to see what we are dealing with
    print(f"Rows before cleaning: {len(df_clean)}")
    
    # A. Clean the GID column
    # Apply the complex cleaning function to every row in the 'Administrative_Area_GID' column
    df_clean[target_col] = df_clean[target_col].apply(get_single_valid_gid) 
    
    # B. Filter out the NaNs
    # Remove any row where the GID cleaning process returned NaN (discarding bad/multiple GID rows)
    df_clean = df_clean.dropna(subset=[target_col]) 
    
    # Debug: Print after cleaning
    print(f"Rows after cleaning: {len(df_clean)}")
    return df_clean


def aggregate_by_eventID(df_clean):
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
L3_Deaths_TC_1900_aggregated = aggregate_by_eventID(clean_dataframe(L3_Deaths_TC_1900))
L3_Damage_TC_1900_aggregated = aggregate_by_eventID(clean_dataframe(L3_Damage_TC_1900))
L3_Injuries_Damage_TC_1900_aggregated = aggregate_by_eventID(clean_dataframe(L3_Injuries_TC_1900))
#5------

#6-------

instance_tables = tables[tables["name"].str.startswith("Instance")]["name"].tolist()

L2 = {}  # dictionary of category -> dataframe

for table_name in instance_tables: #for each table that starts with instance
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
    L2.setdefault(category, []).append(df)

# Get only Deaths, Injuries and Damage
for category in L2:
    L2[category] = pd.concat(L2[category], ignore_index=True)

L2_Deaths = clean_dataframe(L2.get("Deaths"))
L2_Injuries = clean_dataframe(L2.get("Injuries"))
L2_Damage =clean_dataframe(L2.get("Damage"))


