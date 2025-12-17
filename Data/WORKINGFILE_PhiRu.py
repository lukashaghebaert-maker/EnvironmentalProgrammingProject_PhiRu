import sqlite3
import pandas as pd
import numpy as np
import ast # This library turns string "[...]" into list [...]
import matplotlib.pyplot as plt
import seaborn as sns

#1-------
db_path = "impactdb.v1.0.2.dg_filled.db"  # <-- database
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

#5----------

# -----GID CLEANING FUNCTION (Applied to one cell at a time) -----

def get_single_valid_gid(gid_entry):#Checks every single GID at a time

#Handle empty or missing cells -> return NaN
    if gid_entry is None or (isinstance(gid_entry, float) and np.isnan(gid_entry)):
        return np.nan 

    #Convert strings that LOOK like lists into real Python lists
        #Examples:
            #    "['USA']"      -> ['USA']
            #    "[['USA']]"    -> [['USA']]
            #    "USA"          -> stays as "USA"
    if isinstance(gid_entry, str):
        try: #used ast module, turns strings into lists
            check_stringorlist = ast.literal_eval(gid_entry)  #convert string to python object
            # if literal_eval returns a list, use it
            if isinstance(check_stringorlist, list):
                gid_entry = check_stringorlist
        except (ValueError, SyntaxError):
            # If literal_eval fails, treat the string as a single element
            gid_entry = [gid_entry]

    #Ensure the entry is ALWAYS treated as a list of strings
        #Cases handled:
    #    gid_entry = "USA"        -> ['USA']
    #    gid_entry = ['USA']      -> ['USA']
    #    gid_entry = [['USA']]    -> ['USA']
    
    if isinstance(gid_entry, str):
        elements = [gid_entry]  #wrap single string in a list
    else:
        #If it's a list, flatten and ensure all elements are strings
        #Example: [['USA']] -> ['USA']
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
# Execute the process on each of our filtered dataframes:
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

#---- Using  Event_ID from ‘L3_*_1900_aggregated’ filter the events from ’ L2_*`, name as ‘L2_*_filter`
#Extract Event ID from L3
L3_deaths_ids = L3_Deaths_TC_1900_aggregated["Event_ID"].unique()
L3_injuries_ids = L3_Injuries_Damage_TC_1900_aggregated["Event_ID"].unique()
L3_damage_ids = L3_Damage_TC_1900_aggregated["Event_ID"].unique()

#Filter L2 using these Event_ID's from L3
L2_Deaths_filter = L2_Deaths[L2_Deaths["Event_ID"].isin(L3_deaths_ids)].copy()
L2_Injuries_filter = L2_Injuries[L2_Injuries["Event_ID"].isin(L3_injuries_ids)].copy()
L2_Damage_filter = L2_Damage[L2_Damage["Event_ID"].isin(L3_damage_ids)].copy()

#----Using Administrative Area of L3_aggregated and L2_filter, get the same GIS and compute the difference between each impact category 
# Equation is (‘L3_*_1900_aggregated’/ ‘L2_*_filter`)/ ‘L2_*_filter`.


# --- Rename L2 GID column to match L3, AreaS to Area (more prone to error if not changed)
L2_Deaths_filter = L2_Deaths_filter.rename(columns={"Administrative_Areas_GID": "Administrative_Area_GID"})
L2_Injuries_filter = L2_Injuries_filter.rename(columns={"Administrative_Areas_GID": "Administrative_Area_GID"})
L2_Damage_filter = L2_Damage_filter.rename(columns={"Administrative_Areas_GID": "Administrative_Area_GID"})

# --- Merge L3 and L2 ---
merged_deaths = L3_Deaths_TC_1900_aggregated.merge(
    L2_Deaths_filter,
    on=["Event_ID", "Administrative_Area_GID"],
    suffixes=("_L3", "_L2")
)

merged_injuries = L3_Injuries_Damage_TC_1900_aggregated.merge(
    L2_Injuries_filter,
    on=["Event_ID", "Administrative_Area_GID"],
    suffixes=("_L3", "_L2")
)

merged_damage = L3_Damage_TC_1900_aggregated.merge(
    L2_Damage_filter,
    on=["Event_ID", "Administrative_Area_GID"],
    suffixes=("_L3", "_L2")
)

# --- Keep only the required columns (including both GIDs) ---
cols_to_keep = [
    "Event_ID",
    "Administrative_Area_GID",
    "Num_Min_L3", "Num_Max_L3", "Num_Approx_L3",
    "Num_Min_L2", "Num_Max_L2", "Num_Approx_L2"
]

merged_deaths = merged_deaths[cols_to_keep].copy()
merged_injuries = merged_injuries[cols_to_keep].copy()
merged_damage = merged_damage[cols_to_keep].copy()

# Compute relative differences
def vectorized_rel_diff(df, col):
    L3 = df[f"{col}_L3"]
    L2 = df[f"{col}_L2"]

    # Rule 3: If either is NaN → 0
    cond_nan = L3.isna() | L2.isna()

    # Rule 2: If both zero → 0
    cond_both_zero = (L3 == 0) & (L2 == 0)

    # Rule 1: If L3 > 0 and L2 = 0 → 1
    cond_L3_pos_L2_zero = (L3 > 0) & (L2 == 0)

    # Rule 4: Normal case → (L3 - L2) / L2
    normal_case = (L3 - L2) / L2

    # Build final vector using np.select
    return np.select(
        [cond_nan, cond_both_zero, cond_L3_pos_L2_zero],
        [0,        0,             1],
        default=normal_case
    )

impact_columns = ["Num_Min", "Num_Max", "Num_Approx"]

for col in impact_columns:
    merged_deaths[f"{col}_rel_diff"] = vectorized_rel_diff(merged_deaths, col)
    merged_injuries[f"{col}_rel_diff"] = vectorized_rel_diff(merged_injuries, col)
    merged_damage[f"{col}_rel_diff"] = vectorized_rel_diff(merged_damage, col)

# Compute average relative difference per category
avg_rel_diff_deaths = merged_deaths[[c for c in merged_deaths.columns if "rel_diff" in c]].mean()
avg_rel_diff_injuries = merged_injuries[[c for c in merged_injuries.columns if "rel_diff" in c]].mean()
avg_rel_diff_damage = merged_damage[[c for c in merged_damage.columns if "rel_diff" in c]].mean()

# --- Task 7

# Load EM-DAT Excel file
emdat = pd.read_excel("EMDAT.xlsx", sheet_name="EM-DAT Data")

emdat = emdat[[
    "ISO",
    "Start Year", "Start Month",
    "End Year", "End Month", 'Total Deaths', 'No. Injured', "Total Damage ('000 US$)", "Total Damage, Adjusted ('000 US$)"
]].copy()

cols_for_matching = [
    "Event_ID",
    "Administrative_Area_GID",
    "Start_Date_Year", "Start_Date_Month",
    "End_Date_Year", "End_Date_Month",
    "Num_Min", "Num_Max", "Num_Approx"
]

L2_Deaths_match = L2_Deaths_filter[cols_for_matching].copy()
L2_Injuries_match = L2_Injuries_filter[cols_for_matching].copy()
L2_Damage_match = L2_Damage_filter[cols_for_matching].copy()

match_deaths = L2_Deaths_match.merge(
    emdat,
    left_on=["Administrative_Area_GID", "Start_Date_Year", "Start_Date_Month", "End_Date_Year", "End_Date_Month"],
    right_on=["ISO", "Start Year", "Start Month", "End Year", "End Month"],
    how="inner"
)

match_injuries = L2_Injuries_match.merge(
    emdat,
    left_on=["Administrative_Area_GID", "Start_Date_Year", "Start_Date_Month", "End_Date_Year", "End_Date_Month"],
    right_on=["ISO", "Start Year", "Start Month", "End Year", "End Month"],
    how="inner"
)

match_damage = L2_Damage_match.merge(
    emdat,
    left_on=["Administrative_Area_GID", "Start_Date_Year", "Start_Date_Month", "End_Date_Year", "End_Date_Month"],
    right_on=["ISO", "Start Year", "Start Month", "End Year", "End Month"],
    how="inner"
)

cols_final = [
    "Event_ID",
    "ISO",
    "Administrative_Area_GID",
    "Start_Date_Year", "Start_Date_Month",
    "End_Date_Year", "End_Date_Month",
    "Start Year", "Start Month", "End Year", "End Month",
    "Num_Min", "Num_Max", "Num_Approx",
    "Total Deaths",
    "No. Injured",
    "Total Damage ('000 US$)",
    "Total Damage, Adjusted ('000 US$)"
]

match_deaths = match_deaths[cols_final].copy()
match_injuries = match_injuries[cols_final].copy()
match_damage = match_damage[cols_final].copy()

EM_DAT_Wikimapcts_Matched = pd.concat(
    [match_deaths, match_injuries, match_damage],
    ignore_index=True
    )

# --- TASK 7 CONTINUATION ---

def process_and_plot_impacts(df, category_name, emdat_col):
    """
    1. Calculates Wikimpacts Mean.
    2. Calculates Relative Difference vs EM-DAT.
    3. Categorizes into bins.
    4. Plots and saves the result.
    """
    # Work on a copy to avoid SettingWithCopy warnings
    df = df.copy()

    # 1. Calculate Wikimpacts Mean (Row-wise mean of Min, Max, Approx)
    # We use mean(axis=1) which ignores NaNs automatically. 
    df['Wikimpact_Mean'] = df[['Num_Min', 'Num_Max']].mean(axis=1)
    
    # 2. Calculate Relative Difference: (Wikimpacts - EM_DAT) / EM_DAT
    # We must handle cases where EM_DAT is 0 or NaN to avoid infinite errors.
    
    # Extract series for easier handling
    wiki_val = df['Wikimpact_Mean']
    emdat_val = df[emdat_col]
    
    # Define logic for division
    # Case A: Both are 0 -> 0 diff (Perfect Match)
    # Case B: EM_DAT is 0 but Wiki > 0 -> Treat as High Positive (set to 1.0 for binning)
    # Case C: Standard Formula
    
    conditions = [
        (emdat_val == 0) & (wiki_val == 0), # Both zero
        (emdat_val == 0) & (wiki_val > 0),  # EM_DAT zero, Wiki positive
        (emdat_val.isna()) | (wiki_val.isna()) # Missing data
    ]
    
    choices = [
        0.0,  # Perfect match
        1.0,  # Arbitrary high number to push it into +50% bin
        0.0
    ]
    
    # Calculate standard formula
    standard_calc = (wiki_val - emdat_val) / emdat_val
    
    # Apply logic
    df['Relative_Diff'] = np.select(conditions, choices, default=standard_calc)
    
    # Drop rows where we couldn't calculate a difference (NaNs)
    df = df.dropna(subset=['Relative_Diff'])

    # 3. Sort into 5 categories
    # Bins: 
    #   < -0.5       -> -50% less
    #   -0.5 to -0.3 -> -30% less
    #   -0.3 to 0.3  -> Perfect Match
    #   0.3 to 0.5   -> +30% more
    #   > 0.5        -> +50% more
    
    bins = [-np.inf, -0.5, -0.3, 0.3, 0.5, np.inf]
    labels = ['-50% less', '-30% less', '"Perfect" Match', '+30% more', '+50% more']
    
    df['Impact_Category'] = pd.cut(df['Relative_Diff'], bins=bins, labels=labels)

    # 4. Visualization
    plt.figure(figsize=(10, 6))
    
    # Count the values for the plot
    ax = sns.countplot(x='Impact_Category', data=df, palette='viridis', order=labels)
    
    # Formatting
    plt.title(f'Comparison of {category_name}: EM-DAT vs Wikimpacts', fontsize=15)
    plt.xlabel('Impact Difference Category', fontsize=12)
    plt.ylabel('Count of Events', fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Add count labels on top of bars
    for p in ax.patches:
        ax.annotate(f'{int(p.get_height())}', 
                    (p.get_x() + p.get_width() / 2., p.get_height()), 
                    ha = 'center', va = 'center', 
                    xytext = (0, 9), 
                    textcoords = 'offset points')

    # Save the plot
    filename = f"EM_DAT_Wikimpacts_{category_name}_comparison.png"
    plt.savefig(filename, dpi=300)
    print(f"Plot saved: {filename}")
    plt.show() # Optional: Show plot in IDE
    
    return df

# --- Execute for each Category ---

print("Processing Deaths...")
match_deaths_processed = process_and_plot_impacts(
    match_deaths, 
    category_name="Deaths", 
    emdat_col="Total Deaths"
)

print("Processing Injuries...")
match_injuries_processed = process_and_plot_impacts(
    match_injuries, 
    category_name="Injuries", 
    emdat_col="No. Injured"
)

print("Processing Damage...")
match_damage_processed = process_and_plot_impacts(
    match_damage, 
    category_name="Damage", 
    emdat_col="Total Damage, Adjusted ('000 US$)"
)