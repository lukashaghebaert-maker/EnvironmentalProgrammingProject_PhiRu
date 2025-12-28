import sqlite3
import pandas as pd
import data_processing_functions as dpf
import os
import geopandas as gpd
import matplotlib.pyplot as plt

# Task 1------- Connecting to Data base using dynamic paths
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
db_path = os.path.join(project_root, 'Data', 'impactdb.v1.0.2.dg_filled.db')  # <-- database
conn = sqlite3.connect(db_path)

# Task 2-------  Reading data from tables and selecting those with total in title
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


# Task 3----- Filtering for Tropical Storm/Cyclone events

filter_criteria = L1["Main_Event"] == "Tropical Storm/Cyclone"
L1_TC = L1[filter_criteria].copy() #Copy is very imprtant to ensure original data isn't altered

tc_events = L1_TC["Event_ID"].unique()

L3_Deaths_TC = dpf.filter_L3_tc(L3_Deaths, tc_events)
L3_Injuries_TC = dpf.filter_L3_tc(L3_Injuries, tc_events)
L3_Damage_TC  = dpf.filter_L3_tc(L3_Damage, tc_events)
date_cols = [
    "Start_Date_Year", "Start_Date_Month", "Start_Date_Day",
    "End_Date_Year", "End_Date_Month", "End_Date_Day"]

L1_TC_dates = L1_TC[["Event_ID"] + date_cols].drop_duplicates()

L3_Deaths_TC = dpf.fill_dates(L3_Deaths_TC, L1_TC_dates, date_cols)
L3_Injuries_TC = dpf.fill_dates(L3_Injuries_TC, L1_TC_dates, date_cols)
L3_Damage_TC = dpf.fill_dates(L3_Damage_TC, L1_TC_dates, date_cols)

# Task 4---------- Filtering by year
        
year_to_filter = 1900
L3_Deaths_TC_1900 = dpf.filter_year(L3_Deaths_TC, year_to_filter)
L3_Injuries_TC_1900 = dpf.filter_year(L3_Injuries_TC, year_to_filter)
L3_Damage_TC_1900 = dpf.filter_year(L3_Damage_TC, year_to_filter)

# Task 5---------- Aggregate by Administrative Area

# Execute the process on each of our filtered dataframes:
L3_Deaths_TC_1900_aggregated = dpf.aggregate_by_eventID(dpf.clean_dataframe(L3_Deaths_TC_1900))
L3_Damage_TC_1900_aggregated = dpf.aggregate_by_eventID(dpf.clean_dataframe(L3_Damage_TC_1900))
L3_Injuries_Damage_TC_1900_aggregated = dpf.aggregate_by_eventID(dpf.clean_dataframe(L3_Injuries_TC_1900))

# Task 6------- 

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

L2_Deaths = dpf.clean_dataframe(L2.get("Deaths"))
L2_Injuries = dpf.clean_dataframe(L2.get("Injuries"))
L2_Damage = dpf.clean_dataframe(L2.get("Damage"))

#---- Using  Event_ID from ‘L3_*_1900_aggregated’ filter the events from ’ L2_*`, name as ‘L2_*_filter`
#Extract Event ID from L3
L3_deaths_ids = L3_Deaths_TC_1900_aggregated["Event_ID"].unique()
L3_injuries_ids = L3_Injuries_Damage_TC_1900_aggregated["Event_ID"].unique()
L3_damage_ids = L3_Damage_TC_1900_aggregated["Event_ID"].unique()

#Filter L2 using these Event_ID's from L3
L2_Deaths_filter = L2_Deaths[L2_Deaths["Event_ID"].isin(L3_deaths_ids)].copy()
L2_Injuries_filter = L2_Injuries[L2_Injuries["Event_ID"].isin(L3_injuries_ids)].copy()
L2_Damage_filter = L2_Damage[L2_Damage["Event_ID"].isin(L3_damage_ids)].copy()

#----computing the relative difference between each impact category 
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

# --- Keep only the required columns
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
impact_columns = ["Num_Min", "Num_Max", "Num_Approx"]

for col in impact_columns:
    merged_deaths[f"{col}_rel_diff"] = dpf.rel_diff_between_data_levels(merged_deaths, col)
    merged_injuries[f"{col}_rel_diff"] = dpf.rel_diff_between_data_levels(merged_injuries, col)
    merged_damage[f"{col}_rel_diff"] = dpf.rel_diff_between_data_levels(merged_damage, col)

# Compute average relative difference per category
avg_rel_diff_deaths = merged_deaths[[c for c in merged_deaths.columns if "rel_diff" in c]].mean()
avg_rel_diff_injuries = merged_injuries[[c for c in merged_injuries.columns if "rel_diff" in c]].mean()
avg_rel_diff_damage = merged_damage[[c for c in merged_damage.columns if "rel_diff" in c]].mean()

# Task 7 -------

# Load EM-DAT Excel file
emdat = pd.read_excel(os.path.join(project_root, 'Data', 'EMDAT.xlsx'),sheet_name="EM-DAT Data")

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
    how="inner")

match_injuries = L2_Injuries_match.merge(
    emdat,
    left_on=["Administrative_Area_GID", "Start_Date_Year", "Start_Date_Month", "End_Date_Year", "End_Date_Month"],
    right_on=["ISO", "Start Year", "Start Month", "End Year", "End Month"],
    how="inner")

match_damage = L2_Damage_match.merge(
    emdat,
    left_on=["Administrative_Area_GID", "Start_Date_Year", "Start_Date_Month", "End_Date_Year", "End_Date_Month"],
    right_on=["ISO", "Start Year", "Start Month", "End Year", "End Month"],
    how="inner")

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
    "Total Damage, Adjusted ('000 US$)"]

match_deaths = match_deaths[cols_final].copy()
match_injuries = match_injuries[cols_final].copy()
match_damage = match_damage[cols_final].copy()

EM_DAT_Wikimapcts_Matched = pd.concat(
    [match_deaths, match_injuries, match_damage],
    ignore_index=True)


# --- Execute for each Category ---

print("Processing Deaths...")
match_deaths_processed = dpf.process_and_plot_impacts(
    match_deaths, 
    category_name="Deaths", 
    emdat_col="Total Deaths"
)

print("Processing Injuries...")
match_injuries_processed = dpf.process_and_plot_impacts(
    match_injuries, 
    category_name="Injuries", 
    emdat_col="No. Injured"
)

print("Processing Damage...")
match_damage_processed = dpf.process_and_plot_impacts(
    match_damage, 
    category_name="Damage", 
    emdat_col="Total Damage, Adjusted ('000 US$)"
)

# Task 8 -----------
#Spatial Analysis
spatial_results = dpf.process_and_plot_spatial_differences(
    emdat,
    L2_Deaths_filter,
    L2_Injuries_filter,
    L2_Damage_filter
)
