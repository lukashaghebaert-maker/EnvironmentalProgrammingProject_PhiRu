
import sqlite3
import pandas as pd

#1-------
db_path = "impactdb.v1.0.2.dg_filled.db"  # <-- your path
conn = sqlite3.connect(db_path)


#2------- 
# List all tables
tables = pd.read_sql(
    "SELECT name FROM sqlite_master WHERE type='table';", conn
)
print(tables)

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




#4----------
#group_cols = ["Event_ID", "Administrative_Area_GID"] + date_cols
#grouped_deaths = L3_Deaths_TC.groupby(group_cols)
#L3_Deaths_aggregated = grouped_deaths["Num_Max"].sum()
#L3_Deaths_aggregated = L3_Deaths_aggregated.reset_index()




#5------
