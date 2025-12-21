# -*- coding: utf-8 -*-
"""
Created on Thu Dec 18 18:33:01 2025

@author: lukas
"""
import numpy as np
import pandas as pd
import ast # This library turns string "[...]" into list [...]
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ------------ USED IN TASK 3 ------------ 
def filter_L3_tc(df, tc_events):
    """
    Filter a DataFrame to include only level-3 tropical cyclone events.

    The function selects rows whose 'Event_ID' is present in the provided
    list of tropical cyclone event identifiers.

    Args:
        df (pandas.DataFrame): Input DataFrame containing an 'Event_ID' column.
        tc_events (list or set): Collection of Event_IDs corresponding to
            level-3 tropical cyclone events.

    Returns:
        pandas.DataFrame: Filtered DataFrame containing only rows associated
        with level-3 tropical cyclone events.

    Raises:
        KeyError: If the 'Event_ID' column is not present in the DataFrame.
    """
    return df[df["Event_ID"].isin(tc_events)].copy()

def fill_dates(L3_tc, L1_TC_dates, date_cols):
    """
    Fill missing date values in level-3 event data using level-1 reference dates.

    The function merges level-3 tropical cyclone data with level-1 reference
    dates on 'Event_ID'. For each specified date column, missing values in the
    level-3 data are filled using the corresponding level-1 values.

    Args:
        L3_tc (pandas.DataFrame): Level-3 event data containing date columns
            and an 'Event_ID' column.
        L1_TC_dates (pandas.DataFrame): Level-1 reference data providing
            fallback date values, keyed by 'Event_ID'.
        date_cols (list of str): List of date column names to be filled.

    Returns:
        pandas.DataFrame: DataFrame with missing level-3 date values filled
        using level-1 reference dates.

    Raises:
        KeyError: If required columns are missing from the input DataFrames.
    """
    merged = L3_tc.merge(L1_TC_dates, on="Event_ID", how="left", suffixes=("", "_L1"))
    for col in date_cols:
        merged[col] = merged[col].fillna(merged[f"{col}_L1"])
        merged.drop(columns=[f"{col}_L1"], inplace=True)
    return merged

#------------ USED IN TASK 4 ------------ 
def filter_year(df, year):
    
    """
    Filter a DataFrame to include only rows after a specified year.

    The function keeps rows where the 'Start_Date_Year' value is strictly
    greater than the provided year. If the year is not an integer, the
    function prints an error message and returns nothing.

    Args:
        df (pandas.DataFrame): DataFrame containing a 'Start_Date_Year' column.
        year (int): Year threshold used to filter the data.

    Returns:
        pandas.DataFrame: Filtered DataFrame containing only rows with
        'Start_Date_Year' values greater than the specified year.

    Raises:
        TypeError: If the year argument is not of type int.
    """
    
    if type(year) == int:
        year_mask = df["Start_Date_Year"]>year
        return df[year_mask].copy()
    else:
        print ("Year must be an int data type")
        
# ------------ USED IN TASK 5 ------------ 
def get_single_valid_gid(gid_entry):#Checks every single GID at a time
    """
    Extract a single valid 3-letter country GID from a raw entry.

    The function cleans, standardizes, and validates country identifiers.
    It handles strings, lists, nested lists, and missing values. Only rows
    with exactly one valid 3-letter code are returned; all others result in NaN.

    Args:
        gid_entry (str, list, or None): Raw administrative area identifier.
            Can be a string (e.g., "USA" or "['USA']"), a list (e.g., ['USA']),
            a nested list (e.g., [['USA']]), or None/missing.

    Returns:
        str or numpy.nan: A single cleaned 3-letter country code in uppercase
        (e.g., 'CHN') if valid, otherwise np.nan.

    Raises:
        ValueError: If the input cannot be converted to a list using ast.literal_eval.
    """
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
        #   'AUS.10' -> 'AUS'
        #   'chn'    -> 'CHN'
        code = e.strip()[:3].upper()

        # Validation rule:
        # Must be exactly 3 letters AND contain only letters
        if len(code) == 3 and code.isalpha():
            valid_codes.append(code)

    # 5. Enforce "Single Valid GID"
    #    Only accept rows with EXACTLY ONE valid country code
    if len(valid_codes) == 1:
        return valid_codes[0]  # Return the clean code (ex: 'CHN')
    else:
        return np.nan  # If zero or multiple codes found -> discard row

def clean_dataframe(df):
    
    """
    Clean and standardize the administrative area GID column in a DataFrame.

    The function detects whether the DataFrame contains either
    'Administrative_Area_GID' or 'Administrative_Areas_GID', normalizes the
    column format if necessary, applies a validation and extraction routine
    to obtain a single valid GID per row, and removes rows with invalid or
    missing GID values.

    Args:
        df (pandas.DataFrame): Input DataFrame containing administrative area
            identifier columns and event-level data.

    Returns:
        pandas.DataFrame: Cleaned DataFrame with a standardized administrative
        area GID column and invalid rows removed.

    Raises:
        KeyError: If neither 'Administrative_Area_GID' nor
        'Administrative_Areas_GID' is present in the DataFrame.
    """
    
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

        #Step 1: Convert string "[['USA']]" -> [['USA']]
        df_clean[target_col] = df_clean[target_col].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )

        #Step 2: Flatten [['USA']] -> ['USA']
        df_clean[target_col] = df_clean[target_col].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x
        )

        #Step 3: Convert ['USA'] -> "['USA']" (string)
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
    
    """
    Aggregate event-level data while preventing unintended summation of date fields.

    The function groups the input DataFrame by event ID and administrative area,
    applying column-specific aggregation rules. Numerical impact columns are summed,
    while all other columns retain their first observed value to avoid invalid
    aggregations (e.g., adding years).

    Args:
        df_clean (pandas.DataFrame): Cleaned DataFrame containing event-level data
            with numerical impact columns and metadata.

    Returns:
        pandas.DataFrame: Aggregated DataFrame with one row per event ID and
        administrative area, using fixed aggregation rules.

    """
    
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

# ------------ USED IN TASK 6 ------------ 
def rel_diff_between_data_levels(df, col):
    '''
    Compute the relative difference between level 3 and level 2 data.
    
    The relative difference is defined as (L3 − L2) / L2, subject to a set of
    predefined rules handling zero and missing values. The input DataFrame
    must already contain merged level-2 and level-3 data with suffixes
    `_L2` and `_L3`.
    
    Args:
        df (pandas.DataFrame): DataFrame containing merged level-2 and level-3
            data, aggregated by country and event ID.
        col (str): Base column name to compute the relative difference for.

    Returns:
        numpy.ndarray: Array containing the relative difference values computed
        according to the specified rules.
    
    '''
    L3 = df[f"{col}_L3"]
    L2 = df[f"{col}_L2"]

    # Rule 3: If either is NaN -> 0
    cond_nan = L3.isna() | L2.isna()

    # Rule 2: If both zero -> 0
    cond_both_zero = (L3 == 0) & (L2 == 0)

    # Rule 1: If L3 > 0 and L2 = 0 -> 1
    cond_L3_pos_L2_zero = (L3 > 0) & (L2 == 0)

    # Rule 4: Normal case -> (L3 - L2) / L2
    normal_case = (L3 - L2) / L2

    # Build final vector using np.select
    return np.select(
        [cond_nan, cond_both_zero, cond_L3_pos_L2_zero],
        [0,        0,             1],
        default=normal_case)

# ------------ USED IN TASK 7 ------------
def process_and_plot_impacts(df, category_name, emdat_col):
    """
    Calculate Wikimpacts mean, compare against EM-DAT, categorize differences, and plot results.

    The function performs the following steps:
        1. Computes the row-wise mean of 'Num_Min' and 'Num_Max' as 'Wikimpact_Mean'.
        2. Calculates the relative difference between Wikimpacts and EM-DAT values.
        3. Categorizes the differences into bins representing over- or under-estimation.
        4. Plots a bar chart of the counts per category and saves the figure.

    Args:
        df (pandas.DataFrame): Input DataFrame containing 'Num_Min', 'Num_Max', and EM-DAT columns.
        category_name (str): Name of the category for labeling the plot and file.
        emdat_col (str): Name of the column in df containing EM-DAT values for comparison.

    Returns:
        pandas.DataFrame: DataFrame with added columns 'Wikimpact_Mean', 'Relative_Diff',
        and 'Impact_Category' reflecting the comparison results.

    Raises:
        KeyError: If required columns ('Num_Min', 'Num_Max', or emdat_col) are missing from df.
        ValueError: If EM-DAT values contain zeros leading to invalid relative difference calculations.
    """
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
    ax = sns.countplot(x='Impact_Category', data=df, hue='Impact_Category', legend=False, palette='viridis', order=labels, dodge=False)
    
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
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    filename = f"EM_DAT_Wikimpacts_{category_name}_comparison.png"
    image_path = os.path.join(project_root, 'Images', filename)

    plt.savefig(image_path, dpi=300)
    print(f"Plot saved: {filename}")
    #plt.show() # Optional: Show plot in IDE
    
    return df