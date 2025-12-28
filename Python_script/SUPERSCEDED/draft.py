import pandas as pd
import numpy as np

def standardize_and_select_single_gid(gid_entry):
    """
    Cleans and standardizes the Administrative Area GID entry based on the rules.
    1. Extracts the 3-letter ISO code (e.g., 'AUS.10' -> 'AUS').
    2. Filters out auxiliary codes (e.g., 'Z03').
    3. Requires exactly ONE valid ISO code; otherwise, returns NaN to be discarded.
    
    Args:
        gid_entry: The content of the 'Administrative_Area_GID' cell 
                   (can be string, list, or NaN).
        
    Returns:
        A single 3-letter string (e.g., 'CHN') or np.nan if the entry should be discarded.
    """
    
    # 1. Handle NaN or empty entries (Rule: [])
    if pd.isna(gid_entry) or gid_entry == [] or gid_entry is None:
        return np.nan
    
    # Ensure the entry is processed as a list of strings
    if not isinstance(gid_entry, list):
        # Case: Single string like 'MYS' or 'AUS.10'
        elements = [str(gid_entry)]
    else:
        # Case: List like ['Z03', 'CHN', 'Z08', 'Z02']
        elements = [str(e) for e in gid_entry if pd.notna(e)]
    
    
    valid_iso_codes = []
    
    for element in elements:
        # Standardize the GID: Take the first three characters and convert to uppercase
        # This handles: 'AUS.10' -> 'AUS'
        cleaned_gid = element[:3].upper()
        
        # Check if the cleaned GID is a valid ISO country code (Rule: exclude 'Z' codes)
        # It must be exactly 3 letters and not start with 'Z' (which are auxiliary codes).
        if len(cleaned_gid) == 3 and cleaned_gid.isalpha() and not cleaned_gid.startswith('Z'):
             valid_iso_codes.append(cleaned_gid)
             
    # 2. Enforce the "Single Valid GID" Rule
    # If there is exactly one valid ISO code, return it (Rule: ['MYS'], ['Z03', 'CHN', ...])
    if len(valid_iso_codes) == 1:
        return valid_iso_codes[0]
    
    # If there are zero or multiple valid codes, disregard the data (Rule: multiple countries, [])
    # This covers the multiple country list ['AIA', 'ATG', ...]
    return np.nan

# --- Applying the Cleaning and Filtering ---

def clean_and_filter_dataframes(df_1900):
    """Applies the cleaning function and then drops the rows that returned NaN."""
    df = df_1900.copy()
    
    # Apply the complex cleaning function to the GID column
    df['Administrative_Area_GID'] = df['Administrative_Area_GID'].apply(standardize_and_select_single_gid)
    
    # Drop rows where the cleaning function returned NaN (discarded data)
    df_cleaned = df.dropna(subset=['Administrative_Area_GID']).copy()
    
    return df_cleaned




