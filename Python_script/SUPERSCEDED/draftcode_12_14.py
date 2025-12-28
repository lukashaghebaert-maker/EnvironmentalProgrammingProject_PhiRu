def clean_dataframe(df):
    df_clean = df.copy() # Create a copy of the input data to work on safely
    
    # 1. IDENTIFY THE COLUMN
    # Check which one is in the data frame Area or Areas
    if 'Administrative_Area_GID' in df_clean.columns:
        target_col = 'Administrative_Area_GID'
        print("TARGET COLUMN:", target_col)
        print("FIRST VALUES:\n", df_clean[target_col].head())
        print("COLUMN DTYPE:", df_clean[target_col].dtype)
        print("PYTHON TYPE OF VALUE:", type(df_clean[target_col].iloc[2]))
        #pd.set_option('display.max_columns', None)
        #print(df_clean.head(2))
        #print("AREA TYPE:", type(df_clean[target_col][2]))
        
    elif 'Administrative_Areas_GID' in df_clean.columns:
        target_col = 'Administrative_Areas_GID'
    
        import ast
    
        # Step 1: Convert "[['USA']]" → [['USA']]
        df_clean[target_col] = df_clean[target_col].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )
    
        # Step 2: Flatten [['USA']] → ['USA']
        df_clean[target_col] = df_clean[target_col].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x
        )
    
        # Step 3: Convert ['USA'] → "['USA']" (string)
        df_clean[target_col] = df_clean[target_col].apply(
            lambda x: str([x]) if isinstance(x, str) else x
        )
    
        print("TARGET COLUMN:", target_col)
        print("FIRST VALUES:\n", df_clean[target_col].head())
        print("COLUMN DTYPE:", df_clean[target_col].dtype)
        print("PYTHON TYPE OF VALUE:", type(df_clean[target_col].iloc[2]))
        
        
        
   #convert the string [[]] to a list
        #df_clean[target_col] = df_clean[target_col].apply(safe_ast_eval)
   #makes the [[]] into []
   #Process the target column such that it takes the first element of the 2D list: 
        #df_clean[target_col] = df_clean[target_col].apply(
            #lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
   #df_clean[target_col] = df_clean[target_col].apply(
        #    lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)

       # pd.set_option('display.max_columns', None)
        #print(df_clean.head(2))
       # print("AREAS TYPE:", type(df_clean[target_col][2]))

        
        
    else:
        # Fallback if neither exists
        print("Error: Neither 'Area' nor 'Areas' GID column found.")
        return df_clean
    
    -------------------
    
    def get_single_valid_gid(gid_entry): # Checks every single GID at a time
            
        # Handle no data cells and returns it as NaNs
        if pd.isna(gid_entry): #or len(gid_entry) == 0: 
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