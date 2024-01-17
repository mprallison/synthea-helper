def gspread_client():

    """return client object for gspread oauth
    looks for token object in current directory, if not exist let user login
    then save token to current directory
    
    return error if credentials.json not found in current directory
    """

    import gspread

    return  gspread.oauth(credentials_filename="credentials.json",authorized_user_filename="token.json")

def read_sheet(DOCUMENT_ID, sheet_name, gc):
    
    """return google sheet content as dataframe and worksheet object to write changes
    if sheet name not found, create blank sheet with that name and return empty df and ws

    Args:
            DOCUMENT_ID: idenitifying id found in gdoc url https://docs.google.com/spreadsheets/d/!!IN HERE!!/edit
            sheet_name: tab/sheet name in gsheet file
            gc: gspread client object generated with gspread_client()
    """
            
    import pandas as pd
    
    sh = gc.open_by_key(DOCUMENT_ID)
    
    try:
        worksheet = sh.worksheet(sheet_name)
    except:
        sh.add_worksheet(title=sheet_name, rows=1, cols=1)
        worksheet = sh.worksheet(sheet_name)
    
    print(fr"serving sheet: {sh.title}, {sheet_name}")
    
    df = pd.DataFrame(worksheet.get_all_records())
    
    return df.astype(str), worksheet

def write_sheet(df, worksheet):
    
    """write dataframe to google sheet
    
    Args:
            df: pandas dataframe
            worksheet: gspread object generated with read_sheet()
    """

    #need to update syntax in gspread 6.0
    import warnings
    warnings.filterwarnings("ignore", category=UserWarning) 

    #write all df values as string
    df = df.astype(str)
    
    def sheet_range(df):
    
        """return A1, B1, ..., range for dataframe column length"""
        
        def iter_all_strings():
    
            from string import ascii_lowercase
            import itertools
            
            for size in itertools.count(1):
                for s in itertools.product(ascii_lowercase, repeat=size):
                    yield "".join(s)
    
        letters = []
        count = 1
        for s in iter_all_strings():
    
            if count == df.shape[1]:
                break
            else:
                count += 1
    
        return f"A1:{s.upper()}{len(df)+1}"
  
    worksheet.clear()

    #depreciated change syntax
    #worksheet.update(value = [df.columns.values.tolist()] + df.values.tolist(), range = sheet_range(df))
    worksheet.update(sheet_range(df), [df.columns.values.tolist()] + df.values.tolist())

    return