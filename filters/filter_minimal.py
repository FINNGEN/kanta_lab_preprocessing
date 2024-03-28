import pandas as pd

def remove_spaces(df):

    # replace these with NA
    rej_lines = ['Puuttuu','""',"TYHJÄ","_","NULL"]
    df = df.replace(rej_lines,"NA")
    # replace with NA ignoring some column value that it's not clear which one in kira's code..
    neg= "-1"
    return df
