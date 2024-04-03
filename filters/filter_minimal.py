import pandas as pd

def filter_minimal(df,args):
    """
    This function collects all functions here
    """
    df = df.pipe(remove_spaces).pipe(fix_na)
    return df



def remove_spaces(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    return df.map(lambda x: x.strip() if isinstance(x, str) else x)


def fix_na(df):
    """
    Fixes NAs across columns.
    -1 can be a valid entry for the actual result of the lab analysis so we need to skip that column
    """

    # replace the basic one in the lab value
    arvo_col = "tutkimustulosarvo"
    rej_lines = ['Puuttuu','""',"TYHJÄ","_","NULL"]
    df[arvo_col] = df[arvo_col].replace(rej_lines,"NA")
    #apply the basic one to all other columns
    other_cols = df.columns.difference([arvo_col])
    rej_lines = ['Puuttuu','""',"TYHJÄ","_","NULL","-1"]
    df[other_cols] = df[other_cols].replace(rej_lines,"NA")
    return df
