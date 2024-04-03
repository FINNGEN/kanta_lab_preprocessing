import pandas as pd

def filter_minimal(df,args):
    """
    This function collects all functions here
    """
    df = df.pipe(remove_spaces).pipe(fix_na).pipe(filter_hetu,args).pipe(filter_measurement_status,args)
    return df



def filter_measurement_status(df,args):
    col='tutkimusvastauksentila'
    problematic_values = ['K','W','X','I','D','P'] #these values are known to be unreliable
    mask = df['tutkimusvastauksentila'].isin(problematic_values)
    err_df = df[~mask]
    err_df = err_df.assign(err='measurement_status')
    err_df.to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")
    return df[mask]
    

    

def filter_hetu(df,args):
    """
    Filters out if hetu root is incorrect
    """
    mask = df['hetu_root'] == '1.2.246.21'
    err_df = df[~mask]
    err_df = err_df.assign(err='hetu_root')
    err_df.to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")
    return df[mask]
    

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
