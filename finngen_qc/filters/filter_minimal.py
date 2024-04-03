import pandas as pd

def filter_minimal(df,args):
    """
    This function collects all functions here
    """
    df = df.pipe(remove_spaces).pipe(fix_na,args).pipe(filter_hetu,args).pipe(filter_measurement_status,args)
    return df



def filter_measurement_status(df,args):
    """
    Here we deal with measurement statuses that are not final
    """
    col,problematic_values=args.config['problematic_status']
    mask = df[col].isin(problematic_values)
    err_df = df[~mask]
    err_df = err_df.assign(err='measurement_status')
    err_df.to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")
    return df[mask]
    
def filter_hetu(df,args):
    """
    Filters out if hetu root is incorrect
    """
    mask = df['hetu_root'] == args.config['hetu_kw']
    err_df = df[~mask]
    err_df = err_df.assign(err='hetu_root')
    err_df.to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")
    return df[mask]
    

def remove_spaces(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    for col in df.columns:
        df[col] = df[col].str.strip()
    return df


def fix_na(df,args):
    """
    Fixes NAs across columns.
    -1 can be a valid entry for the actual result of the lab analysis so we need to skip that column
    """

    # get special exclusion values dictionary
    exception_columns = set(args.config['NA_map'].keys())
    for col in exception_columns:
        df[col] = df[col].replace(args.config['NA_map'][col],"NA")
    #apply the basic one to all other columns
    other_cols = df.columns.difference(exception_columns)
    df[other_cols] = df[other_cols].replace(args.config['NA_kws'],"NA")
    return df
