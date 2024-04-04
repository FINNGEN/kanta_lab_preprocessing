import pandas as pd

def filter_minimal(df,args):
    """
    This function collects all functions here
    """
    df = df.pipe(remove_spaces).pipe(fix_na,args).pipe(filter_hetu,args).pipe(filter_measurement_status,args).pipe(lab_name_map,args).pipe(get_lab_abbrv,args)
    return df



def lab_unit_filter(df,args):
    '''
    This fixes problematic chars in lab_unit value
    lab_unit = remove_chars(lab_unit, ' ');
    lab_unit = remove_chars(lab_unit, '_');
    lab_unit = remove_chars(lab_unit, ',');
    lab_unit = remove_chars(lab_unit, '.');
    lab_unit = remove_chars(lab_unit, '-');
    lab_unit = remove_chars(lab_unit, ')');
    lab_unit = remove_chars(lab_unit, '(');
    lab_unit = remove_chars(lab_unit, '{');
    lab_unit = remove_chars(lab_unit, '}');
    lab_unit = remove_chars(lab_unit, '\'');
    lab_unit = remove_chars(lab_unit, '?');
    lab_unit = remove_chars(lab_unit, '!');
    '''
    return df

def get_lab_abbrv(df,args):
    """
    if(lab_id_source == "0") {
        lab_name = to_lower(lab_name);
        lab_name = lab_name;
        lab_abbrv = lab_name;
    } else {
        // Mapping lab IDs to abbreviations
        if(thl_abbrv_map.find(lab_id) != thl_abbrv_map.end()) {
            lab_abbrv = thl_abbrv_map[lab_id];
        } else {
            lab_abbrv = "NA";
        }  
    }  
    return(lab_abbrv);

    Looks like it assigns LAB_ABBREVIATION, keeping the name if source is local or mapping it if THL
    N.B.LAB ABBREVIATION is already read on reading from paikallinentutkimusnimike (from config) so no need to create it, just update
    """
    df['LAB_ABBREVIATION'] =df['LAB_ABBREVIATION'].str.lower()
    # now we need to update THL abbreviation from map

    # update values based on mapping
    dd=args.config['thl_lab_map']
    mask = df.LAB_ID_SOURCE != "0"
    values = df.loc[mask,'LAB_ID']
    df.loc[mask,'LAB_ABBREVIATION'] = [dd[elem] for elem in values]

    err_mask= df['LAB_ABBREVIATION'] =='MISSING'
    df.loc[err_mask,'ERR'] = 'LAB_ABBREVIATION_missing'
    df.loc[err_mask,'ERR_VALUE'] = df.loc[err_mask,'LAB_ID']
    return df

def lab_name_map(df,args):
    """
    Assings LAB_ID and LAB_ID_SOURCE to the DF
    paikallinentutkimusnimikeid  --> std::string local_lab_abbrv = remove_chars(line_vec[31], ' ');
    paikallinentutkimusnimike  --> std::string local_lab_id = remove_chars(line_vec[32], ' ');
    laboratoriotutkimusoid --> std::string thl_lab_id = remove_chars(line_vec[0], ' ');

    """
    # first assign local/finland wide THL id
    # initiate value as being valid and the THL id as being the laboratoriotutkimusoid
    df =df.assign(LAB_ID_SOURCE='1')
    df['LAB_ID'] = df['laboratoriotutkimusoid']

    # if id is local
    mask = df['laboratoriotutkimusoid'] == "NA"
    df.loc[mask,'LAB_ID_SOURCE'] = "0" # --> FLAG AS LOCAL
    df.loc[mask,"LAB_ID"] = df.loc[mask,'paikallinentutkimusnimikeid'] #--> get values from local id
    return df
    
def filter_measurement_status(df,args):
    """
    Here we deal with measurement statuses that are not final
    """
    col,problematic_values=args.config['problematic_status']
    err_mask = df[col].isin(problematic_values)
    df.loc[err_mask,'ERR'] = 'measurement_status'
    df.loc[err_mask,'ERR_VALUE'] = df.loc[err_mask,col]
    return df
    

def filter_hetu(df,args):
    """
    Filters out if hetu root is incorrect
    """
    err_mask = df['hetu_root'] != args.config['hetu_kw']
    df.loc[err_mask,'ERR'] = 'hetu_root'
    df.loc[err_mask,'ERR_VALUE'] = df.loc[err_mask,'hetu_root']
    return df
    

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
