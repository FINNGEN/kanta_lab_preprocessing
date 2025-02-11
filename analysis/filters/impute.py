import pandas as pd
import re

def impute_all(df,args):

    df = (
        df
        .pipe(impute_measurement,args)
    )
    return df


def impute_measurement(df,args):
    """
    Creates new imputed::MEASURMENT_VALUE column with data extracted from MEASUREMENT_FREE_TEXT column
    """

    col_name = "imputed::MEASUREMENT_VALUE"
    mes_col = "harmonization_omop::MEASUREMENT_VALUE"
    ft_col = "MEASUREMENT_FREE_TEXT"
    unit_col= 'harmonization_omop::MEASUREMENT_UNIT'
    # get mask where mes is na
    mask = df[mes_col].isna() & ~df[ft_col].isna()
    df.loc[:,col_name] = df.loc[:,ft_col].where(mask,df.loc[:,mes_col]).astype(str).str.lower().str.strip().str.replace(r'\s', '', regex=True).fillna("NA") # this removes ALL spaces
    # create series with target unit for omop values and remove that from the free text column
    target_unit = df["harmonization_omop::OMOP_ID"].astype(int).map(args.omop_unit_table)
    df.loc[:,col_name] = df.apply(lambda row: row[col_name].replace(
    str(target_unit[row.name]), ''  # Replace target unit with empty string
), axis=1)

    # regex replacements
    all_replacements =  [(rf'\b{re.escape(word)}\b', '') for word in args.config['free_text_result_strings']] + args.config['free_text_measurement_replacements']
    for rep in all_replacements:
        df.loc[:,col_name] = df.loc[:,col_name].replace(rep[0],rep[1],regex=True)
    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')

    return df
