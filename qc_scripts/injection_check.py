import pandas as pd
import numpy as np
import os
import argparse
import subprocess
import logging
from io import StringIO

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Get the absolute path to the directory where this script lives
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Navigate: qc_scripts/.. -> finngen_qc/data/
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "finngen_qc", "data")

# Define file paths relatively
DEFAULT_RULES = os.path.join(DATA_DIR, "fix_unit_based_in_abbreviation.tsv")
DEFAULT_USAGI = os.path.join(DATA_DIR, "LABfi_ALL.usagi.csv")


def run_ch_query(query):
    # Reverting to the version you confirmed worked
    process = subprocess.run(['clickhouse', '--query', query, '--format', 'TSVWithNames'], 
                         capture_output=True, text=True, check=True)
    return pd.read_csv(StringIO(process.stdout), sep='\t', keep_default_na=False)

def clean_str(s):
    """Standardizes all null-like values to 'na' for join keys."""
    s = str(s).lower().strip()
    if s in ['nan', 'none', 'null', '', 'na', r'\\n', r'\\v']:
        return 'na'
    return s

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="munged.parquet")
    parser.add_argument("--rules", default=DEFAULT_RULES)
    parser.add_argument("--usagi", default=DEFAULT_USAGI)
    parser.add_argument("-o", "--output", default="audit.txt")
    args = parser.parse_args()

    # 1. LOAD RULES
    df_rules = pd.read_csv(args.rules, sep='\t', keep_default_na=False)
    df_rules['source_unit_clean'] = df_rules['source_unit_clean'].replace(['', 'nan'], 'NA')
    df_rules['source_unit_clean_fix'] = df_rules['source_unit_clean_fix'].replace(['', 'nan'], 'NA')

    df_rules['abbr_key'] = df_rules['TEST_NAME_ABBREVIATION'].apply(clean_str)
    df_rules['src_key'] = df_rules['source_unit_clean'].apply(clean_str)
    df_rules['trg_key'] = df_rules['source_unit_clean_fix'].apply(clean_str)
    df_rules = df_rules.drop_duplicates(subset=['abbr_key', 'src_key', 'trg_key'])

    # 2. LOAD USAGI
    df_usagi = pd.read_csv(args.usagi, keep_default_na=False)
    # Extracting conceptId for the OMOP ID column
    df_usagi_clean = pd.DataFrame({
        'abbr_key': df_usagi['ADD_INFO:testNameAbbreviation'].apply(clean_str),
        'trg_key': df_usagi['ADD_INFO:measurementUnit'].apply(clean_str),
        'LINKED_OMOP_IDS': df_usagi['conceptId'],
        'mappingStatus': df_usagi['mappingStatus'].str.upper().str.strip().str.replace(' ', '_')
    })
    df_usagi_clean['rank'] = df_usagi_clean['mappingStatus'].map({'APPROVED': 0}).fillna(1)
    df_usagi_clean = df_usagi_clean.sort_values(['abbr_key', 'trg_key', 'rank'])
    df_usagi_clean = df_usagi_clean.drop_duplicates(subset=['abbr_key', 'trg_key'])

    # 3. DATA SWEEP (Using your confirmed working query format)
    sweep_query = (
        f"SELECT lower(trim(TEST_NAME)) as abbr_raw, "
        f"ifNull(MEASUREMENT_UNIT_PRE_FIX, 'NA') as unit_before, "
        f"ifNull(MEASUREMENT_UNIT_CLEANED, 'NA') as unit_after, "
        f"count() as row_count "
        f"FROM file('{args.input}', 'Parquet') "
        f"GROUP BY abbr_raw, unit_before, unit_after"
    )
    
    logger.info("Sweeping Parquet data...")
    df_data = run_ch_query(sweep_query)
    df_data['unit_before'] = df_data['unit_before'].replace(['', r'\\N'], 'NA')
    df_data['unit_after'] = df_data['unit_after'].replace(['', r'\\N'], 'NA')
    df_data['abbr_key'] = df_data['abbr_raw'].apply(clean_str)
    df_data['src_key'] = df_data['unit_before'].apply(clean_str)
    df_data['trg_key'] = df_data['unit_after'].apply(clean_str)

    # 4. JOINS
    merged = pd.merge(df_rules, df_data[['abbr_key', 'src_key', 'trg_key', 'row_count']], 
                      on=['abbr_key', 'src_key', 'trg_key'], how='left')
    merged = pd.merge(merged, df_usagi_clean[['abbr_key', 'trg_key', 'LINKED_OMOP_IDS', 'mappingStatus']], 
                      on=['abbr_key', 'trg_key'], how='left')

    # 5. ASSIGN FINAL STATUS
    def determine_status(row):
        if pd.isna(row['row_count']) or row['row_count'] == 0:
            return "NO_INJECTION_HAPPENED"
        return "APPROVED" if str(row['mappingStatus']).strip().upper() == "APPROVED" else "NOT_APPROVED"

    merged['row_count'] = merged['row_count'].fillna(0).astype(int)
    merged['FINAL_STATUS'] = merged.apply(determine_status, axis=1)

    # 6. OUTPUT (Renamed and Ordered per your request)
    output_df = pd.DataFrame({
        'TEST_NAME_ABBREVIATION': merged['TEST_NAME_ABBREVIATION'],
        'source_unit_clean': merged['source_unit_clean'],
        'source_unit_clean_fix': merged['source_unit_clean_fix'],
        'LINKED_OMOP_IDS': merged['LINKED_OMOP_IDS'],
        'MATCH_STATUS': merged['mappingStatus'].fillna('MISSING'),
        'FINAL_STATUS': merged['FINAL_STATUS'],
        'row_count': merged['row_count']
    })
    
    # Final cleanup
    output_df = output_df.replace(['nan', 'None', '', None], 'NA')
    output_df['MATCH_STATUS'] = output_df['MATCH_STATUS'].str.replace(' ', '_')
    
    output_df.to_csv(args.output, sep='\t', index=False)
    logger.info(f"Audit complete. Results saved to {args.output}")

if __name__ == "__main__":
    main()
