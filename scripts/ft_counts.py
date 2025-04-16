import argparse
import duckdb as ddb
import pandas as pd
import numpy as np
from collections import defaultdict as dd

pd.set_option('future.no_silent_downcasting', True)

def process_omop_concept(con, parquet_file, omop_id, limit=None):
    """Process a single OMOP concept ID and return statistics."""
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
    SELECT
        OMOP_CONCEPT_ID,
        MEASUREMENT_VALUE_EXTRACTED,
        IS_VALUE_EXTRACTED,
        TEST_OUTCOME_MERGED,
        TEST_OUTCOME_SOURCE,
        OUTCOME_TEXT_EXTRACTED
    FROM '{parquet_file}'
    WHERE OMOP_CONCEPT_ID = {omop_id} {limit_clause}
    """
    
    df = con.execute(query).fetchdf()
    N = len(df)
    
    if N == 0:
        return None
    
    # Create merged outcome with priority: VALUE > ORIGINAL > EXTRACTED
    df['ORIGINAL_OUTCOME'] = df['TEST_OUTCOME_MERGED'].where(df['TEST_OUTCOME_SOURCE'] == 'O')
    df['OUTCOME_MERGED_SOURCE'] = np.nan
    df['OUTCOME_MERGED_SOURCE'] = (
        df['OUTCOME_MERGED_SOURCE']
        .fillna(df['MEASUREMENT_VALUE_EXTRACTED'].notna().map({True: 'VALUE', False: np.nan}))
        .fillna(df['ORIGINAL_OUTCOME'].notna().map({True: "ORIGINAL", False: np.nan}))
        .fillna(df['OUTCOME_TEXT_EXTRACTED'].notna().map({True: "EXTRACTED", False: np.nan}))
    )
    
    # Calculate statistics
    extracted_dict = df.loc[df.OUTCOME_MERGED_SOURCE == 'VALUE', 'IS_VALUE_EXTRACTED'].value_counts().to_dict()
    counts = (df.OUTCOME_MERGED_SOURCE.fillna("NA").value_counts() * 100 / N).round(2).to_dict()
    
    # Calculate extracted value percentage if available
    if 1 in extracted_dict or '1' in extracted_dict:
        extracted_key_1 = 1 if 1 in extracted_dict else '1'
        extracted_key_0 = 0 if 0 in extracted_dict else '0'
        ev_pct = 100*round(extracted_dict.get(extracted_key_1, 0) / 
                      (extracted_dict.get(extracted_key_1, 0) + extracted_dict.get(extracted_key_0, 0)), 2)
    else:
        ev_pct = 0
        
    counts['EV%'] = ev_pct
    counts['N'] = N
    counts = dd(float, counts)
    
    OUT_COLS = ["N", 'VALUE', 'EV%', 'ORIGINAL', 'EXTRACTED', "NA"]
    outline = '\t'.join(list(map(str, [omop_id] + [counts[elem] for elem in OUT_COLS])))
    
    return outline

def get_all_omop_concepts(con, parquet_file):
    """Get all unique OMOP concept IDs from the parquet file."""
    query = f"""
    SELECT DISTINCT OMOP_CONCEPT_ID 
    FROM '{parquet_file}'
    WHERE OMOP_CONCEPT_ID IS NOT NULL
    ORDER BY OMOP_CONCEPT_ID
    """
    result = con.execute(query).fetchdf()
    return result['OMOP_CONCEPT_ID'].tolist()

def main():
    parser = argparse.ArgumentParser(description='Process OMOP concepts in a parquet file')
    parser.add_argument('--parquet_file', help='Path to the parquet file',default='/mnt/disks/data/kanta/ft/release/kanta_analysis_ft_outcome.parquet')
    parser.add_argument('--output_file', help='Path to the output file',default='/mnt/disks/data/kanta/ft/ft_outcome_summary.txt')
    parser.add_argument('--limit', type=int, help='Optional limit for number of rows per OMOP concept')
    parser.add_argument('--omop_id', help='Optional specific OMOP concept ID to process')
    parser.add_argument('--test', help='Test mode',action='store_true')
    
    args = parser.parse_args()
    
    # Connect to DuckDB
    con = ddb.connect(database=':memory:')
    
    # Header for output file
    header = "OMOP_CONCEPT_ID\tN\tVALUE\tEV%\tORIGINAL\tEXTRACTED\tNA"
    
    with open(args.output_file, 'wt') as f:
        f.write(header + '\n')
        
        # Process single OMOP concept if specified
        if args.omop_id:
            result = process_omop_concept(con, args.parquet_file, args.omop_id, args.limit)
            if result:
                f.write(result + '\n')
        else:
            # Process all OMOP concepts
            omop_ids = get_all_omop_concepts(con, args.parquet_file)
            if args.test:omop_ids = omop_ids[:10]
            print(f"Found {len(omop_ids)} unique OMOP concept IDs to process")
            for i, omop_id in enumerate(omop_ids):
                if i % 10 == 0:
                    print(f"Processing {i}/{len(omop_ids)} concepts...")
                
                result = process_omop_concept(con, args.parquet_file, omop_id, args.limit)
                if result:
                    f.write(result + '\n')
    
    print(f"Analysis complete. Results written to {args.output_file}")

if __name__ == "__main__":
    main()
