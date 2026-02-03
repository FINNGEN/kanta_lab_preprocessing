import pandas as pd
import numpy as np
import argparse
import gc
from scipy.stats import ks_2samp

def audit_harmonization(input_file, parquet_file, output_file, min_count=1000, test_depth=None):
    # 1. Load targets - explicitly using tab for TSV to prevent line-splitting errors
    # keep_default_na=False prevents "NA" units from disappearing
    targets = pd.read_csv(input_file, sep='\t', keep_default_na=False)
    
    # Ensure COUNT is numeric
    targets['COUNT'] = pd.to_numeric(targets['COUNT'], errors='coerce').fillna(0)
    
    # 2. De-duplicate and Filter
    # This prevents the "Input 120 -> Output 255" mismatch
    targets = targets.drop_duplicates()
    
    initial_len = len(targets)
    targets = targets[targets['COUNT'] >= min_count].copy()
    
    print(f">>> Input Rows (Unique): {initial_len}")
    print(f">>> Filtered input to {len(targets)} rows (Min Count: {min_count})")
    
    if test_depth is not None:
        print(f">>> Running in TEST MODE: Processing top {test_depth} entries.")
        targets = targets.head(test_depth)

    results_list = []

    for idx, (original_idx, row) in enumerate(targets.iterrows()):
        omop_id = str(row['harmonization_omop::OMOP_ID'])
        test_abbr = row['cleaned::TEST_NAME_ABBREVIATION']
        
        print(f"[{idx+1}/{len(targets)}] Auditing OMOP {omop_id} for test '{test_abbr}'...")

        row_dict = row.to_dict()
        # Ensure exact column order requested
        row_dict.update({
            'NEW_UNIT': '',
            'NOTES': 'NA',
            'N_SOURCE': 0,
            'N_HARM': 0,
            'SOURCE_DECILES': 'NA',
            'HARM_DECILES': 'NA',
            'KS_STAT': 'NA'
        })

        try:
            source_df = pd.read_parquet(
                parquet_file,
                columns=['MEASUREMENT_VALUE_SOURCE', 'MEASUREMENT_VALUE_HARMONIZED'],
                filters=[('OMOP_CONCEPT_ID', '==', omop_id), ('TEST_NAME', '==', test_abbr)]
            )
            
            source_mask = source_df['MEASUREMENT_VALUE_SOURCE'].notna() & source_df['MEASUREMENT_VALUE_HARMONIZED'].isna()
            source_pop = pd.to_numeric(source_df.loc[source_mask, 'MEASUREMENT_VALUE_SOURCE'], errors='coerce').dropna()
            
            row_dict['N_SOURCE'] = len(source_pop)
            if not source_pop.empty:
                row_dict['SOURCE_DECILES'] = str(source_pop.quantile(np.linspace(0, 1, 11)).round(4).tolist())
            
            del source_df
            gc.collect()

            harm_df = pd.read_parquet(
                parquet_file,
                columns=['MEASUREMENT_VALUE_HARMONIZED', 'MEASUREMENT_UNIT_HARMONIZED'],
                filters=[('OMOP_CONCEPT_ID', '==', omop_id)]
            )
            
            valid_harm_rows = harm_df[harm_df['MEASUREMENT_VALUE_HARMONIZED'].notna()]
            harm_pop = pd.to_numeric(valid_harm_rows['MEASUREMENT_VALUE_HARMONIZED'], errors='coerce').dropna()
            row_dict['N_HARM'] = len(harm_pop)
            
            valid_units = valid_harm_rows['MEASUREMENT_UNIT_HARMONIZED'].astype(str).replace(['nan', 'None', ''], 'NA')
            common_unit = valid_units.mode().iloc[0] if not valid_units.empty else "NA"
            
            del harm_df
            gc.collect()

            if source_pop.empty:
                row_dict['NOTES'] = "No unharmonized source values found"
            elif harm_pop.empty:
                row_dict['NOTES'] = "No harmonized reference data in this OMOP ID"
            else:
                ks_stat, p_val = ks_2samp(source_pop, harm_pop)
                row_dict['HARM_DECILES'] = str(harm_pop.quantile(np.linspace(0, 1, 11)).round(4).tolist())
                row_dict['KS_STAT'] = f"{ks_stat:.4f} ({p_val:.4e})"
                row_dict['NEW_UNIT'] = common_unit if ks_stat < 0.3 else ""
                row_dict['NOTES'] = "Success" if ks_stat < 0.3 else "Distributions differ (KS >= 0.3)"

        except Exception as e:
            row_dict['NOTES'] = f"Error: {str(e)}"

        results_list.append(row_dict)

    final_df = pd.DataFrame(results_list)
    final_df.to_csv(output_file, index=False, sep='\t')
    print(f"\nAudit complete. Final output lines (inc header): {len(final_df) + 1}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--parquet", required=True)
    parser.add_argument("--output", default="harmonization_audit_results.tsv")
    parser.add_argument("--min_count", type=int, default=1000)
    parser.add_argument("--test", nargs='?', const=3, type=int)
    args = parser.parse_args()
    
    audit_harmonization(args.input, args.parquet, args.output, args.min_count, args.test)
