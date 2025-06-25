import duckdb
import pandas as pd
import argparse
import json
import re

def analyze_and_join_parquet_data(parquet_file, output_file_counts, output_file_joined, threshold=95.0):
    """
    1. Analyzes a Parquet file to get counts of all ID/TEXT pairs
    2. Joins this with the outcome analysis where TEST_OUTCOME_SOURCE=='O'
    3. Implements special merging logic for L/H with A values
    4. Only selects keys for extracted::TEST_OUTCOME that meet a minimum threshold percentage
    
    Args:
        parquet_file (str): The path to the input Parquet file.
        output_file_counts (str): The path to the output counts TSV file.
        output_file_joined (str): The path to the output joined TSV file.
        threshold (float): Minimum percentage threshold for selecting a key (default: 95.0).
    """
    try:
        con = duckdb.connect(database=':memory:')
        
        # Query 1: Get counts of all ID/TEXT pairs

        count_query = f"""
            SELECT
                OMOP_CONCEPT_ID,
                OUTCOME_TEXT_EXTRACTED,
                COUNT(CASE
                         WHEN OUTCOME_TEXT_EXTRACTED IS NOT NULL
                         AND (TEST_OUTCOME_SOURCE != 'O' OR TEST_OUTCOME_SOURCE IS NULL)
                    THEN 1
                    ELSE NULL
                END) AS TotalCount
            FROM '{parquet_file}'
            WHERE OUTCOME_TEXT_EXTRACTED IS NOT NULL AND OMOP_CONCEPT_ID IS NOT NULL
            GROUP BY OMOP_CONCEPT_ID, OUTCOME_TEXT_EXTRACTED
            ORDER BY TotalCount DESC, OMOP_CONCEPT_ID, OUTCOME_TEXT_EXTRACTED;
        """
        
        counts_df = con.execute(count_query).fetchdf()
        print(counts_df)
        # Query 2: Get the distributions directly with individual columns to avoid MAP parsing issues
        distribution_query = f"""
            WITH Filtered AS (
                SELECT
                    OMOP_CONCEPT_ID,
                    OUTCOME_TEXT_EXTRACTED,
                    TEST_OUTCOME_MERGED
                FROM '{parquet_file}'
                WHERE TEST_OUTCOME_SOURCE = 'O'
                  AND OMOP_CONCEPT_ID IS NOT NULL
                  AND OUTCOME_TEXT_EXTRACTED IS NOT NULL
            ),
            Grouped AS (
                SELECT
                    OMOP_CONCEPT_ID,
                    OUTCOME_TEXT_EXTRACTED,
                    TEST_OUTCOME_MERGED,
                    COUNT(*) AS MergedCount,
                    SUM(COUNT(*)) OVER (PARTITION BY OMOP_CONCEPT_ID, OUTCOME_TEXT_EXTRACTED) AS TotalCount
                FROM Filtered
                GROUP BY OMOP_CONCEPT_ID, OUTCOME_TEXT_EXTRACTED, TEST_OUTCOME_MERGED
            ),
            PercentageCalculated AS (
                SELECT
                    OMOP_CONCEPT_ID,
                    OUTCOME_TEXT_EXTRACTED,
                    TEST_OUTCOME_MERGED,
                    MergedCount,
                    TotalCount,
                    CAST((MergedCount * 100.0) / TotalCount AS FLOAT) AS Percentage
                FROM Grouped
            )
            SELECT
                OMOP_CONCEPT_ID,
                OUTCOME_TEXT_EXTRACTED,
                LIST(TEST_OUTCOME_MERGED) AS Keys,
                LIST(Percentage) AS Percentages,
                LIST(MergedCount) AS Counts,
                FIRST(TEST_OUTCOME_MERGED ORDER BY Percentage DESC) AS MostCommonOutcome,
                MAX(Percentage) AS MaxPercentage,
                SUM(MergedCount) AS FilteredCount
            FROM PercentageCalculated
            GROUP BY OMOP_CONCEPT_ID, OUTCOME_TEXT_EXTRACTED
            ORDER BY OMOP_CONCEPT_ID, OUTCOME_TEXT_EXTRACTED;
        """
        distribution_df = con.execute(distribution_query).fetchdf()
        con.close()
        
        # Process the distribution data
        if distribution_df is not None and not distribution_df.empty:
            # Function to create distribution dictionary from separate lists
            def process_distribution_and_outcome(row):
                try:
                    # Process lists directly from DuckDB
                    keys = row['Keys']
                    percentages = row['Percentages']
                    
                    # Create distribution dictionary
                    dist_dict = {}
                    for k, p in zip(keys, percentages):
                        if k is not None:
                            dist_dict[k] = round(float(p), 2)
                    
                    # Check for merging conditions: either L or H (but not both) with A
                    has_L = 'L' in dist_dict
                    has_H = 'H' in dist_dict
                    has_A = 'A' in dist_dict
                    
                    merged_dict = dist_dict.copy()
                    merged_outcome = None
                    
                    # Apply merging logic
                    if has_A and (has_L != has_H):  # If there's A and either L or H (but not both)
                        if has_L:
                            # Merge L and A
                            merged_percentage = dist_dict.get('L', 0) + dist_dict.get('A', 0)
                            merged_dict['L,A'] = round(merged_percentage, 2)
                            merged_outcome = 'L,A'
                        elif has_H:
                            # Merge H and A
                            merged_percentage = dist_dict.get('H', 0) + dist_dict.get('A', 0)
                            merged_dict['H,A'] = round(merged_percentage, 2)
                            merged_outcome = 'H,A'
                    
                    # Format as JSON string with double quotes
                    clean_dist_str = json.dumps(dist_dict)

                    # Check if any merged outcome meets the threshold
                    outcome = row['MostCommonOutcome']
                    max_percentage = row['MaxPercentage']
                    
                    if merged_outcome and merged_dict[merged_outcome] >= threshold:
                        outcome = merged_outcome
                    elif pd.notna(max_percentage) and max_percentage >= threshold:
                        outcome = row['MostCommonOutcome']
                    else:
                        outcome = 'NA'
                    
                    return clean_dist_str, outcome
                except Exception as e:
                    print(f"Error processing row: {e}")
                    return '{}', 'NA'
            
            # Apply processing to each row
            distribution_df[['MergedDistribution', 'extracted::TEST_OUTCOME']] = distribution_df.apply(
                lambda row: pd.Series(process_distribution_and_outcome(row)), axis=1)
            
            # Drop unnecessary columns
            distribution_df = distribution_df.drop(columns=['Keys', 'Percentages', 'Counts', 'MostCommonOutcome', 'MaxPercentage'])
        else:
            print("Warning: No distribution data found!")
            distribution_df = pd.DataFrame(columns=['OMOP_CONCEPT_ID', 'OUTCOME_TEXT_EXTRACTED', 
                                                   'MergedDistribution', 'extracted::TEST_OUTCOME', 'FilteredCount'])

        # Save counts to file
        if counts_df is not None and not counts_df.empty:
            counts_df.to_csv(output_file_counts, sep='\t', index=False)
            print(f"Count results saved to: {output_file_counts}")
        else:
            print("Warning: No count data found!")
        
            # Join the dataframes
        if counts_df is not None and distribution_df is not None and not counts_df.empty:
            # Perform left join to keep all ID/TEXT pairs
            joined_df = pd.merge(
                counts_df,
                distribution_df,
                on=['OMOP_CONCEPT_ID', 'OUTCOME_TEXT_EXTRACTED'],
                how='left'
            )

            # Fill NaN values
            joined_df['MergedDistribution'] = joined_df['MergedDistribution'].fillna('{}').apply(lambda d: str(d).replace("\"", "'"))
            joined_df['extracted::TEST_OUTCOME'] = joined_df['extracted::TEST_OUTCOME'].fillna('NA')
            joined_df['FilteredCount'] = joined_df['FilteredCount'].fillna(0).astype(int)

            # Reorder columns to have extracted::TEST_OUTCOME as the 4th column
            joined_df = joined_df[['OMOP_CONCEPT_ID', 'OUTCOME_TEXT_EXTRACTED', 'TotalCount',
                                     'extracted::TEST_OUTCOME', 'MergedDistribution', 'FilteredCount']]

            # Save joined results
            joined_df.to_csv(output_file_joined, sep='\t', index=False)
            print(f"Joined results saved to: {output_file_joined}")
            print(f"Note: Using minimum threshold of {threshold}% for extracted outcome selection")
            print("        Special merging applied for L/H with A values when appropriate")
        else:
            print("Error: Cannot join dataframes due to missing or empty data")
    except Exception as e:
        print(f"Error processing Parquet file '{parquet_file}': {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyzes a Parquet file to count all ID/TEXT pairs and join with outcome analysis.")
    parser.add_argument("-i", "--input", dest="parquet_file", default='kanta_analysis_ft_outcome.parquet',
                        help="Path to the input Parquet file (default: kanta_analysis_ft_outcome.parquet)")
    parser.add_argument("-c", "--counts", dest="output_file_counts", default='outcome_counts.tsv',
                        help="Path to the output counts TSV file (default: outcome_counts.tsv)")
    parser.add_argument("-j", "--joined", dest="output_file_joined", default='outcome_joined.tsv',
                        help="Path to the output joined TSV file (default: outcome_joined.tsv)")
    parser.add_argument("-t", "--threshold", dest="threshold", type=float, default=95.0,
                        help="Minimum percentage threshold for selecting an outcome key (default: 95.0)")

    args = parser.parse_args()

    analyze_and_join_parquet_data(args.parquet_file, args.output_file_counts, args.output_file_joined, args.threshold)
