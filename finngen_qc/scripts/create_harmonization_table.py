import pandas as pd
import argparse
import sys
import os

def main():
    parser = argparse.ArgumentParser(description="Find most common units for OMOP IDs.")
    # Use nargs='?' to make the positional argument optional
    parser.add_argument("input", nargs='?', 
                        default=os.path.expanduser("~/fg-3/kanta_v3/munged/kanta_v3_harmonized_2025_01_16.txt.gz"), 
                        help="Path to the input .gz or .txt file")
    parser.add_argument("--test", action="store_true", help="Only process the first 10,000 lines")
    parser.add_argument("--out", default="harmonization_counts.txt", help="Output filename")
    args = parser.parse_args()

    # Column Mapping
    id_in = "harmonization_omop::OMOP_ID"
    qt_in = "harmonization_omop::omopQuantity"
    un_in = "cleaned::MEASUREMENT_UNIT"
    un_out = "harmonization_omop::MEASUREMENT_UNIT"

    if not os.path.exists(args.input):
        print(f"Error: File not found at {args.input}")
        sys.exit(1)

    print(f"--- Starting Process ---")
    print(f"Input:  {args.input}")
    print(f"Test:   {args.test}")
    
    try:
        # Use chunksize to keep memory usage low
        reader = pd.read_csv(
            args.input, 
            sep='\t', 
            usecols=[id_in, qt_in, un_in],
            nrows=1000000 if args.test else None,
            chunksize=1_000_000, 
            engine='c',
            low_memory=False
        )

        full_counts = pd.DataFrame()

        for i, chunk in enumerate(reader):
            print(f" > Processing lines: {(i+1)}M...", end='\r', flush=True)
            
            # Count occurrences within this chunk
            chunk_counts = chunk.groupby([id_in, qt_in, un_in], dropna=False).size().reset_index(name='n')
            
            # Combine with existing counts and re-sum
            full_counts = pd.concat([full_counts, chunk_counts])
            full_counts = full_counts.groupby([id_in, qt_in, un_in], as_index=False).sum()

        print("\n--- Aggregating Final Results ---")
        
        # Sort by 'n' descending so drop_duplicates keeps the most frequent unit
        result = (
            full_counts.sort_values('n', ascending=False)
            .drop_duplicates(subset=[id_in, qt_in])
            .drop(columns=['n'])
            .rename(columns={un_in: un_out})
        )

        result.to_csv(args.out, sep='\t', index=False)
        print(f"Success! Results saved to: {args.out}")

    except Exception as e:
        print(f"\nError during processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
