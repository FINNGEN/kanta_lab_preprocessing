import gzip
import argparse
from collections import Counter
import sys

def main():
    parser = argparse.ArgumentParser(description="Analyze Kanta data for mismatches and unmapped tests with mapping status.")
    parser.add_argument("input", help="Path to the kanta_..._munged.txt.gz file")
    parser.add_argument("-o", "--mismatch-output", help="Output TSV for mismatches")
    parser.add_argument("-u", "--unmapped-output", help="Output TSV for unmapped tests")
    args = parser.parse_args()

    # Counters and State
    mismatch_counts = Counter()
    unmapped_counts = Counter()
    # Keep track of which test abbreviations have at least one valid OMOP mapping (> -1)
    mapped_test_names = set()

    # Exact column names
    COL_OMOP = "harmonization_omop::OMOP_ID"
    COL_TEST = "cleaned::TEST_NAME_ABBREVIATION"
    COL_UNIT = "cleaned::MEASUREMENT_UNIT"
    COL_SRC_VAL = "source::MEASUREMENT_VALUE"
    COL_HARM_VAL = "harmonization_omop::MEASUREMENT_VALUE"

    try:
        with gzip.open(args.input, 'rt', encoding='utf-8') as f:
            line = f.readline()
            if not line:
                return
            
            header = line.strip().split('\t')
            try:
                idx_omop = header.index(COL_OMOP)
                idx_test = header.index(COL_TEST)
                idx_unit = header.index(COL_UNIT)
                idx_src_val = header.index(COL_SRC_VAL)
                idx_harm_val = header.index(COL_HARM_VAL)
            except ValueError as e:
                print(f"Error: Could not find required column: {e}", file=sys.stderr)
                sys.exit(1)

            for line in f:
                fields = line.strip().split('\t')
                if len(fields) <= max(idx_omop, idx_test, idx_unit, idx_src_val, idx_harm_val):
                    continue
                
                omop_id   = fields[idx_omop]
                test_name = fields[idx_test]
                unit      = fields[idx_unit]
                src_val   = fields[idx_src_val]
                harm_val  = fields[idx_harm_val]

                if omop_id == "-1":
                    unmapped_counts[(test_name, unit)] += 1
                else:
                    # Mark this test name as having at least one valid mapping in the file
                    mapped_test_names.add(test_name)
                    
                    # Logic 2: Mismatches
                    if src_val != "NA" and (harm_val == "NA" or harm_val == ""):
                        mismatch_counts[(omop_id, test_name, unit)] += 1

        # Write Mismatch Output
        if args.mismatch_output:
            with open(args.mismatch_output, 'w', encoding='utf-8') as out_m:
                out_m.write(f"{COL_OMOP}\t{COL_TEST}\t{COL_UNIT}\tCOUNT\n")
                for (omop, test, unit), count in mismatch_counts.most_common():
                    out_m.write(f"{omop}\t{test}\t{unit}\t{count}\n")

        # Write Unmapped Output
        if args.unmapped_output:
            with open(args.unmapped_output, 'w', encoding='utf-8') as out_u:
                # Adding the new boolean column to the header
                out_u.write(f"{COL_TEST}\t{COL_UNIT}\tCOUNT\tHAS_ANY_MAPPING\n")
                for (test, unit), count in unmapped_counts.most_common():
                    # Check if this specific test_name was ever seen with a valid OMOP_ID
                    has_mapping = test in mapped_test_names
                    out_u.write(f"{test}\t{unit}\t{count}\t{has_mapping}\n")
            
            print(f"Reports generated successfully.")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
