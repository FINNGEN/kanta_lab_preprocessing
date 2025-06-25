import argparse, gzip, sys, os
from operator import itemgetter
from datetime import datetime
from pathlib import Path
from magic_config import config

def main(args):
    # Convert sort columns from 1-based to 0-based indexing
    date = datetime.now().strftime("%Y_%m_%d")
    unique = os.path.join(args.out, f"{args.prefix}.txt.gz")
    dups = os.path.join(args.out, f"{args.prefix}_duplicates.txt.gz")
    print(f"Writing unique entries to: {unique}")
    print(f"Writing duplicates to: {dups}")
    
    with gzip.open(args.input, 'rt') as i, gzip.open(dups, 'wt') as dup, gzip.open(unique, 'wt') as out:
        header = next(i)
        out.write(f"{header}")
        dup.write(header)
        cols = [header.strip().split().index(elem) for elem in args.duplicate_cols]
        values = [''] * len(cols)
        print(cols)
        print(f"bash {','.join(map(str,[elem +1 for elem in cols]))}")
        dup_count = count = err_count = 0
        print(itemgetter(*cols)(header.strip().split()))
        for line in i:
            # Read in new sort values to compare
            new_values = itemgetter(*cols)(line.strip().split('\t'))
            if new_values != values:  # New value found
                values = new_values
                # Add row_id to unique entries
                out.write(f"{line}")
                count += 1
            else:
                dup.write(line)
                dup_count += 1
    
    total = count + dup_count
    dup_rate = round(dup_count / total, 4) if total > 0 else 0
    
    print(f"\nResults:")
    print(f"Unique entries: {count}")
    print(f"Duplicates: {dup_count}")
    print(f"Duplicate rate: {dup_rate:.2%}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process files for duplicate detection based on specified columns')
    parser.add_argument('--prefix', help='Output file prefix')
    parser.add_argument('--input', required=True, help='Input sorted file')
    parser.add_argument('-o', "--out", type=str, help="Folder in which to save the results (default = current working directory)", default=os.getcwd())
    args = parser.parse_args()
    args.duplicate_cols = config['dup_cols']
    print(args.duplicate_cols)
    if not args.prefix:
        args.prefix = Path(args.input).stem
    main(args)
