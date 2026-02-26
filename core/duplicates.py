import argparse, gzip, os
from operator import itemgetter
from datetime import datetime
from pathlib import Path
from magic_config import config

TEST_MODE_DEFAULT_LINES = 1_000_000  # Default number of lines for test mode

def main(args):
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
        total_lines = 0

        limit = args.test  # None if no --test, or int if --test is set

        for line in i:
            if limit is not None and total_lines >= limit:
                break
            new_values = itemgetter(*cols)(line.strip().split('\t'))
            if new_values != values:
                values = new_values
                out.write(f"{line}")
                count += 1
            else:
                dup.write(line)
                dup_count += 1
            total_lines += 1
            if total_lines % 1_000_000 == 0:
                print(f"Processed {total_lines:,} lines...")

    total = count + dup_count
    dup_rate = round(dup_count / total, 4) if total > 0 else 0
    
    print(f"\nResults:")
    print(f"Unique entries: {count}")
    print(f"Duplicates: {dup_count}")
    print(f"Processed lines: {total_lines}")
    print(f"Duplicate rate: {dup_rate:.2%}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process files for duplicate detection based on specified columns')
    parser.add_argument('--prefix', help='Output file prefix')
    parser.add_argument('--input', required=True, help='Input sorted file')
    parser.add_argument('-o', "--out", type=str, help="Folder in which to save the results (default = current working directory)", default=os.getcwd())
    parser.add_argument(
        '--test',
        nargs='?',
        const=TEST_MODE_DEFAULT_LINES,
        type=int,
        help=f"Enable test mode; processes a limited number of lines (default: {TEST_MODE_DEFAULT_LINES:,} if flag is given without value)"
    )
    args = parser.parse_args()
    args.duplicate_cols = config['dup_cols']
    print(args.duplicate_cols)
    if not args.prefix:
        args.prefix = Path(args.input).stem
    main(args)
