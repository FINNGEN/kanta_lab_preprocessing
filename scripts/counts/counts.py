import duckdb
import sys
import os
import argparse
from pathlib import Path

def analyze_omop_data(file_path, columns, binary_columns, output_folder, test_mode=False, top_n=None, prefix="", true_columns=(),
                      any_columns=()):
    """
    Analyze non-NA entries for specified columns broken down by OMOP ID.

    Args:
        file_path: Path to the Parquet file
        columns: List of column names to analyze for non-NA counts
        binary_columns: List of binary column names to analyze for count of 0s
        true_columns: List of boolean column names to analyze for count of TRUE values
        any_columns: List of (name, [columns]) pairs to count the rows where at least one of the columns is non-NA
        output_folder: Folder to save output markdown file
        test_mode: If True, only analyze top 5 OMOP IDs and first 10k rows
        top_n: If provided, only analyze top N OMOP IDs by count

    A column missing from the input (e.g. one an older release doesn't have yet) is counted as all missing, so two
    releases' tables keep the same columns for count_plot.py.
    """
    # Connect to DuckDB
    con = duckdb.connect()

    # Default OMOP ID column name
    omop_column = 'OMOP_CONCEPT_ID'

    # Create a view with optional row limit for test mode
    if test_mode:
        print("Running in TEST MODE: analyzing first 10,000 rows and top 5 OMOP IDs")
        con.execute(f"CREATE VIEW data_view AS SELECT * FROM '{file_path}' LIMIT 10000")
    else:
        con.execute(f"CREATE VIEW data_view AS SELECT * FROM '{file_path}'")

    # Get total row count
    total_rows = con.execute("SELECT COUNT(*) FROM data_view").fetchone()[0]

    present = {r[0] for r in con.execute("DESCRIBE data_view").fetchall()}
    missing = sorted({c for c in [*columns, *binary_columns, *true_columns, *(c for _, cs in any_columns for c in cs)] if c not in present})
    if missing:
        print(f"Not in {file_path}, counted as all missing: {', '.join(missing)}")
    ref = lambda col: f'"{col}"' if col in present else 'NULL'

    # The measures, in output order: (header, SQL count)
    measures = (
        [(f"{col.upper()}_COUNT", f'COUNT(CASE WHEN {ref(col)} IS NOT NULL THEN 1 END)') for col in columns]
        + [(f"{col.upper()}_ZEROS", f'COUNT(CASE WHEN {ref(col)} = 0 THEN 1 END)') for col in binary_columns]
        + [(f"{col.upper()}_TRUES", f'COUNT(CASE WHEN {ref(col)}::BOOLEAN THEN 1 END)') for col in true_columns]
        + [(f"{name.upper()}_ANY_COUNT", f"COUNT(CASE WHEN {' OR '.join(f'{ref(c)} IS NOT NULL' for c in cols)} THEN 1 END)")
           for name, cols in any_columns]
    )
    headers = [h for h, _ in measures]
    clauses = [f"{sql} as m{i}" for i, (_, sql) in enumerate(measures)]

    # Build the main query
    print("Building aggregation query...")
    main_query = f"""
    SELECT {', '.join([omop_column, 'COUNT(DISTINCT finngenid) as finngenid_count', 'COUNT(*) as row_count'] + clauses)}
    FROM data_view
    WHERE {omop_column} IS NOT NULL
    GROUP BY {omop_column}
    ORDER BY row_count DESC
    """

    print("Executing aggregation query...")
    results = con.execute(main_query).fetchall()

    # Apply limits if needed
    if test_mode:
        results = results[:5]
        print("Limited to top 5 OMOP IDs (test mode)")
    elif top_n is not None:
        results = results[:top_n]
        print(f"Limited to top {top_n} OMOP IDs")

    print(f"Found {len(results)} OMOP IDs to analyze")

    # Calculate overall statistics (ALL row)
    print("Calculating overall statistics...")
    overall = con.execute(f"""
    SELECT {', '.join(['COUNT(DISTINCT finngenid) as finngenid_count'] + clauses)}
    FROM data_view
    WHERE finngenid IS NOT NULL
    """).fetchone()

    # Rows of the table: name, people, then one count per measure
    rows = [("ALL", overall[0], list(overall[1:]))] + [(str(r[0]), r[1], list(r[3:])) for r in results]

    # Create output markdown
    output = "## Non-NA Entry Analysis by OMOP ID\n\n"
    output += f"**Total rows analyzed:** {total_rows:,}\n"
    output += f"**Columns analyzed (non-NA count):** {', '.join(columns)}\n"
    if binary_columns:
        output += f"**Binary columns analyzed (count of 0s):** {', '.join(binary_columns)}\n"
    if true_columns:
        output += f"**Boolean columns analyzed (count of TRUE):** {', '.join(true_columns)}\n"
    if any_columns:
        output += f"**At least one non-NA:** {', '.join(f'{n}: ' + ' or '.join(cs) for n, cs in any_columns)}\n"
    if missing:
        output += f"**Not in the input (counted as missing):** {', '.join(missing)}\n"
    if test_mode:
        output += "**Mode:** TEST (10k rows, top 5 OMOP IDs)\n"
    elif top_n is not None:
        output += f"**Limit:** Top {top_n} OMOP IDs by count\n"
    output += "\n"

    name_width = max(len("NAME"), max((len(r[0]) for r in rows[1:]), default=10))
    id_width = max(len("FINNGENID"), 10)
    widths = [max(len(h), 10) for h in headers]
    line = lambda parts: "| " + " | ".join(parts) + " |\n"
    output += line(["NAME".ljust(name_width), "FINNGENID".rjust(id_width)] + [h.rjust(w) for h, w in zip(headers, widths)])
    output += line(["-" * name_width, "-" * id_width] + ["-" * w for w in widths])
    for name, people, counts in rows:
        output += line([name.ljust(name_width), f"{people:,}".rjust(id_width)] + [f"{n:,}".rjust(w) for n, w in zip(counts, widths)])

    # Save markdown output to file
    os.makedirs(output_folder, exist_ok=True)
    mode_suffix = "_test" if test_mode else ""
    output_file = os.path.join(output_folder, f"{prefix}_omop_analysis{mode_suffix}.md")
    with open(output_file, 'w') as f:
        f.write(output)
    print(f"\nAnalysis complete! Markdown output saved to: {output_file}")

    # Create TSV output
    tsv_file = os.path.join(output_folder, f"{prefix}_omop_analysis{mode_suffix}.tsv")
    with open(tsv_file, 'w') as f:
        f.write("\t".join(["NAME", "FINNGENID"] + headers) + "\n")
        for name, people, counts in rows:
            f.write("\t".join([name, str(people)] + [str(n) for n in counts]) + "\n")
    print(f"TSV output saved to: {tsv_file}")

    # Close connection
    con.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Analyze non-NA entries and binary traits by OMOP ID from Parquet files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Examples:
        # Basic usage with regular columns only
        python script.py data.parquet -c measurement_value,finngenid -o ./output
        
        # With binary columns
        python script.py data.parquet -c measurement_value,finngenid -b trait1,trait2 -o ./output
        
        # Analyze only top 10 OMOP IDs
        python script.py data.parquet -c measurement_value -b trait1 -o ./output --top-n 10
        
  # Test mode (first 10k rows, top 5 OMOP IDs)
        python script.py data.parquet -c measurement_value -b trait1 -o ./output --test
        
        # With custom prefix
        python script.py data.parquet -c measurement_value -b trait1 -o ./output -p kanta
        """
    )
    
    parser.add_argument('parquet_file', 
                        help='Path to the Parquet file')
    parser.add_argument('-c', '--columns', 
                        required=True,
                        help='Comma-separated list of columns to analyze for non-NA counts (e.g., measurement_value,finngenid)')
    parser.add_argument('-b', '--binary-columns', 
                        default='',
                        help='Comma-separated list of binary columns to analyze for count of 0s (optional)')
    parser.add_argument('-t', '--true-columns',
                        default='',
                        help='Comma-separated list of boolean columns to analyze for count of TRUE values (optional)')
    parser.add_argument('-a', '--any-columns',
                        default='',
                        help='Comma-separated NAME=COL1|COL2 groups: count the rows where at least one column is non-NA, as NAME_ANY_COUNT '
                             '(optional; e.g. REFERENCE_RANGE=REFERENCE_RANGE_LOW_VALUE|REFERENCE_RANGE_HIGH_VALUE)')
    parser.add_argument('-o', '--output', 
                        required=True,
                        help='Output folder for results')
    parser.add_argument('-p', '--prefix', 
                        default="kanta",
                        help='Output prefix (default: kanta)')
    parser.add_argument('--test', 
                        action='store_true',
                        help='Run in test mode (first 10k rows, top 5 OMOP IDs)')
    parser.add_argument('--top-n', 
                        type=int, 
                        metavar='N',
                        help='Analyze only the top N OMOP IDs by count')
    
    args = parser.parse_args()
    
    # Parse column lists
    columns = [col.strip() for col in args.columns.split(',') if col.strip()]
    binary_columns = [col.strip() for col in args.binary_columns.split(',') if col.strip()]
    true_columns = [col.strip() for col in args.true_columns.split(',') if col.strip()]
    any_columns = [x.strip().split('=', 1) for x in args.any_columns.split(',') if x.strip()]
    if any(len(g) != 2 for g in any_columns):
        parser.error("-a/--any-columns takes NAME=COL1|COL2 groups")
    any_columns = [(name, [c.strip() for c in cols.split('|') if c.strip()]) for name, cols in any_columns]
    
    if not columns:
        parser.error("At least one column must be specified with -c/--columns")
    
    analyze_omop_data(
        args.parquet_file, 
        columns, 
        binary_columns, 
        args.output, 
        args.test, 
        args.top_n,
        args.prefix,
        true_columns,
        any_columns
    )
