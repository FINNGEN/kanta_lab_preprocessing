import duckdb
import sys
import os
import argparse
from pathlib import Path

#python3 -c "import pandas as pd; v3=pd.read_csv('kanta_v3_omop_analysis.tsv',sep='\t'); v2=pd.read_csv('kanta_v2_omop_analysis.tsv',sep='\t'); names=pd.read_csv('/mnt/disks/data/kanta/meta/omop_name_table.tsv',sep='\t'); names['NAME']=names['conceptId'].astype(str); m=v3.merge(v2,on='NAME',how='left',suffixes=('_v3','_v2')); m=m.merge(names[['NAME','conceptName']],on='NAME',how='left'); cols=[c for c in v3.columns if c!='NAME']; out={'NAME':m['NAME']}; out.update({c:m.apply(lambda r,col=c:pd.NA if pd.isna(r[f'{col}_v2']) or r[f'{col}_v2']==0 else round(r[f'{col}_v3']/r[f'{col}_v2'],3),axis=1) for c in cols}); out['conceptName']=m['conceptName']; pd.DataFrame(out).to_csv('relative_change.tsv',sep='\t',index=False,na_rep='NA'); print('Done')"

def analyze_omop_data(file_path, columns, binary_columns, output_folder, test_mode=False, top_n=None,prefix =""):
    """
    Analyze non-NA entries for specified columns broken down by OMOP ID.
    
    Args:
        file_path: Path to the Parquet file
        columns: List of column names to analyze for non-NA counts
        binary_columns: List of binary column names to analyze for count of 0s
        output_folder: Folder to save output markdown file
        test_mode: If True, only analyze top 5 OMOP IDs and first 10k rows
        top_n: If provided, only analyze top N OMOP IDs by count
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
    
    # Get OMOP IDs sorted by count (descending) and cache the results
    print("Getting OMOP ID counts...")
    omop_query = f"""
        SELECT {omop_column}, COUNT(*) as cnt 
        FROM data_view 
        WHERE {omop_column} IS NOT NULL
        GROUP BY {omop_column} 
        ORDER BY cnt DESC
    """
    omop_results = con.execute(omop_query).fetchall()
    
    # In test mode, limit to top 5 OMOP IDs
    if test_mode:
        omop_results = omop_results[:5]
    # If top_n is specified, limit to top N OMOP IDs
    elif top_n is not None:
        omop_results = omop_results[:top_n]
        print(f"Limiting analysis to top {top_n} OMOP IDs")
    
    print(f"Found {len(omop_results)} OMOP IDs to analyze")
    
    # Create output markdown
    output = f"## Non-NA Entry Analysis by OMOP ID\n\n"
    output += f"**Total rows analyzed:** {total_rows:,}\n"
    output += f"**Columns analyzed (non-NA count):** {', '.join(columns)}\n"
    if binary_columns:
        output += f"**Binary columns analyzed (count of 0s):** {', '.join(binary_columns)}\n"
    if test_mode:
        output += f"**Mode:** TEST (10k rows, top 5 OMOP IDs)\n"
    elif top_n is not None:
        output += f"**Limit:** Top {top_n} OMOP IDs by count\n"
    output += "\n"
    
    # Prepare the table structure
    # Calculate column widths
    name_width = max(len("NAME"), max(len(str(omop_id)) for omop_id, _ in omop_results) if omop_results else 10)
    id_width = max(len("FINNGENID"), 10)
    
    # Column widths for each analyzed column (non-NA counts)
    col_widths = {col: max(len(col.upper() + "_COUNT"), 10) for col in columns}
    
    # Column widths for binary columns (count of 0s)
    binary_widths = {col: max(len(col.upper() + "_ZEROS"), 10) for col in binary_columns}
    
    # Create header
    header_parts = [
        "NAME".ljust(name_width),
        "FINNGENID".rjust(id_width)
    ]
    header_parts.extend([f"{col.upper()}_COUNT".rjust(col_widths[col]) for col in columns])
    header_parts.extend([f"{col.upper()}_ZEROS".rjust(binary_widths[col]) for col in binary_columns])
    output += "| " + " | ".join(header_parts) + " |\n"
    
    # Create separator
    separator_parts = ["-" * name_width, "-" * id_width]
    separator_parts.extend(["-" * col_widths[col] for col in columns])
    separator_parts.extend(["-" * binary_widths[col] for col in binary_columns])
    output += "| " + " | ".join(separator_parts) + " |\n"
    
    # Calculate overall statistics (ALL row)
    print("Calculating overall statistics...")
    overall_finngenid_count = con.execute("SELECT COUNT(DISTINCT finngenid) FROM data_view WHERE finngenid IS NOT NULL").fetchone()[0]
    
    # Store results for TSV output
    overall_stats = {"finngenid": overall_finngenid_count}
    
    overall_row = ["ALL".ljust(name_width), f"{overall_finngenid_count:,}".rjust(id_width)]
    
    # Non-NA counts for regular columns
    for col in columns:
        count_query = f'SELECT COUNT(*) FROM data_view WHERE "{col}" IS NOT NULL'
        count = con.execute(count_query).fetchone()[0]
        overall_stats[col] = count
        overall_row.append(f"{count:,}".rjust(col_widths[col]))
    
    # Count of 0s for binary columns
    for col in binary_columns:
        count_query = f'SELECT COUNT(*) FROM data_view WHERE "{col}" = 0'
        count = con.execute(count_query).fetchone()[0]
        overall_stats[f"{col}_zeros"] = count
        overall_row.append(f"{count:,}".rjust(binary_widths[col]))
    
    output += "| " + " | ".join(overall_row) + " |\n"
    
    # Process each OMOP ID
    omop_stats = []  # Store results for TSV output
    
    for idx, (omop_id, omop_count) in enumerate(omop_results, 1):
        print(f"Processing OMOP ID {omop_id} ({idx}/{len(omop_results)})...")
        
        # Count distinct FINNGENIDs for this OMOP ID
        finngenid_query = f"""
            SELECT COUNT(DISTINCT finngenid) 
            FROM data_view 
            WHERE {omop_column} = {omop_id} AND finngenid IS NOT NULL
        """
        finngenid_count = con.execute(finngenid_query).fetchone()[0]
        
        # Store this OMOP ID's stats
        omop_stat = {"omop_id": omop_id, "finngenid": finngenid_count}
        
        # Create row for this OMOP ID
        row_parts = [str(omop_id).ljust(name_width), f"{finngenid_count:,}".rjust(id_width)]
        
        # Count non-NA entries for each column for this OMOP ID
        for col in columns:
            count_query = f"""
                SELECT COUNT(*) 
                FROM data_view 
                WHERE {omop_column} = {omop_id} AND "{col}" IS NOT NULL
            """
            count = con.execute(count_query).fetchone()[0]
            omop_stat[col] = count
            row_parts.append(f"{count:,}".rjust(col_widths[col]))
        
        # Count 0s for each binary column for this OMOP ID
        for col in binary_columns:
            count_query = f"""
                SELECT COUNT(*) 
                FROM data_view 
                WHERE {omop_column} = {omop_id} AND "{col}" = 0
            """
            count = con.execute(count_query).fetchone()[0]
            omop_stat[f"{col}_zeros"] = count
            row_parts.append(f"{count:,}".rjust(binary_widths[col]))
        
        omop_stats.append(omop_stat)
        output += "| " + " | ".join(row_parts) + " |\n"
    
    # Save markdown output to file
    os.makedirs(output_folder, exist_ok=True)
    mode_suffix = "_test" if test_mode else ""
    output_file = os.path.join(output_folder, f"{prefix}_omop_analysis{mode_suffix}.md")
    
    with open(output_file, 'w') as f:
        f.write(output)
    
    print(f"\nAnalysis complete! Markdown output saved to: {output_file}")
    
    # Create TSV output using stored results
    tsv_file = os.path.join(output_folder, f"{prefix}_omop_analysis{mode_suffix}.tsv")
    
    with open(tsv_file, 'w') as f:
        # Write header
        header_parts = ["NAME", "FINNGENID"]
        header_parts.extend([f"{col.upper()}_COUNT" for col in columns])
        header_parts.extend([f"{col.upper()}_ZEROS" for col in binary_columns])
        f.write("\t".join(header_parts) + "\n")
        
        # Write ALL row using stored overall_stats
        all_row = ["ALL", str(overall_stats["finngenid"])]
        all_row.extend([str(overall_stats[col]) for col in columns])
        all_row.extend([str(overall_stats[f"{col}_zeros"]) for col in binary_columns])
        f.write("\t".join(all_row) + "\n")
        
        # Write OMOP ID rows using stored omop_stats
        for stat in omop_stats:
            row = [str(stat["omop_id"]), str(stat["finngenid"])]
            row.extend([str(stat[col]) for col in columns])
            row.extend([str(stat[f"{col}_zeros"]) for col in binary_columns])
            f.write("\t".join(row) + "\n")
    
    print(f"TSV output saved to: {tsv_file}")
    print("\n" + output)
    
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
        """
    )
    
    parser.add_argument('parquet_file', help='Path to the Parquet file')
    parser.add_argument('-c', '--columns', required=True, 
                        help='Comma-separated list of columns to analyze for non-NA counts (e.g., measurement_value,finngenid)')
    parser.add_argument('-b', '--binary-columns', default='',
                        help='Comma-separated list of binary columns to analyze for count of 0s (optional)')
    parser.add_argument('-o', '--output', required=True,
                        help='Output folder for results')
    parser.add_argument('-p', '--prefix', default ="kanta",
                        help='Output prefix')
    
    parser.add_argument('--test', action='store_true',
                        help='Run in test mode (first 10k rows, top 5 OMOP IDs)')
    parser.add_argument('--top-n', type=int, metavar='N',
                        help='Analyze only the top N OMOP IDs by count')
    
    args = parser.parse_args()
    
    # Parse column lists
    columns = [col.strip() for col in args.columns.split(',') if col.strip()]
    binary_columns = [col.strip() for col in args.binary_columns.split(',') if col.strip()]
    
    if not columns:
        parser.error("At least one column must be specified with -c/--columns")
    
    analyze_omop_data(args.parquet_file, columns, binary_columns, args.output, args.test, args.top_n,args.prefix)
