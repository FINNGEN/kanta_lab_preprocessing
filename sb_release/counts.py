import duckdb
import sys
import os
import argparse
from pathlib import Path

#python3 -c "import pandas as pd; import matplotlib.pyplot as plt; import numpy as np; v3=pd.read_csv('kanta_v3_omop_analysis.tsv',sep='\t'); v2=pd.read_csv('kanta_v2_omop_analysis.tsv',sep='\t'); names=pd.read_csv('/mnt/disks/data/kanta/meta/omop_name_table.tsv',sep='\t'); names.__setitem__('NAME',names['conceptId'].astype(str)); m=v3.merge(v2,on='NAME',how='left',suffixes=('_v3','_v2')).merge(names[['NAME','conceptName']],on='NAME',how='left'); cols=[c for c in v3.columns if c!='NAME']; out={'NAME':m['NAME']}; out.update({c:m.apply(lambda r,col=c:pd.NA if pd.isna(r[f'{col}_v2']) or r[f'{col}_v2']==0 else round(r[f'{col}_v3']/r[f'{col}_v2'],3),axis=1) for c in cols}); out.__setitem__('conceptName',m['conceptName']); df_out=pd.DataFrame(out); df_out.to_csv('relative_change.tsv',sep='\t',index=False,na_rep='NA'); print('Saved relative_change.tsv'); fig,axes=plt.subplots(len(cols),2,figsize=(16,4*len(cols))); axes=axes.reshape(-1,2) if len(cols)>1 else axes.reshape(1,2); [(lambda data,i,col: (axes[i,0].hist(data,bins=50,edgecolor='black',alpha=0.7), axes[i,0].axvline(x=1.0,color='red',linestyle='--',linewidth=2,label='No change (1.0)'), axes[i,0].set_xlabel('Relative Change (v3/v2)'), axes[i,0].set_ylabel('Frequency'), axes[i,0].set_title(f'{col} - Full Distribution'), axes[i,0].legend(), axes[i,0].grid(True,alpha=0.3), axes[i,0].text(0.98,0.98,f'Mean: {data.mean():.3f}\\nMedian: {data.median():.3f}\\nN: {len(data)}',transform=axes[i,0].transAxes,verticalalignment='top',horizontalalignment='right',bbox=dict(boxstyle='round',facecolor='wheat',alpha=0.5)), axes[i,1].hist(data[data<=2],bins=50,edgecolor='black',alpha=0.7), axes[i,1].axvline(x=1.0,color='red',linestyle='--',linewidth=2,label='No change (1.0)'), axes[i,1].set_xlabel('Relative Change (v3/v2)'), axes[i,1].set_ylabel('Frequency'), axes[i,1].set_title(f'{col} - Zoomed [0,2]'), axes[i,1].set_xlim(0,2), axes[i,1].legend(), axes[i,1].grid(True,alpha=0.3), axes[i,1].text(0.98,0.98,f'Mean: {data[data<=2].mean():.3f}\\nMedian: {data[data<=2].median():.3f}\\nN [0,2]: {len(data[data<=2])}\\nN >2: {len(data[data>2])}',transform=axes[i,1].transAxes,verticalalignment='top',horizontalalignment='right',bbox=dict(boxstyle='round',facecolor='wheat',alpha=0.5))))(df_out[col].dropna(),i,col) for i,col in enumerate(cols) if len(df_out[col].dropna())>0]; plt.tight_layout(); plt.savefig('relative_change_histograms.png',dpi=300,bbox_inches='tight'); plt.close(); print('Saved relative_change_histograms.png'); print('Done!')"
def analyze_omop_data(file_path, columns, binary_columns, output_folder, test_mode=False, top_n=None, prefix=""):
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
    
    # Build the aggregation query
    print("Building aggregation query...")
    
    # Build SELECT clauses for non-NA counts
    non_na_clauses = [f'COUNT(CASE WHEN "{col}" IS NOT NULL THEN 1 END) as {col}_count' 
                      for col in columns]
    
    # Build SELECT clauses for binary column zero counts
    zero_clauses = [f'COUNT(CASE WHEN "{col}" = 0 THEN 1 END) as {col}_zeros' 
                    for col in binary_columns]
    
    # Combine all SELECT clauses
    select_clauses = [
        f'{omop_column}',
        'COUNT(DISTINCT finngenid) as finngenid_count',
        'COUNT(*) as row_count'
    ] + non_na_clauses + zero_clauses
    
    # Build the main query
    main_query = f"""
    SELECT {', '.join(select_clauses)}
    FROM data_view
    WHERE {omop_column} IS NOT NULL
    GROUP BY {omop_column}
    ORDER BY row_count DESC
    """
    
    print("Executing aggregation query...")
    results = con.execute(main_query).fetchall()
    
    # Get column names from the query
    col_names = [desc[0] for desc in con.description]
    
    # Apply limits if needed
    if test_mode:
        results = results[:5]
        print(f"Limited to top 5 OMOP IDs (test mode)")
    elif top_n is not None:
        results = results[:top_n]
        print(f"Limited to top {top_n} OMOP IDs")
    
    print(f"Found {len(results)} OMOP IDs to analyze")
    
    # Calculate overall statistics (ALL row)
    print("Calculating overall statistics...")
    
    overall_clauses = [
        'COUNT(DISTINCT finngenid) as finngenid_count'
    ] + non_na_clauses + zero_clauses
    
    overall_query = f"""
    SELECT {', '.join(overall_clauses)}
    FROM data_view
    WHERE finngenid IS NOT NULL
    """
    
    overall_result = con.execute(overall_query).fetchone()
    
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
    name_width = max(len("NAME"), max(len(str(row[0])) for row in results) if results else 10)
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
    
    # Store results for TSV output
    overall_stats = {"finngenid": overall_result[0]}
    overall_row = ["ALL".ljust(name_width), f"{overall_result[0]:,}".rjust(id_width)]
    
    # Add overall stats for regular columns
    idx = 1
    for col in columns:
        overall_stats[col] = overall_result[idx]
        overall_row.append(f"{overall_result[idx]:,}".rjust(col_widths[col]))
        idx += 1
    
    # Add overall stats for binary columns
    for col in binary_columns:
        overall_stats[f"{col}_zeros"] = overall_result[idx]
        overall_row.append(f"{overall_result[idx]:,}".rjust(binary_widths[col]))
        idx += 1
    
    output += "| " + " | ".join(overall_row) + " |\n"
    
    # Process each OMOP ID from results
    omop_stats = []
    for row in results:
        omop_id = row[0]
        finngenid_count = row[1]
        
        # Store this OMOP ID's stats
        omop_stat = {"omop_id": omop_id, "finngenid": finngenid_count}
        
        # Create row for this OMOP ID
        row_parts = [str(omop_id).ljust(name_width), f"{finngenid_count:,}".rjust(id_width)]
        
        # Add counts for regular columns (skip omop_id, finngenid_count, row_count)
        idx = 3
        for col in columns:
            count = row[idx]
            omop_stat[col] = count
            row_parts.append(f"{count:,}".rjust(col_widths[col]))
            idx += 1
        
        # Add counts for binary columns
        for col in binary_columns:
            count = row[idx]
            omop_stat[f"{col}_zeros"] = count
            row_parts.append(f"{count:,}".rjust(binary_widths[col]))
            idx += 1
        
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
    
    if not columns:
        parser.error("At least one column must be specified with -c/--columns")
    
    analyze_omop_data(
        args.parquet_file, 
        columns, 
        binary_columns, 
        args.output, 
        args.test, 
        args.top_n,
        args.prefix
    )
