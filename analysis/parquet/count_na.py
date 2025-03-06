import duckdb
import sys
# Path to your large Parquet file
file_path = sys.argv[1]

# Connect to DuckDB (in-memory database by default)
con = duckdb.connect()

# Query to count NA values in each column
# First, get the column names
columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = 'parquet_scan' AND table_schema = 'main'"
con.execute(f"CREATE VIEW parquet_view AS SELECT * FROM '{file_path}'")
column_results = con.execute(columns_query).fetchall()

# If the above doesn't work (older DuckDB versions), try this alternative to get columns
if not column_results:
    # Sample a small number of rows to get column names
    sample_query = f"SELECT * FROM '{file_path}' LIMIT 1"
    sample_df = con.execute(sample_query).fetchdf()
    column_names = sample_df.columns.tolist()
else:
    column_names = [col[0] for col in column_results]

# For each column, count NULLs
results = {}
for column in column_names:
    query = f"SELECT COUNT(*) AS null_count FROM '{file_path}' WHERE \"{column}\" IS NULL"
    count = con.execute(query).fetchone()[0]
    results[column] = count

# Also get total row count for percentage calculation
total_rows = con.execute(f"SELECT COUNT(*) FROM '{file_path}'").fetchone()[0]

# Create a markdown table
output = f"## NA Value Analysis\n\n"
output += f"**Total rows:** {total_rows:,}\n\n"
output += "| Column Name | NA Count | Percentage |\n"
output += "| ----------- | --------:| ----------:|\n"

# For each column, count NULLs
for column in column_names:
    query = f"SELECT COUNT(*) AS null_count FROM '{file_path}' WHERE \"{column}\" IS NULL"
    count = con.execute(query).fetchone()[0]
    percentage = (count / total_rows) * 100 if total_rows > 0 else 0
    
    # Format the output as a markdown table row
    output += f"| {column} | {count:,} | {percentage:.2f}% |\n"

# Print results in markdown format
print(output)

# Close the connection
con.close()
