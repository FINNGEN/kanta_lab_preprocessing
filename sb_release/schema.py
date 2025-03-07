#!/usr/bin/env python3
import duckdb
import json
import sys

def parquet_schema_to_bq_json(parquet_file):
    # Connect to DuckDB
    con = duckdb.connect(database=':memory:')
    
    # Register the parquet file
    con.execute(f"CREATE VIEW parquet_table AS SELECT * FROM read_parquet('{parquet_file}')")
    
    # Get schema information
    schema_query = """
    SELECT 
        column_name, 
        data_type,
        is_nullable
    FROM duckdb_columns() 
    WHERE table_name = 'parquet_table'
    """
    
    # Execute schema query
    column_info = con.execute(schema_query).fetch_df()
    
    # Map DuckDB types to BigQuery types
    type_mapping = {
        'VARCHAR': 'STRING',
        'DOUBLE': 'FLOAT64',
        'FLOAT': 'FLOAT64',
        'INTEGER': 'INT64',
        'BIGINT': 'INT64',
        'BOOLEAN': 'BOOL',
        'TIMESTAMP': 'DATETIME',
        'TIMESTAMP_NS': 'DATETIME',
        'TIMESTAMP_MS': 'DATETIME',
        'TIMESTAMP_SEC': 'DATETIME',
        'DATE': 'DATE'
    }
    
    # Create BigQuery schema
    bq_schema = []
    for index, row in column_info.iterrows():
        field_name = row['column_name']
        duckdb_type = row['data_type'].upper()
        is_nullable = row['is_nullable']
        
        # Map the DuckDB type to BigQuery type
        if duckdb_type in type_mapping:
            bq_type = type_mapping[duckdb_type]
        else:
            bq_type = "STRING"  # Default to STRING for unknown types
            
        # Determine mode (REQUIRED or NULLABLE)
        mode = "NULLABLE" if is_nullable else "REQUIRED"
        
        # Add field to schema
        bq_schema.append({
            "name": field_name,
            "type": bq_type,
            "mode": mode
        })
    
    return bq_schema

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_schema_duckdb.py <parquet_file> [output_file]")
        sys.exit(1)
        
    parquet_file = sys.argv[1]
    
    schema = parquet_schema_to_bq_json(parquet_file)
    
    # Pretty print the schema
    formatted_schema = json.dumps(schema, indent=4)
    
    # Save to file if specified, otherwise print to stdout
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
        with open(output_file, 'w') as f:
            f.write(formatted_schema)
        print(f"Schema saved to {output_file}")
    else:
        print(formatted_schema)
