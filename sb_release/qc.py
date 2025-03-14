import duckdb,sys

def execute_and_store_results(file_path):
    con = duckdb.connect()

    # Assuming it's a Parquet file:
    query = """
    SELECT
        concat('Number of rows: ', replace(CAST(count(*) AS VARCHAR), ',', '\\u00A0')) as result
    FROM read_parquet(?)
    UNION ALL
    SELECT
        concat('Number of FINNGENIDs: ', replace(CAST(count(DISTINCT FINNGENID) AS VARCHAR), ',', '\\u00A0')) as result
    FROM read_parquet(?)
    UNION ALL
    SELECT
        concat('Number of OMOP Concept IDs: ', replace(CAST(count(DISTINCT OMOP_CONCEPT_ID) AS VARCHAR), ',', '\\u00A0')) as result
    FROM read_parquet(?)
    """

    results = con.execute(query, (file_path, file_path, file_path)).fetchall()

    con.close()
    return [row[0] for row in results]


def format_number_with_nbsp(number):
    """
    Formats an integer by adding non-breaking spaces between groups of 3 digits.
    """
    reversed_str = str(number)[::-1]  # Reverse the string
    formatted_parts = []
    for i in range(0, len(reversed_str), 3):formatted_parts.append(reversed_str[i:i + 3])
    formatted_str = "\u00A0".join(formatted_parts)[::-1]
    return formatted_str


results_list = execute_and_store_results(sys.argv[1])

# Print the results
for result in results_list:
    text,number = result.split(":")
    print(f"{text} : {format_number_with_nbsp(number)}")
