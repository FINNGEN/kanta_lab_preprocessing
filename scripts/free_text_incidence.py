import pandas as pd
import matplotlib.pyplot as plt
import argparse
import sys
from tqdm import tqdm
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from functools import partial

# Define required columns globally for both reading and chunk processing
REQUIRED_COLS = ['APPROX_EVENT_DATETIME', 'MEASUREMENT_FREE_TEXT', 'TEST_ID_IS_NATIONAL']

def get_data_chunks(file_path: str, chunk_size: int, limit: int | None):
    """
    Generator function that yields chunks of data from the input file.
    Stops after the first chunk if in test mode (limit is set).
    """
    is_test_mode = limit is not None
    
    read_kwargs = {
        'filepath_or_buffer': file_path,
        'sep': '\t',
        'engine': 'python',
        'usecols': REQUIRED_COLS,
        'chunksize': chunk_size,
        'iterator': True
    }

    chunk_iterator = pd.read_csv(**read_kwargs)
    
    for i, chunk in enumerate(chunk_iterator):
        
        # --- Test Mode Truncation ---
        if is_test_mode and i == 0 and len(chunk) > limit:
            chunk = chunk.head(limit)
        
        yield chunk
        
        # --- Test Mode Break Condition ---
        if is_test_mode:
            return

def process_chunk_wrapper(chunk):
    """
    Wrapper function for multiprocessing that returns tallies instead of updating in-place.
    This function must be picklable (top-level function).
    """
    monthly_tallies = {}
    contingency_tallies = defaultdict(int)
    
    # 1. Data Cleaning and Feature Engineering (Free Text)
    def is_text_available(text):
        if pd.isna(text):
            return False
        cleaned_text = str(text).strip()
        return cleaned_text != "" and cleaned_text.upper() != "NA"

    chunk['is_text_available'] = chunk['MEASUREMENT_FREE_TEXT'].apply(is_text_available)

    # 2. Data Cleaning and Feature Engineering (National ID)
    chunk['TEST_ID_IS_NATIONAL_NUM'] = pd.to_numeric(
        chunk['TEST_ID_IS_NATIONAL'], 
        errors='coerce'
    ).fillna(0).astype(int)

    # 3. Time Series Feature
    try:
        chunk['APPROX_EVENT_DATETIME'] = pd.to_datetime(chunk['APPROX_EVENT_DATETIME'])
        chunk['YearMonth'] = chunk['APPROX_EVENT_DATETIME'].dt.to_period('M')
    except Exception as e:
        print(f"Warning: Failed to parse datetimes in a chunk. Skipping chunk. Error: {e}")
        return monthly_tallies, contingency_tallies, 0

    # 4. Calculate Monthly Tallies
    for year_month, group in chunk.groupby('YearMonth'):
        total_events = len(group)
        available_text_count = group['is_text_available'].sum()
        
        if year_month not in monthly_tallies:
            monthly_tallies[year_month] = {'total_events': 0, 'available_text_count': 0}
        
        monthly_tallies[year_month]['total_events'] += total_events
        monthly_tallies[year_month]['available_text_count'] += available_text_count

    # 5. Calculate Contingency Table Tallies
    contingency_counts = chunk.groupby([
        'is_text_available',
        'TEST_ID_IS_NATIONAL_NUM'
    ]).size()
    
    for (text_available, is_national), count in contingency_counts.items():
        key = (int(text_available), int(is_national))
        contingency_tallies[key] += count
    
    return monthly_tallies, contingency_tallies, len(chunk)


def merge_tallies(monthly_tallies, contingency_tallies, result):
    """
    Merge results from a processed chunk into the main tallies.
    """
    chunk_monthly, chunk_contingency, rows = result
    
    # Merge monthly tallies
    for year_month, tallies in chunk_monthly.items():
        if year_month not in monthly_tallies:
            monthly_tallies[year_month] = {'total_events': 0, 'available_text_count': 0}
        monthly_tallies[year_month]['total_events'] += tallies['total_events']
        monthly_tallies[year_month]['available_text_count'] += tallies['available_text_count']
    
    # Merge contingency tallies
    for key, count in chunk_contingency.items():
        contingency_tallies[key] += count
    
    return rows


def analyze_and_plot_text_incidence(file_path: str, limit: int | None, chunk_size: int, 
                                    output_file: str, num_workers: int):
    """
    Processes event data using multiprocessing chunking with in-memory tallies only,
    calculates incidence and contingency table, and saves the plot to a file.
    """
    
    # Use dictionaries to store running tallies instead of lists
    monthly_tallies = {}  # {YearMonth: {'total_events': int, 'available_text_count': int}}
    contingency_tallies = defaultdict(int)  # {(text_available, is_national): count}
    total_processed_rows = 0

    is_test_mode = limit is not None

    print("\n--- Data Loading and Chunk Processing ---")

    if is_test_mode:
        print(f"File: {file_path}. Processing first chunk (up to {chunk_size} rows), limited to {limit} rows (--test mode).")
        print(f"Running in single-threaded mode for test.")
        actual_workers = 1
    else:
        print(f"File: {file_path}. Processing entire file with chunk size {chunk_size}.")
        print(f"Using {num_workers} worker processes for parallel processing.")
        actual_workers = num_workers

    try:
        # Get the generator
        chunk_generator = get_data_chunks(file_path, chunk_size, limit)
        
        if actual_workers == 1:
            # Single-threaded processing (for test mode or when workers=1)
            chunks_to_process = tqdm(
                chunk_generator,
                desc="Processing File Chunks",
                unit="chunk",
                disable=is_test_mode,
            )

            for i, chunk in enumerate(chunks_to_process):
                current_chunk_size = len(chunk)
                
                status_msg = f"Processing chunk {i+1} ({current_chunk_size} rows)..."
                if not is_test_mode:
                    chunks_to_process.set_description(status_msg.split('(')[0].strip()) 
                    chunks_to_process.set_postfix_str(f"Chunk size: {current_chunk_size:,}")
                else:
                    print(status_msg)
                
                # Process the chunk
                result = process_chunk_wrapper(chunk)
                rows_processed = merge_tallies(monthly_tallies, contingency_tallies, result)
                
                if rows_processed > 0:
                    total_processed_rows += rows_processed
                    if not is_test_mode:
                        chunks_to_process.set_postfix_str(f"Total Rows: {total_processed_rows / 1_000_000:.2f}M")
                
                if is_test_mode:
                    print(f"Stopping after chunk {i+1} because --test flag is present. Processed {total_processed_rows} rows.")
        
        else:
            # Multi-threaded processing
            with Pool(processes=actual_workers) as pool:
                # Use imap for lazy evaluation - processes chunks as they're read
                chunk_results = pool.imap(process_chunk_wrapper, chunk_generator, chunksize=1)
                
                # Wrap with tqdm for progress bar
                chunks_with_progress = tqdm(
                    chunk_results,
                    desc="Processing File Chunks",
                    unit="chunk"
                )
                
                for i, result in enumerate(chunks_with_progress):
                    rows_processed = merge_tallies(monthly_tallies, contingency_tallies, result)
                    
                    if rows_processed > 0:
                        total_processed_rows += rows_processed
                        chunks_with_progress.set_postfix_str(f"Total Rows: {total_processed_rows / 1_000_000:.2f}M")

    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return
    except Exception as e:
        print(f"An error occurred during file reading or processing: {e}")
        return

    # Check if any data was processed
    if not monthly_tallies:
        print("No data loaded or processed. Exiting.")
        return

    # --- Final Aggregation and Calculation ---
    
    # 1. Convert monthly tallies dictionary to DataFrame
    monthly_data = []
    for year_month, tallies in monthly_tallies.items():
        monthly_data.append({
            'YearMonth': year_month,
            'total_events': tallies['total_events'],
            'available_text_count': tallies['available_text_count']
        })
    
    monthly_summary = pd.DataFrame(monthly_data).sort_values('YearMonth')
    
    # Calculate the incidence
    monthly_summary['incidence'] = (
        monthly_summary['available_text_count'] / monthly_summary['total_events']
    )
    
    # Convert Period to Timestamp for plotting
    monthly_summary['Date'] = monthly_summary['YearMonth'].apply(lambda x: x.to_timestamp())

    # 2. Create contingency table from tallies
    print("\n--- Contingency Table (MEASUREMENT_FREE_TEXT vs. TEST_ID_IS_NATIONAL) ---")
    
    # Define labels for clarity
    row_labels = {1: 'Text Available (1)', 0: 'Text NA/Missing (0)'}
    col_labels = {1: 'Is National (1)', 0: 'Not National (0)'}

    # Build contingency table from tallies
    contingency_data = []
    for (text_available, is_national), count in contingency_tallies.items():
        contingency_data.append({
            'text_available': text_available,
            'is_national': is_national,
            'count': count
        })
    
    contingency_df = pd.DataFrame(contingency_data)
    contingency_table = contingency_df.pivot_table(
        index='text_available',
        columns='is_national',
        values='count',
        fill_value=0
    ).astype(int)
    
    contingency_table.index = [row_labels.get(i, i) for i in contingency_table.index]
    contingency_table.columns = [col_labels.get(i, i) for i in contingency_table.columns]
    contingency_table.index.name = 'MEASUREMENT_FREE_TEXT Status'
    contingency_table.columns.name = 'TEST_ID_IS_NATIONAL Status'

    print(contingency_table)

    # --- Reporting the Final Tally ---
    print("\n--- Final Monthly Tally Summary (Top 5 Rows) ---")
    print(monthly_summary[['YearMonth', 'total_events', 'available_text_count', 'incidence']].head())
    print(f"\nTotal events processed: {total_processed_rows}")
    
    print("\n--- Final Monthly Tally Summary (Bottom 5 Rows) ---")
    print(monthly_summary[['YearMonth', 'total_events', 'available_text_count', 'incidence']].tail())

    # --- Plotting and Saving the Time Evolution ---
    plt.figure(figsize=(12, 6))
    plt.plot(
        monthly_summary['Date'], 
        monthly_summary['incidence'] * 100,
        marker='o', 
        linestyle='-', 
        color='#1f77b4',
        label='Free Text Availability'
    )

    plt.title('Time Evolution of Available Free Text Incidence', fontsize=16)
    plt.xlabel('Time (Year-Month)', fontsize=12)
    plt.ylabel('Incidence of Available Free Text (%)', fontsize=12)
    
    plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45, ha='right')
    
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    
    try:
        plt.savefig(output_file)
        print(f"\nPlot saved successfully to: {output_file}")
    except Exception as e:
        print(f"Error saving plot to {output_file}: {e}")
        
    plt.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Analyze event data for free text incidence and plot the time evolution using chunking for large files."
    )
    
    parser.add_argument(
        'file_path', 
        help='Path to the input TXT file (tab-separated).'
    )
    
    parser.add_argument(
        '--test', 
        type=int, 
        metavar='N', 
        nargs='?',
        const=10000,
        default=None, 
        help='If present, read only the first N lines of the first chunk. Reads 10000 lines if N is not specified. If the flag is omitted, the entire file is read.'
    )
    
    parser.add_argument(
        '--chunksize',
        type=int,
        default=1000000,
        help='Number of rows to process per chunk. Defaults to 1000000.'
    )
    
    parser.add_argument(
        '-o', '--out',
        type=str,
        default='free_text_incidence_plot.png',
        help='Filename for the output plot image (e.g., incidence.png). Defaults to "free_text_incidence_plot.png".'
    )
    
    parser.add_argument(
        '-w', '--workers',
        type=int,
        default=None,
        help='Number of worker processes for parallel processing. Defaults to CPU count - 1. Use 1 for single-threaded processing.'
    )

    args = parser.parse_args()
    
    # Determine number of workers
    if args.workers is None:
        num_workers = max(1, cpu_count() - 1)  # Leave one CPU free
    else:
        num_workers = max(1, args.workers)  # Ensure at least 1 worker
    
    analyze_and_plot_text_incidence(
        file_path=args.file_path, 
        limit=args.test,
        chunk_size=args.chunksize,
        output_file=args.out,
        num_workers=num_workers
    )
