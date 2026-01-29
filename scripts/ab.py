#!/bin/python3
"""
Abnormality Threshold Analysis Script

This script analyzes medical laboratory measurements to determine abnormality thresholds
(upper and lower bounds) based on test outcome classifications. It processes OMOP 
(Observational Medical Outcomes Partnership) standardized healthcare data to estimate 
reference ranges for different laboratory tests.

The script operates in two phases:
1. Split Phase: Divides input data by OMOP measurement ID
2. Analysis Phase: Calculates threshold values where abnormal results consistently appear

Author: [Original Author]
Date: [Date]
"""

import sys
import os
import argparse
import gzip
import shutil
import shlex
import numbers
from pathlib import Path
from collections import defaultdict
import subprocess
import numpy as np
from utils import make_sure_path_exists, count_lines, progressBar
from operator import itemgetter


def split_input(munged_file, omop_dir):
    """
    Split a munged medical data file into separate files for each OMOP measurement ID.
    
    This function processes a large gzipped medical data file and creates individual
    files for each unique OMOP measurement ID. Each output file contains only the
    measurement values and their abnormality status.
    
    Args:
        munged_file (str): Path to the gzipped input file containing all measurements.
                          Expected format: tab-separated with header row
        omop_dir (str): Output directory where individual OMOP files will be created
        
    Expected Input Columns:
        - harmonization_omop::OMOP_ID: Unique identifier for measurement type (integer)
        - harmonization_omop::MEASUREMENT_VALUE: Numeric measurement value
        - TEST_OUTCOME: Abnormality status (N, L, LL, H, HH, A, AA, NA)
        
    Output:
        Creates files named {OMOP_ID}.txt in omop_dir, each containing:
        - Column 1: Measurement value
        - Column 2: Abnormality status
        
    Status Codes:
        N  = Normal
        L  = Low
        LL = Very Low
        H  = High
        HH = Very High
        A  = Abnormal (unspecified)
        AA = Very Abnormal
        NA = Not Available/Missing
        
    Notes:
        - Removes existing omop_dir if present (clean start)
        - Only processes entries where OMOP_ID > 0 and value != "NA"
        - Appends to existing OMOP files (allows incremental processing)
    """
    print(omop_dir)
    print(munged_file)
    
    # Remove all previous OMOP files to ensure clean start
    shutil.rmtree(omop_dir, ignore_errors=True)
    make_sure_path_exists(omop_dir)
    
    # Open the gzipped input file
    with gzip.open(munged_file, 'rt') as i:
        # Read header line and split into column names
        header = next(i).strip().split()
        
        # Get indices of required columns
        cols = [header.index(elem) for elem in [
            'harmonization_omop::OMOP_ID',
            'harmonization_omop::MEASUREMENT_VALUE',
            'TEST_OUTCOME'
        ]]
        print(cols)
        
        # Process each data line
        for line in i:
            line = line.strip().split()
            
            # Extract required fields using pre-computed indices
            omop, value, abnorm = [line[elem] for elem in cols]
            
            # Only write entries with valid OMOP ID and non-missing values
            if int(omop) > 0 and value != "NA":
                # Append to the appropriate OMOP file
                with open(os.path.join(omop_dir, f"{omop}.txt"), 'at') as o:
                    o.write(value + '\t' + abnorm + '\n')
    
    return


def sort_omop(abnorm_file, sorted_dir, ID):
    """
    Create sorted versions of an OMOP measurement file for efficient threshold detection.
    
    This function generates two sorted versions of the measurement file:
    1. Ascending order (low to high) - for finding lower abnormality bounds
    2. Descending order (high to low) - for finding upper abnormality bounds
    
    Sorting enables the "walking" algorithm to efficiently find values where
    abnormal results begin to cluster.
    
    Args:
        abnorm_file (str): Path to unsorted OMOP measurement file
        sorted_dir (str): Directory where sorted files will be created
        ID (str): OMOP measurement ID (used in output filenames)
        
    Returns:
        tuple: (out_file_low, out_file_high)
            out_file_low (str): Path to ascending-sorted file
            out_file_high (str): Path to descending-sorted file
            
    Output Files:
        - {ID}_low.txt: Values sorted ascending (smallest first)
        - {ID}_high.txt: Values sorted descending (largest first)
        
    Sort Commands:
        - 'sort -gk1': General numeric sort, ascending order
        - 'sort -rgk1': General numeric sort, reverse (descending) order
        - -k1: Sort by first column (the measurement value)
        
    Notes:
        - Skips sorting if output files already exist (caching)
        - Uses system 'sort' command for efficiency on large files
        - Sorts numerically, not lexicographically (handles decimals correctly)
    """
    # LOW (ascending sort)
    out_file_low = os.path.join(sorted_dir, f"{ID}_low.txt")
    if not os.path.isfile(out_file_low):
        with open(out_file_low, 'wt') as o:
            # Execute sort command: -g (general numeric), -k1 (by column 1)
            subprocess.run(shlex.split(f"sort -gk1 {abnorm_file}"), stdout=o)
    
    # HIGH (descending sort)
    out_file_high = os.path.join(sorted_dir, f"{ID}_high.txt")
    if not os.path.isfile(out_file_high):
        with open(out_file_high, 'wt') as o:
            # Execute sort command: -r (reverse), -g (general numeric), -k1 (by column 1)
            subprocess.run(shlex.split(f"sort -rgk1 {abnorm_file}"), stdout=o)
    
    return out_file_low, out_file_high


def get_high_low_percentiles(sorted_file, high_keys=['H', 'HH'], low_keys=['L', 'LL'], percentile=5):
    """
    Calculate percentile-based reference ranges from abnormal measurement values.
    
    This function provides a simple statistical approach to reference ranges by
    computing percentiles of values that are already marked as abnormal. This
    complements the more sophisticated threshold detection algorithm.
    
    Args:
        sorted_file (str): Path to file containing measurement values and statuses
        high_keys (list): Status codes indicating high abnormal values (default: ['H', 'HH'])
        low_keys (list): Status codes indicating low abnormal values (default: ['L', 'LL'])
        percentile (int): Percentile to calculate (default: 5)
        
    Returns:
        tuple: (low_percentile, high_percentile)
            low_percentile (str): Xth percentile of low abnormal values (or "NA")
            high_percentile (str): (100-X)th percentile of high abnormal values (or "NA")
            
    Example:
        If percentile=5:
        - Returns 5th percentile of all "L" and "LL" values (conservative lower bound)
        - Returns 95th percentile of all "H" and "HH" values (conservative upper bound)
        
    Interpretation:
        - Low percentile: 95% of abnormally LOW values are below this threshold
        - High percentile: 95% of abnormally HIGH values are above this threshold
        
    Notes:
        - Returns "NA" if no abnormal values found for a category
        - Assumes values can be converted to float
        - Provides a simple, interpretable alternative to ratio-based thresholds
    """
    print(sorted_file)
    
    with open(sorted_file) as i:
        high_values = []
        low_values = []
        
        # Separate values by abnormality type
        for line in i:
            value, status = line.strip().split()
            
            # Convert to float (skip if conversion fails)
            try:
                value = float(value)
            except ValueError:
                continue
            
            # Categorize by status
            if status in high_keys:
                high_values.append(value)
            elif status in low_keys:
                low_values.append(value)
        
        # Calculate percentiles (or "NA" if no values)
        # For low abnormals: use low percentile (e.g., 5th) as lower bound
        lp = np.percentile(low_values, percentile) if low_values else "NA"
        
        # For high abnormals: use high percentile (e.g., 95th) as upper bound
        hp = np.percentile(high_values, 100 - percentile) if high_values else "NA"
        
        return str(lp), str(hp)


def return_bound(sorted_file, t_hold, n_lines, numerator_keys, denominator_keys):
    """
    Determine the threshold value where abnormal results exceed a specified ratio.
    
    This is the core threshold detection algorithm. It "walks" through sorted values
    and finds the point where the ratio of abnormal to total results exceeds the
    specified threshold. This identifies where abnormal values begin to cluster.
    
    Args:
        sorted_file (str): Path to sorted measurement file (ascending or descending)
        t_hold (float): Threshold ratio (e.g., 0.9 means 90% abnormal)
        n_lines (int): Number of lines to examine from the sorted file
        numerator_keys (list): Status codes counted as abnormal (e.g., ['A', 'AA', 'L', 'LL'])
        denominator_keys (list): All relevant status codes including normal (e.g., [..., 'N'])
        
    Returns:
        str: Threshold value, optionally with "*" suffix
            - Without "*": Clear threshold where ratio > t_hold
            - With "*": Uncertain threshold (ratio never clearly exceeded)
            
    Algorithm:
        1. Read through sorted values sequentially
        2. Maintain running counts of each status type
        3. Calculate ratio: sum(numerator_keys) / sum(denominator_keys)
        4. Track the last value where ratio > threshold
        5. Return that value once ratio drops below threshold
        
    Example (finding lower bound at 90% threshold):
        Sorted values (ascending):
        Value  Status   Abnormal   Total   Ratio   Action
        2.1    LL       1          1       1.00    Save as candidate (>0.9)
        2.3    L        2          2       1.00    Save as candidate (>0.9)
        2.5    L        3          3       1.00    Save as candidate (>0.9)
        2.8    N        3          4       0.75    Stop, return 2.5 (last >0.9)
        
    Asterisk Meaning:
        "*" indicates the threshold was never clearly achieved:
        - Ratio never exceeded t_hold (insufficient abnormal values)
        - Ratio stayed above t_hold for all n_lines (gradual transition)
        - Suggests uncertainty in the threshold estimate
        
    Notes:
        - Skips "NA" values in the count
        - Adds 0.0001 to denominator to prevent division by zero
        - is_valid tracks whether we found a clear threshold crossing
    """
    with open(sorted_file) as i:
        counts = defaultdict(int)  # Track count of each status type
        
        # Initialize result tracking
        res = "NA"  # Default if no valid threshold found
        is_valid = False  # Whether we found a clear threshold crossing
        
        # Walk through the first n_lines of the sorted file
        for j in range(n_lines):
            value, status = next(i).strip().split()
            counts[status] += 1
            
            # Calculate numerator: sum of counts for abnormal status codes
            num = np.sum([counts[elem] for elem in numerator_keys])
            
            # Calculate denominator: sum of all relevant status codes
            # Add small value (0.0001) to prevent division by zero
            den = np.sum([counts[elem] for elem in denominator_keys]) + 0.0001
            
            # Only update for non-NA values
            if status != "NA":
                if den != 0:
                    if num / den > t_hold:
                        # Ratio exceeds threshold: save this value as candidate
                        res = value
                        is_valid = False  # Not valid until ratio drops below
                    else:
                        # Ratio dropped below threshold: previous value is the bound
                        is_valid = True
        
        # Return result based on validity
        if is_valid:
            # Clear threshold found: ratio exceeded then dropped
            return str(res)
        else:
            # Uncertain threshold: ratio never clearly dropped below t_hold
            # Mark with asterisk to indicate uncertainty
            return value + "*"


def count_abnorm(f):
    """
    Count the frequency of each abnormality status in a measurement file.
    
    Args:
        f (str): Path to measurement file (value\tstatus format)
        
    Returns:
        dict: Status counts sorted by frequency (descending)
        
    Example Output:
        {'N': 5000, 'H': 300, 'L': 200, 'HH': 50, 'LL': 30}
        
    Notes:
        - Useful for understanding the distribution of abnormality types
        - Helps identify if a measurement has enough abnormal values
        - Sorted by count (most frequent first) for easy interpretation
    """
    counts = defaultdict(int)
    
    with open(f) as i:
        for line in i:
            # Extract status (second column)
            status = line.strip().split()[1]
            counts[status] += 1
    
    # Sort by count (descending) and return
    return {k: v for k, v in sorted(counts.items(), key=lambda item: item[1], reverse=True)}


def abnormality(out_file, omop_dir, t_holds, max_walk, min_count, percentile, test):
    """
    Main analysis function that processes all OMOP measurements and calculates thresholds.
    
    This function orchestrates the complete threshold analysis pipeline:
    1. Loads all OMOP measurement files
    2. Filters by minimum count
    3. Sorts values for efficient processing
    4. Calculates multiple threshold estimates
    5. Computes percentile-based reference ranges
    6. Generates comprehensive results table
    
    Args:
        out_file (str): Path to output results file
        omop_dir (str): Directory containing split OMOP files
        t_holds (list): List of threshold ratios to test (e.g., [0.9, 0.95, 0.99])
        max_walk (float): Fraction of sorted data to examine (0-0.5, typically 0.5)
        min_count (int): Minimum number of measurements required to analyze
        percentile (int): Percentile for reference range calculation (typically 5)
        test (bool): If True, only process test subset of OMOP IDs
        
    Threshold Ratios:
        Multiple thresholds provide different sensitivity levels:
        - 0.9 (90%): Sensitive, catches borderline abnormals
        - 0.95 (95%): Moderate specificity
        - 0.99 (99%): Highly specific, only extreme abnormals
        
    Max Walk:
        Controls how much of sorted data to examine:
        - 0.5: Examine top/bottom 50% of values
        - Prevents outliers from affecting threshold detection
        - Focuses on the region where abnormal→normal transition occurs
        
    Test Mode:
        If test=True, only processes these OMOP IDs:
        [3008486, 3009201, 3027238, 3032333, 3023199, 3020460, 3018572]
        Useful for quick validation during development
        
    Output Format:
        Tab-separated file with columns:
        - ID: OMOP measurement identifier
        - LOWER_X: Lower threshold at X ratio (for each threshold in t_holds)
        - UPPER_X: Upper threshold at X ratio (for each threshold in t_holds)
        - LOW_P: Pth percentile of low abnormal values
        - HIGH_P: (100-P)th percentile of high abnormal values
        - ENTRIES: Total number of measurements
        - COUNTS: Dictionary of status frequencies
        
    Example Output:
        ID      LOWER_0.9  UPPER_0.9  LOWER_0.95  UPPER_0.95  LOW_5  HIGH_95  ENTRIES  COUNTS
        3001    4.2*       10.5       3.8         11.2        3.5    12.0     15000    {'N': 12000, ...}
        
    Processing Steps:
        For each OMOP measurement:
        1. Check if it has sufficient data (≥ min_count)
        2. Calculate number of lines to walk: count * max_walk
        3. Sort values ascending and descending
        4. For each threshold ratio:
           a. Find lower bound (where low abnormals cluster)
           b. Find upper bound (where high abnormals cluster)
        5. Calculate percentile-based bounds
        6. Count abnormality status frequencies
        
    Error Handling:
        - Wraps calculations in try-except blocks
        - Returns "NA" for failed calculations
        - Prints error messages for debugging
        - Continues processing other measurements on errors
        
    Notes:
        - Results are sorted by entry count (most data first)
        - Creates sorted_dir subdirectory for cached sorted files
        - Asterisks (*) in results indicate uncertain thresholds
    """
    # Get all OMOP measurement files
    paths = [entry.path for entry in os.scandir(omop_dir) if entry.is_file()]
    
    # Create directory for sorted files
    sorted_dir = os.path.join(omop_dir, 'sorted')
    make_sure_path_exists(sorted_dir)
    
    results = []  # Store all results for final output
    
    # Extract OMOP IDs from filenames
    IDS = [(Path(f).stem, f) for f in paths]
    
    # If in test mode, only process specific OMOP IDs
    if args.test:
        IDS = [elem for elem in IDS if elem[0] in 
               ['3008486', '3009201', '3027238', '3032333', '3023199', '3020460', '3018572']]
    
    # Process each OMOP measurement
    for i, elem in enumerate(IDS):
        ID, omop_file = elem
        
        # Skip measurements with insufficient data
        count = count_lines(omop_file)
        if count < min_count:
            continue
        
        # Calculate how many lines to examine (top/bottom max_walk fraction)
        lines = int(count * max_walk)
        
        # Create sorted versions of the file
        out_file_low, out_file_high = sort_omop(omop_file, sorted_dir, ID)
        
        print(ID, count, f"{i+1}/{len(IDS)}")
        
        # --- Calculate LOWER bounds ---
        # For lower bounds, we want to find where LOW abnormals cluster
        try:
            # Numerator: abnormal low values (A, AA, L, LL)
            num_keys = ['A', 'AA', 'L', 'LL']
            # Denominator: all relevant values (abnormal + normal)
            den_keys = num_keys + ['N', 'H', 'HH']
            
            # Calculate lower bound for each threshold
            low_estimates = [
                return_bound(out_file_low, t_hold, lines, num_keys, den_keys)
                for t_hold in t_holds
            ]
        except Exception as e:
            # On error, set all lower bounds to NA
            low_estimates = ["NA" for elem in t_holds]
            print(f"problems with {ID} low: {e}")
        
        # --- Calculate UPPER bounds ---
        # For upper bounds, we want to find where HIGH abnormals cluster
        try:
            # Numerator: abnormal high values (A, AA, H, HH)
            num_keys = ['A', 'AA', 'H', 'HH']
            # Denominator: all relevant values (abnormal + normal)
            den_keys = num_keys + ['N', 'L', 'LL']
            
            # Calculate upper bound for each threshold
            high_estimates = [
                return_bound(out_file_high, t_hold, lines, num_keys, den_keys)
                for t_hold in t_holds
            ]
        except Exception as e:
            # On error, set all upper bounds to NA
            high_estimates = ["NA" for elem in t_holds]
            print(f"problems with {ID} high: {e}")
        
        # Count abnormality status frequencies
        counts = count_abnorm(out_file_low)
        
        # Calculate percentile-based reference ranges
        low_percentile, high_percentile = get_high_low_percentiles(
            omop_file, percentile=percentile
        )
        
        # Print details if in test mode
        if args.test:
            print(ID, count, low_estimates, high_estimates, low_percentile, high_percentile)
        
        # Interleave low and high estimates for each threshold
        # [LOWER_0.9, UPPER_0.9, LOWER_0.95, UPPER_0.95, ...]
        interleaved_estimates = [x for z in zip(low_estimates, high_estimates) for x in z]
        
        # Compile complete result row
        results.append([
            ID,
            *interleaved_estimates,
            low_percentile,
            high_percentile,
            count,
            str(dict(counts))
        ])
    
    # Write results to output file
    with open(out_file, 'wt') as o:
        # Construct header
        header = ['ID']
        for t_hold in t_holds:
            header += [f"LOWER_{t_hold}", f"UPPER_{t_hold}"]
        header += [f"LOW_{percentile}", f"HIGH_{100-percentile}", 'ENTRIES', 'COUNTS']
        
        o.write('\t'.join(header) + '\n')
        
        # Write results sorted by entry count (descending)
        # Index -2 is the count column (before COUNTS dictionary)
        for res in sorted(results, key=itemgetter(-2), reverse=True):
            o.write('\t'.join(map(str, res)) + '\n')
    
    print('\nDone')
    return


def main(args):
    """
    Main entry point for the abnormality threshold analysis pipeline.
    
    This function coordinates the two-phase analysis process:
    
    Phase 1 (--split flag):
        - Reads the large munged input file
        - Splits it into individual files per OMOP measurement ID
        - Run this once to prepare data for analysis
        
    Phase 2 (default):
        - Processes the split OMOP files
        - Calculates threshold estimates and reference ranges
        - Generates comprehensive results table
        
    Args:
        args: Parsed command-line arguments containing:
            - kanta_file: Path to input munged data file
            - out: Output directory
            - split: Whether to run split phase
            - thresholds: List of threshold ratios
            - max_walk: Fraction of data to examine
            - min_count: Minimum measurements required
            - percentile: Percentile for reference ranges
            - test: Test mode flag
            
    Workflow:
        # First run (split data)
        python script.py --split
        
        # Subsequent runs (analyze)
        python script.py --thresholds 0.9 0.95 0.99
        
    Notes:
        - Split phase only needs to run once per input file
        - Analysis phase can be run multiple times with different parameters
        - Output directory is created if it doesn't exist
    """
    omop_dir = os.path.join(args.out, 'omop_files/')
    
    if args.split:
        # Phase 1: Split input file by OMOP ID
        split_input(args.kanta_file, omop_dir)
    else:
        # Phase 2: Analyze OMOP files and calculate thresholds
        out_file = os.path.join(args.out, f'abnormality_estimation.txt')
        abnormality(
            out_file,
            omop_dir,
            args.thresholds,
            args.max_walk,
            args.min_count,
            args.percentile,
            args.test
        )
    
    return


if __name__ == "__main__":
    # Set up command-line argument parser
    parser = argparse.ArgumentParser(
        description='Analyze medical laboratory measurements to determine abnormality thresholds',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Split input file (run once)
  %(prog)s --split
  
  # Analyze with default settings
  %(prog)s
  
  # Analyze with custom thresholds
  %(prog)s --thresholds 0.85 0.90 0.95 0.99
  
  # Test run on subset
  %(prog)s --test
  
  # Full custom analysis
  %(prog)s --min-count 2000 --percentile 10 --thresholds 0.9 0.95
        """
    )
    
    # Input/Output arguments
    parser.add_argument(
        '--kanta_file',
        default='/home/pete/fg-3/kanta/munged/kanta_2024_08_09_munged.txt.gz',
        help='Path to input munged data file (gzipped, tab-separated)'
    )
    parser.add_argument(
        '--out',
        default="/mnt/disks/data/kanta/abnorm/",
        help='Output directory for results and intermediate files'
    )
    
    # Analysis parameters
    parser.add_argument(
        '--min-count',
        default=1000,
        type=int,
        help='Minimum number of measurements required to analyze an OMOP ID (default: 1000)'
    )
    parser.add_argument(
        '--percentile',
        default=5,
        type=int,
        help='Percentile for reference range calculation (default: 5 for 5th/95th percentiles)'
    )
    parser.add_argument(
        '--max-walk',
        default=.5,
        type=float,
        help='Fraction of sorted data to examine (0-0.5, default: 0.5 for top/bottom 50%%)'
    )
    parser.add_argument(
        '--thresholds',
        default=[.9, .95, .99],
        nargs='*',
        type=float,
        help='Threshold ratios to test (default: 0.9 0.95 0.99)'
    )
    
    # Mode flags
    parser.add_argument(
        "--split",
        action='store_true',
        help="Split input file by OMOP ID (run once before analysis)"
    )
    parser.add_argument(
        "--test",
        action='store_true',
        help="Test mode: only process subset of OMOP IDs"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    make_sure_path_exists(args.out)
    assert 0 < args.max_walk <= .5, "max_walk must be between 0 and 0.5"
    
    print(f"Thresholds to test: {args.thresholds}")
    
    # Run main analysis
    main(args)
