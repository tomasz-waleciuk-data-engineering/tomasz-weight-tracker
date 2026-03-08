import os
import pandas as pd
from collections import defaultdict

# --- Configuration ---
# The name of the folder containing your CSV files.
# The script assumes this folder is in the same directory as the script.
FOLDER_NAME = './sensors_csv_source' 
# The name of the output report file.
OUTPUT_FILE = 'granularity_report.txt'
# --- End of Configuration ---


def find_datetime_column(df):
    """
    Analyzes a DataFrame's columns to find the one most likely to contain timestamps.
    It prioritizes columns with common date/time names.
    """
    # Look for columns with common timestamp names first
    common_names = ['timestamp', 'datetime', 'date', 'time', 'created_at', 'updated_at']
    for col in df.columns:
        if col.lower() in common_names:
            return col
            
    # If no common name is found, try to infer by data type
    for col in df.columns:
        # Attempt to convert a sample of the column to datetime
        # If successful for a high percentage, assume it's the right one
        try:
            pd.to_datetime(df[col].head(), errors='coerce')
            return col
        except (ValueError, TypeError):
            continue
            
    return None # Return None if no suitable column is found

def get_file_granularity(file_path):
    """
    Reads a CSV file and determines its most frequent time granularity.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Handle empty or single-row files
        if len(df) < 2:
            return "single_row_or_empty"

        # Find the column with timestamps
        time_col = find_datetime_column(df)
        if not time_col:
            return "no_datetime_column_found"
        
        # Convert the column to datetime objects, coercing errors to NaT (Not a Time)
        # This is robust against malformed date strings
        df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
        
        # Drop rows where conversion failed
        df.dropna(subset=[time_col], inplace=True)
        
        if len(df) < 2:
            return "not_enough_valid_dates"

        # Calculate the difference between consecutive timestamps
        # The .mode()[0] gets the most frequently occurring time difference
        time_diff = df[time_col].diff().mode()
        
        if not time_diff.empty:
            return time_diff[0]
        else:
            return "could_not_determine"

    except pd.errors.EmptyDataError:
        return "empty_file"
    except Exception as e:
        # Catch any other exceptions during file processing
        # print(f"  - Warning for {os.path.basename(file_path)}: {e}")
        return f"error: {e}"


def main():
    """
    Main function to orchestrate the folder scan and report generation.
    """
    if not os.path.isdir(FOLDER_NAME):
        print(f"Error: Folder '{FOLDER_NAME}' not found. Please make sure it's in the same directory as the script.")
        return

    print(f"Scanning folder '{FOLDER_NAME}' for CSV files...")
    
    # Find all .csv files, ignoring case
    try:
        all_files = os.listdir(FOLDER_NAME)
        csv_files = [f for f in all_files if f.lower().endswith('.csv')]
    except Exception as e:
        print(f"Error reading directory '{FOLDER_NAME}': {e}")
        return

    if not csv_files:
        print("No CSV files found in the folder.")
        return

    print(f"Found {len(csv_files)} CSV files. Analyzing granularity...")

    # Use a defaultdict to easily group filenames by granularity
    granularity_map = defaultdict(list)
    total_files = len(csv_files)

    for i, filename in enumerate(csv_files, 1):
        file_path = os.path.join(FOLDER_NAME, filename)
        print(f"Processing ({i}/{total_files}): {filename}")
        
        granularity = get_file_granularity(file_path)
        
        # Convert pandas Timedelta to a more readable string key
        key = str(granularity)
        granularity_map[key].append(filename)

    print("\nAnalysis complete. Writing report to", OUTPUT_FILE)

    # Write the results to the output file
    with open(OUTPUT_FILE, 'w') as f:
        f.write("Granularity Analysis Report\n")
        f.write("=============================\n\n")

        # Sort the granularities for a consistent output
        sorted_granularities = sorted(granularity_map.keys())

        for granularity_key in sorted_granularities:
            files = granularity_map[granularity_key]
            # Try to create a more human-readable title
            try:
                td = pd.to_timedelta(granularity_key)
                title = f"FILES with {td.total_seconds() / 60:.0f}-minute granularity ({td}):"
            except ValueError:
                title = f"FILES IN CATEGORY '{granularity_key.upper()}':"

            f.write(f"{title}\n")
            for filename in sorted(files):
                f.write(f"  {filename}\n")
            f.write("\n")

    print(f"\nDone! Report saved as '{OUTPUT_FILE}'.")

if __name__ == "__main__":
    main()

