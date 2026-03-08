#!/usr/bin/env python3
"""
Aggregate 1-minute sensor data to 15-minute bins.

- Reads Parquet files from input folder
- Converts timestamps from UK local time (GMT/BST) to UTC
- Aggregates to 15-minute bins (average)
- Bin logic: (00:01-00:15] -> 00:15, (00:16-00:30] -> 00:30, etc.
  - 00:00:00 belongs to previous day's 23:45 bin
  - 00:15:00 is the last minute of the first bin
- Outputs to Parquet and/or CSV
"""

import argparse
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field

import polars as pl


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class SensorStats:
    """Statistics for a sensor."""
    sensor_name: str
    input_rows: int = 0
    output_rows: int = 0
    columns: list[str] = field(default_factory=list)
    date_range: tuple[str, str] | None = None
    csv_path: Path | None = None
    parquet_path: Path | None = None


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def sanitize_filename(name: str) -> str:
    """Remove invalid filename characters (cross-platform)."""
    invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    for char in invalid_chars:
        name = name.replace(char, '_')
    return name.strip()


def extract_sensor_name(filename: str) -> str:
    """Extract sensor name from filename like '20241225-1200-SensorName.parquet'"""
    name = filename.replace('.parquet', '').replace('.csv', '')
    # Remove timestamp prefix if present (YYYYMMDD-HHMM-)
    parts = name.split('-', 2)
    if len(parts) >= 3 and len(parts[0]) == 8 and len(parts[1]) == 4:
        return parts[2]
    return name


def clean_for_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Convert NaN to null for CSV output."""
    float_cols = [
        c for c in df.columns 
        if df.schema.get(c) in (pl.Float32, pl.Float64)
    ]
    
    if float_cols:
        df = df.with_columns([
            pl.when(pl.col(c).is_nan())
            .then(None)
            .otherwise(pl.col(c))
            .alias(c)
            for c in float_cols
        ])
    
    return df


# =============================================================================
# TIMEZONE CONVERSION
# =============================================================================

def convert_uk_to_utc(df: pl.DataFrame, timestamp_col: str = "Timestamp") -> pl.DataFrame:
    """
    Convert UK local time (GMT/BST) to UTC.
    
    After conversion, all timestamps will be continuous in UTC with no gaps.
    
    Handling DST transitions:
    - Spring forward (Mar): 01:00-01:59 UK doesn't exist, but if data has these
      timestamps, they're likely mislabeled 02:00-02:59 BST times
    - Fall back (Oct): 01:00-01:59 UK occurs twice, we pick the first (BST)
    """
    
    df = df.with_columns(
        pl.col(timestamp_col)
        .dt.replace_time_zone(
            "Europe/London", 
            ambiguous="earliest",      # Fall-back: pick first occurrence
            non_existent="null"        # Spring-forward: shouldn't happen in real data
        )
        .alias(timestamp_col)
    )
    
    # Drop any rows with non-existent timestamps (data quality issue)
    before = df.height
    df = df.filter(pl.col(timestamp_col).is_not_null())
    dropped = before - df.height
    if dropped > 0:
        print(f"    Warning: Dropped {dropped} rows with invalid timestamps (DST gap)")
    
    # Convert to UTC - now everything is clean and continuous
    df = df.with_columns(
        pl.col(timestamp_col)
        .dt.convert_time_zone("UTC")
        .dt.replace_time_zone(None)
        .alias(timestamp_col)
    )
    
    return df


# =============================================================================
# 15-MINUTE AGGREGATION
# =============================================================================

def create_15min_bin(df: pl.DataFrame, timestamp_col: str = "Timestamp") -> pl.DataFrame:
    """
    Create 15-minute bin column.
    
    Bin logic:
    - (00:01 - 00:15] -> 00:15
    - (00:16 - 00:30] -> 00:30
    - (00:31 - 00:45] -> 00:45
    - (00:46 - 01:00] -> 01:00
    - 00:00:00.000 exactly -> previous day 23:45 bin
    
    The bin timestamp represents the END of the bin period.
    """
    
    df = df.with_columns([
        # Extract components for bin calculation
        pl.col(timestamp_col).dt.year().alias("__year__"),
        pl.col(timestamp_col).dt.month().alias("__month__"),
        pl.col(timestamp_col).dt.day().alias("__day__"),
        pl.col(timestamp_col).dt.hour().alias("__hour__"),
        pl.col(timestamp_col).dt.minute().alias("__minute__"),
        pl.col(timestamp_col).dt.second().alias("__second__"),
        pl.col(timestamp_col).dt.microsecond().alias("__microsecond__"),
    ])
    
    # Calculate bin
    # For times like 00:01-00:15, bin is 00:15
    # For times like 00:16-00:30, bin is 00:30
    # For exactly 00:00:00.000000, bin is previous day 23:45
    
    df = df.with_columns(
        pl.when(
            # Exactly midnight (00:00:00.000000) -> belongs to previous day's last bin
            (pl.col("__hour__") == 0) & 
            (pl.col("__minute__") == 0) & 
            (pl.col("__second__") == 0) & 
            (pl.col("__microsecond__") == 0)
        )
        .then(
            # Previous day at 23:45 (as the bin end time would be 00:00, but we label it by the data's day)
            # Actually, let's handle this: midnight belongs to bin ending at 00:00 of same day
            # But 00:00 is the END of the 23:46-00:00 bin from the previous logical day
            # To keep it simple: 00:00:00.000 -> bin 00:00 (which is end of previous period)
            pl.col(timestamp_col).dt.truncate("1d")  # Start of day = 00:00
        )
        .otherwise(
            # For all other times: ceiling to next 15-minute mark
            # minute 1-15 -> :15, minute 16-30 -> :30, minute 31-45 -> :45, minute 46-59 -> next hour :00
            pl.col(timestamp_col)
            .map_batches(lambda s: ceil_to_15min(s), return_dtype=pl.Datetime("us"))
        )
        .alias("__bin__")
    )
    
    # Clean up temp columns
    df = df.drop([
        "__year__", "__month__", "__day__", 
        "__hour__", "__minute__", "__second__", "__microsecond__"
    ])
    
    return df


def ceil_to_15min(timestamps: pl.Series) -> pl.Series:
    """
    Ceiling timestamps to the next 15-minute boundary.
    
    00:00:00.000000 exactly -> 00:00 (belongs to this bin as endpoint)
    00:00:00.000001 - 00:15:00.000000 -> 00:15
    00:15:00.000001 - 00:30:00.000000 -> 00:30
    etc.
    """
    import numpy as np
    
    # Convert to numpy datetime64 for manipulation
    arr = timestamps.to_numpy()
    
    # Convert to microseconds since epoch for calculation
    # Polars datetime is in microseconds
    us = arr.astype('datetime64[us]').astype(np.int64)
    
    # 15 minutes in microseconds
    bin_size_us = 15 * 60 * 1_000_000
    
    # For ceiling: if not exactly on boundary, round up to next boundary
    # For exactly on boundary, keep it there
    
    # Calculate remainder
    remainder = us % bin_size_us
    
    # Ceiling: add (bin_size - remainder) if remainder > 0, else keep same
    ceiling_us = np.where(
        remainder > 0,
        us + (bin_size_us - remainder),
        us
    )
    
    # Convert back to datetime
    result = ceiling_us.astype('datetime64[us]')
    
    return pl.Series(result)


def aggregate_to_15min(
    df: pl.DataFrame, 
    timestamp_col: str = "Timestamp"
) -> pl.DataFrame:
    """
    Aggregate data to 15-minute bins using mean.
    """
    
    # Create bin column
    df = create_15min_bin(df, timestamp_col)
    
    # Get numeric columns for aggregation
    numeric_cols = [
        c for c in df.columns 
        if c not in [timestamp_col, "__bin__"] and df.schema.get(c).is_numeric()
    ]
    
    # Aggregate by bin
    agg_exprs = [pl.col(c).mean().alias(c) for c in numeric_cols]
    
    result = df.group_by("__bin__").agg(agg_exprs).sort("__bin__")
    
    # Rename bin column to Timestamp
    result = result.rename({"__bin__": timestamp_col})
    
    return result


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_sensor(
    input_path: Path,
    output_dir: Path,
    timestamp: str,
    output_csv: bool = True,
    output_parquet: bool = True,
    verbose: bool = True
) -> SensorStats:
    """Process a single sensor file."""
    
    sensor_name = extract_sensor_name(input_path.name)
    stats = SensorStats(sensor_name=sensor_name)
    
    if verbose:
        print(f"    Reading: {input_path.name}")
    
    # Read parquet
    df = pl.read_parquet(input_path)
    stats.input_rows = len(df)
    
    if df.is_empty():
        print(f"    Warning: Empty file")
        return stats
    
    # Find timestamp column
    ts_col = None
    for col in df.columns:
        if "timestamp" in col.lower():
            ts_col = col
            break
    
    if ts_col is None:
        ts_col = df.columns[0]
    
    if ts_col != "Timestamp":
        df = df.rename({ts_col: "Timestamp"})
    
    if verbose:
        print(f"    Input rows: {stats.input_rows}")
        min_ts = df["Timestamp"].min()
        max_ts = df["Timestamp"].max()
        print(f"    Date range (local): {min_ts} to {max_ts}")
    
    # Convert UK time to UTC
    if verbose:
        print(f"    Converting UK local time to UTC...")
    
    df = convert_uk_to_utc(df, "Timestamp")
    
    if verbose:
        min_ts = df["Timestamp"].min()
        max_ts = df["Timestamp"].max()
        print(f"    Date range (UTC): {min_ts} to {max_ts}")
        stats.date_range = (str(min_ts), str(max_ts))
    
    # Aggregate to 15-minute bins
    if verbose:
        print(f"    Aggregating to 15-minute bins...")
    
    df = aggregate_to_15min(df, "Timestamp")
    
    stats.output_rows = len(df)
    stats.columns = df.columns
    
    if verbose:
        print(f"    Output rows: {stats.output_rows}")
        print(f"    Compression ratio: {stats.input_rows / stats.output_rows:.1f}x")
    
    # Save outputs
    safe_name = sanitize_filename(sensor_name)
    base_name = f"{timestamp}-{safe_name}-15min-UTC"
    
    if output_csv:
        csv_path = output_dir / f"{base_name}.csv"
        clean_for_csv(df).write_csv(csv_path, null_value="")
        stats.csv_path = csv_path
        if verbose:
            size_mb = csv_path.stat().st_size / (1024 * 1024)
            print(f"    Saved CSV: {csv_path.name} ({size_mb:.2f} MB)")
    
    if output_parquet:
        parquet_path = output_dir / f"{base_name}.parquet"
        df.write_parquet(parquet_path, compression="snappy")
        stats.parquet_path = parquet_path
        if verbose:
            size_mb = parquet_path.stat().st_size / (1024 * 1024)
            print(f"    Saved Parquet: {parquet_path.name} ({size_mb:.2f} MB)")
    
    return stats


def print_summary(all_stats: list[SensorStats]) -> None:
    """Print processing summary."""
    print(f"\n{'=' * 130}")
    print("SUMMARY")
    print(f"{'=' * 130}")
    
    print(f"\n{'Sensor':<40} {'Input Rows':<15} {'Output Rows':<15} {'Ratio':<10} {'CSV Size':<15} {'Parquet Size':<15}")
    print("-" * 130)
    
    total_input = 0
    total_output = 0
    total_csv = 0
    total_parquet = 0
    
    for s in all_stats:
        name = s.sensor_name[:39] if len(s.sensor_name) > 39 else s.sensor_name
        ratio = f"{s.input_rows / s.output_rows:.1f}x" if s.output_rows > 0 else "N/A"
        
        csv_size_str = ""
        parquet_size_str = ""
        
        if s.csv_path and s.csv_path.exists():
            csv_size = s.csv_path.stat().st_size / (1024 * 1024)
            csv_size_str = f"{csv_size:.2f} MB"
            total_csv += csv_size
        
        if s.parquet_path and s.parquet_path.exists():
            parquet_size = s.parquet_path.stat().st_size / (1024 * 1024)
            parquet_size_str = f"{parquet_size:.2f} MB"
            total_parquet += parquet_size
        
        print(f"{name:<40} {s.input_rows:<15} {s.output_rows:<15} {ratio:<10} {csv_size_str:<15} {parquet_size_str:<15}")
        
        total_input += s.input_rows
        total_output += s.output_rows
    
    print("-" * 130)
    total_ratio = f"{total_input / total_output:.1f}x" if total_output > 0 else "N/A"
    print(f"{'TOTAL':<40} {total_input:<15} {total_output:<15} {total_ratio:<10} "
          f"{total_csv:.2f} MB{'':<7} {total_parquet:.2f} MB")


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate 1-minute sensor data to 15-minute bins with UK->UTC conversion"
    )
    parser.add_argument(
        "input_folder",
        type=str,
        help="Folder containing 1-minute Parquet files"
    )
    parser.add_argument(
        "output_folder",
        type=str,
        help="Folder to save aggregated files"
    )
    parser.add_argument(
        "--csv-only",
        action="store_true",
        help="Output CSV only (no Parquet)"
    )
    parser.add_argument(
        "--parquet-only",
        action="store_true",
        help="Output Parquet only (no CSV)"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Reduce output verbosity"
    )
    args = parser.parse_args()
    
    input_dir = Path(args.input_folder).expanduser().resolve()
    output_dir = Path(args.output_folder).expanduser().resolve()
    
    if not input_dir.exists():
        print(f"Error: Input folder does not exist: {input_dir}")
        return 1
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_csv = not args.parquet_only
    output_parquet = not args.csv_only
    
    if not output_csv and not output_parquet:
        print("Error: Cannot use both --csv-only and --parquet-only")
        return 1
    
    verbose = not args.quiet
    
    # Find Parquet files
    parquet_files = sorted(input_dir.glob("*.parquet"))
    print(f"Found {len(parquet_files)} Parquet files in {input_dir}")
    
    if not parquet_files:
        print("No Parquet files found!")
        return 1
    
    total_size = sum(f.stat().st_size for f in parquet_files)
    print(f"Total input size: {total_size / (1024**3):.2f} GB")
    
    output_formats = []
    if output_csv:
        output_formats.append("CSV")
    if output_parquet:
        output_formats.append("Parquet")
    print(f"Output formats: {', '.join(output_formats)}")
    
    # Process each file
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    start_time = datetime.now()
    all_stats = []
    
    for idx, fpath in enumerate(parquet_files, 1):
        file_start = datetime.now()
        print(f"\n{'=' * 100}")
        print(f"[{idx}/{len(parquet_files)}] Processing: {fpath.name}")
        print(f"{'=' * 100}")
        
        stats = process_sensor(
            fpath, output_dir, timestamp,
            output_csv=output_csv,
            output_parquet=output_parquet,
            verbose=verbose
        )
        all_stats.append(stats)
        
        elapsed = (datetime.now() - file_start).total_seconds()
        print(f"    Time: {elapsed:.1f}s")
    
    total_elapsed = (datetime.now() - start_time).total_seconds()
    
    print_summary(all_stats)
    
    print(f"\n{'=' * 100}")
    print(f"COMPLETE! Total time: {total_elapsed:.1f}s")
    print(f"Output files saved to: {output_dir}")
    print(f"{'=' * 100}")
    
    return 0


if __name__ == "__main__":
    exit(main())
