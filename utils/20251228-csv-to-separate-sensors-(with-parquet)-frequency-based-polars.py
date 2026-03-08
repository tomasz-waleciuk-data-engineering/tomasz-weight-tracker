#!/usr/bin/env python3
"""
Sensor CSV Processing Pipeline (Polars) - SMART MERGE

For each sensor:
1. Read all CSV files
2. For each (timestamp, column):
   - Default: use most frequent value
   - But if alternative value fits local trend better, use that instead
3. Output clean CSV and Parquet: Timestamp + data columns only (no conflict columns)
"""

import argparse
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field

import polars as pl
import numpy as np


# =============================================================================
# CONSTANTS & PATTERNS
# =============================================================================

SENSOR_NAME_RE = re.compile(
    r"^(?P<sensor>.+?)_export_\d{12}.*\.csv$",
    re.IGNORECASE
)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ColumnStats:
    """Statistics for a column's merge process."""
    column_name: str
    total_timestamps: int = 0
    timestamps_with_conflicts: int = 0
    smoothness_swaps: int = 0


@dataclass
class SensorStats:
    """Statistics for a sensor."""
    sensor_name: str
    rows: int = 0
    columns: list[str] = field(default_factory=list)
    column_stats: dict[str, ColumnStats] = field(default_factory=dict)
    csv_path: Path | None = None
    parquet_path: Path | None = None


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_sensor_name(filename: str) -> str | None:
    m = SENSOR_NAME_RE.match(filename)
    if m:
        return m.group("sensor").strip()
    return None


def get_sort_key(path: Path) -> tuple[str, str]:
    m = re.search(r"_export_(\d{12})", path.name)
    ts = m.group(1) if m else "999999999999"
    return (ts, path.name)


def find_timestamp_column(columns: list[str]) -> str:
    for col in columns:
        if "timestamp" in col.lower():
            return col
    return columns[0] if columns else "Timestamp"


def is_numeric_dtype(dtype: pl.DataType) -> bool:
    return dtype.is_numeric()


def sanitize_filename(name: str) -> str:
    for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']:
        name = name.replace(char, '_')
    return name.strip()


def clean_for_output(df: pl.DataFrame) -> pl.DataFrame:
    """Convert NaN to null, remove empty columns."""
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
    
    non_empty_cols = [
        col for col in df.columns 
        if df[col].null_count() < len(df)
    ]
    
    return df.select(non_empty_cols)


# =============================================================================
# READ CSV
# =============================================================================

def read_csv_normalized(path: Path) -> pl.DataFrame | None:
    """Read CSV and normalize timestamp column."""
    try:
        df = pl.read_csv(
            path, 
            infer_schema_length=10000,
            null_values=["", "NaN", "nan", "NULL", "null", "NA", "na", "N/A", "n/a"]
        )
    except Exception as e:
        print(f"      Warning: Could not read {path.name}: {e}")
        return None
    
    if df.is_empty() or len(df.columns) == 0:
        return None
    
    ts_col = find_timestamp_column(df.columns)
    if ts_col != "Timestamp":
        df = df.rename({ts_col: "Timestamp"})
    
    try:
        df = df.with_columns(
            pl.col("Timestamp").str.to_datetime(strict=False)
        )
    except Exception:
        try:
            df = df.with_columns(
                pl.col("Timestamp").cast(pl.Datetime)
            )
        except Exception as e:
            print(f"      Warning: Could not parse timestamps: {e}")
            return None
    
    df = df.filter(pl.col("Timestamp").is_not_null())
    
    if df.is_empty():
        return None
    
    return df.sort("Timestamp")


# =============================================================================
# SMOOTHNESS ANALYSIS
# =============================================================================

def calculate_local_expected(
    values: np.ndarray,
    idx: int,
    window: int = 5
) -> float | None:
    """
    Calculate expected value at index based on neighbors.
    Uses linear interpolation from nearest valid neighbors.
    """
    n = len(values)
    
    left_idx = None
    left_val = None
    for i in range(idx - 1, max(idx - window - 1, -1), -1):
        if i >= 0 and not np.isnan(values[i]):
            left_idx = i
            left_val = values[i]
            break
    
    right_idx = None
    right_val = None
    for i in range(idx + 1, min(idx + window + 1, n)):
        if not np.isnan(values[i]):
            right_idx = i
            right_val = values[i]
            break
    
    if left_idx is not None and right_idx is not None:
        t = (idx - left_idx) / (right_idx - left_idx)
        return left_val + t * (right_val - left_val)
    elif left_idx is not None:
        return left_val
    elif right_idx is not None:
        return right_val
    
    return None


def calculate_smoothness_score(
    values: np.ndarray,
    idx: int,
    value: float,
    window: int = 5
) -> float:
    """
    Calculate how well a value fits with its neighbors.
    Lower score = better fit (smoother).
    """
    n = len(values)
    
    neighbors = []
    for i in range(max(0, idx - window), min(n, idx + window + 1)):
        if i != idx and not np.isnan(values[i]):
            neighbors.append(values[i])
    
    if len(neighbors) < 2:
        return 0.0
    
    neighbors = np.array(neighbors)
    local_mean = np.mean(neighbors)
    local_std = np.std(neighbors)
    
    if local_std > 1e-9:
        deviation_score = abs(value - local_mean) / local_std
    else:
        deviation_score = abs(value - local_mean)
    
    left_val = None
    right_val = None
    
    for i in range(idx - 1, -1, -1):
        if not np.isnan(values[i]):
            left_val = values[i]
            break
    
    for i in range(idx + 1, n):
        if not np.isnan(values[i]):
            right_val = values[i]
            break
    
    curvature_score = 0.0
    if left_val is not None and right_val is not None:
        expected_mid = (left_val + right_val) / 2
        curvature_score = abs(value - expected_mid)
        if local_std > 1e-9:
            curvature_score /= local_std
    
    return 0.5 * deviation_score + 0.5 * curvature_score


def optimize_for_smoothness(
    base_values: np.ndarray,
    alt_values: np.ndarray,
    min_improvement_ratio: float = 1.3,
    window: int = 5
) -> tuple[np.ndarray, int]:
    """
    For each position where we have an alternative value,
    check if using it would be smoother. If yes, swap.
    
    Returns: (optimized_values, number_of_swaps)
    """
    result = base_values.copy()
    swap_count = 0
    
    conflict_indices = np.where(
        ~np.isnan(base_values) & ~np.isnan(alt_values)
    )[0]
    
    for idx in conflict_indices:
        base_val = result[idx]
        alt_val = alt_values[idx]
        
        if abs(base_val - alt_val) < 1e-6:
            continue
        
        expected = calculate_local_expected(result, idx, window)
        
        if expected is None:
            continue
        
        base_score = calculate_smoothness_score(result, idx, base_val, window)
        alt_score = calculate_smoothness_score(result, idx, alt_val, window)
        
        base_deviation = abs(base_val - expected)
        alt_deviation = abs(alt_val - expected)
        
        base_total = 0.5 * base_score + 0.5 * base_deviation
        alt_total = 0.5 * alt_score + 0.5 * alt_deviation
        
        if alt_total > 0 and base_total / alt_total > min_improvement_ratio:
            result[idx] = alt_val
            swap_count += 1
    
    return result, swap_count


# =============================================================================
# MERGE BY FREQUENCY + SMOOTHNESS
# =============================================================================

def merge_sensor_files(
    files: list[Path], 
    min_improvement_ratio: float = 1.3,
    verbose: bool = True
) -> tuple[pl.DataFrame, dict[str, ColumnStats]]:
    """
    Merge all CSV files for a sensor.
    
    1. Get most frequent value for each (timestamp, column) -> base
    2. Get 2nd most frequent value -> alternative
    3. For conflicts, check if alternative is smoother
    4. Return single clean dataframe with best values
    """
    files = sorted(files, key=get_sort_key)
    
    all_dfs: list[pl.DataFrame] = []
    for i, fpath in enumerate(files):
        if verbose:
            print(f"      [{i+1}/{len(files)}] Reading: {fpath.name}")
        
        df = read_csv_normalized(fpath)
        if df is not None and not df.is_empty():
            all_dfs.append(df)
            if verbose:
                print(f"        {len(df)} rows, {len(df.columns)} cols")
        else:
            if verbose:
                print(f"        Skipped (empty/invalid)")
    
    if not all_dfs:
        return pl.DataFrame({"Timestamp": []}), {}
    
    all_timestamps = pl.concat(
        [df.select("Timestamp") for df in all_dfs]
    ).unique().sort("Timestamp")
    
    all_data_columns: set[str] = set()
    for df in all_dfs:
        all_data_columns.update(c for c in df.columns if c != "Timestamp")
    
    if verbose:
        print(f"      Total unique timestamps: {len(all_timestamps)}")
        print(f"      Data columns: {sorted(all_data_columns)}")
    
    result = all_timestamps
    all_column_stats: dict[str, ColumnStats] = {}
    
    for col in sorted(all_data_columns):
        if verbose:
            print(f"      Processing column: {col}")
        
        col_stats = ColumnStats(column_name=col)
        
        col_data_list: list[pl.DataFrame] = []
        for df in all_dfs:
            if col in df.columns:
                subset = df.select(["Timestamp", col]).filter(pl.col(col).is_not_null())
                if not subset.is_empty():
                    col_data_list.append(subset)
        
        if not col_data_list:
            continue
        
        col_data = pl.concat(col_data_list)
        
        col_dtype = col_data.schema.get(col)
        is_numeric = col_dtype is not None and is_numeric_dtype(col_dtype)
        
        if is_numeric:
            col_data = col_data.with_columns(
                pl.col(col).round(6).alias(col)
            )
        
        freq_counts = col_data.group_by(["Timestamp", col]).len().rename({"len": "__freq__"})
        
        freq_counts = freq_counts.with_columns(
            pl.col("__freq__")
            .rank(method="ordinal", descending=True)
            .over("Timestamp")
            .alias("__rank__")
        )
        
        max_rank = int(freq_counts.select(pl.col("__rank__").max()).item() or 1)
        
        col_stats.total_timestamps = len(all_timestamps)
        
        base_values = freq_counts.filter(pl.col("__rank__") == 1).select(["Timestamp", col])
        base_values = base_values.unique(subset=["Timestamp"], keep="first")
        
        base_df = all_timestamps.join(base_values, on="Timestamp", how="left")
        
        if max_rank > 1 and is_numeric:
            alt_values = freq_counts.filter(pl.col("__rank__") == 2).select(["Timestamp", col])
            alt_values = alt_values.unique(subset=["Timestamp"], keep="first")
            alt_df = all_timestamps.join(
                alt_values.rename({col: f"{col}__alt__"}), 
                on="Timestamp", 
                how="left"
            )
            
            conflicts = freq_counts.group_by("Timestamp").agg(
                pl.len().alias("n_values")
            ).filter(pl.col("n_values") > 1)
            col_stats.timestamps_with_conflicts = len(conflicts)
            
            if verbose:
                print(f"        Conflicts: {col_stats.timestamps_with_conflicts}")
            
            base_np = base_df[col].to_numpy().astype(float)
            alt_np = alt_df[f"{col}__alt__"].to_numpy().astype(float)
            
            optimized, swaps = optimize_for_smoothness(
                base_np, alt_np, 
                min_improvement_ratio=min_improvement_ratio
            )
            col_stats.smoothness_swaps = swaps
            
            if verbose and swaps > 0:
                print(f"        Smoothness swaps: {swaps}")
            
            result = result.with_columns(
                pl.Series(name=col, values=optimized)
            )
        else:
            result = result.join(base_values, on="Timestamp", how="left")
        
        all_column_stats[col] = col_stats
    
    return result.sort("Timestamp"), all_column_stats


# =============================================================================
# OUTPUT FUNCTIONS
# =============================================================================

def save_outputs(
    df: pl.DataFrame,
    output_dir: Path,
    base_name: str,
    output_csv: bool = True,
    output_parquet: bool = True,
    verbose: bool = True
) -> tuple[Path | None, Path | None]:
    """
    Save dataframe to CSV and/or Parquet.
    Returns: (csv_path, parquet_path)
    """
    csv_path = None
    parquet_path = None
    
    if output_csv:
        csv_path = output_dir / f"{base_name}.csv"
        df.write_csv(csv_path, null_value="")
        if verbose:
            csv_size = csv_path.stat().st_size / (1024 * 1024)
            print(f"    Saved CSV: {csv_path.name} ({csv_size:.2f} MB)")
    
    if output_parquet:
        parquet_path = output_dir / f"{base_name}.parquet"
        df.write_parquet(parquet_path, compression="snappy")
        if verbose:
            parquet_size = parquet_path.stat().st_size / (1024 * 1024)
            print(f"    Saved Parquet: {parquet_path.name} ({parquet_size:.2f} MB)")
    
    return csv_path, parquet_path


# =============================================================================
# MAIN
# =============================================================================

def process_sensor(
    sensor_name: str,
    files: list[Path],
    output_dir: Path,
    timestamp: str,
    min_improvement_ratio: float = 1.3,
    output_csv: bool = True,
    output_parquet: bool = True,
    verbose: bool = True
) -> SensorStats:
    """Process a single sensor."""
    stats = SensorStats(sensor_name=sensor_name)
    
    if verbose:
        print(f"    Merging {len(files)} files...")
    
    df, column_stats = merge_sensor_files(
        files, 
        min_improvement_ratio=min_improvement_ratio,
        verbose=verbose
    )
    stats.column_stats = column_stats
    
    if df.is_empty() or len(df.columns) <= 1:
        print(f"    Warning: No data merged for {sensor_name}")
        return stats
    
    df = clean_for_output(df)
    
    stats.rows = len(df)
    stats.columns = df.columns
    
    # Save outputs
    safe_name = sanitize_filename(sensor_name)
    base_name = f"{timestamp}-{safe_name}"
    
    csv_path, parquet_path = save_outputs(
        df, output_dir, base_name,
        output_csv=output_csv,
        output_parquet=output_parquet,
        verbose=verbose
    )
    
    stats.csv_path = csv_path
    stats.parquet_path = parquet_path
    
    if verbose:
        print(f"    Final: {stats.rows} rows, columns: {stats.columns}")
    
    return stats


def print_summary(all_stats: list[SensorStats]) -> None:
    """Print processing summary."""
    print(f"\n{'=' * 120}")
    print("SUMMARY")
    print(f"{'=' * 120}")
    
    print(f"\n{'Sensor':<40} {'Rows':<12} {'Conflicts':<12} {'Swaps':<12} {'CSV Size':<15} {'Parquet Size':<15}")
    print("-" * 120)
    
    total_rows = 0
    total_conflicts = 0
    total_swaps = 0
    total_csv_size = 0
    total_parquet_size = 0
    
    for s in all_stats:
        name = s.sensor_name[:39] if len(s.sensor_name) > 39 else s.sensor_name
        
        conflicts = sum(cs.timestamps_with_conflicts for cs in s.column_stats.values())
        swaps = sum(cs.smoothness_swaps for cs in s.column_stats.values())
        
        csv_size_str = ""
        parquet_size_str = ""
        
        if s.csv_path and s.csv_path.exists():
            csv_size = s.csv_path.stat().st_size / (1024 * 1024)
            csv_size_str = f"{csv_size:.2f} MB"
            total_csv_size += csv_size
        
        if s.parquet_path and s.parquet_path.exists():
            parquet_size = s.parquet_path.stat().st_size / (1024 * 1024)
            parquet_size_str = f"{parquet_size:.2f} MB"
            total_parquet_size += parquet_size
        
        print(f"{name:<40} {s.rows:<12} {conflicts:<12} {swaps:<12} {csv_size_str:<15} {parquet_size_str:<15}")
        
        total_rows += s.rows
        total_conflicts += conflicts
        total_swaps += swaps
    
    print("-" * 120)
    print(f"{'TOTAL':<40} {total_rows:<12} {total_conflicts:<12} {total_swaps:<12} "
          f"{total_csv_size:.2f} MB{'':<7} {total_parquet_size:.2f} MB")
    
    if total_conflicts > 0:
        print(f"\nSmoothed {total_swaps}/{total_conflicts} conflicts ({100*total_swaps/total_conflicts:.1f}%)")
    
    if total_csv_size > 0 and total_parquet_size > 0:
        compression_ratio = total_csv_size / total_parquet_size
        savings = (1 - total_parquet_size / total_csv_size) * 100
        print(f"Parquet compression: {compression_ratio:.1f}x ({savings:.1f}% smaller than CSV)")


def main():
    parser = argparse.ArgumentParser(
        description="Merge sensor CSV exports with frequency + smoothness optimization"
    )
    parser.add_argument(
        "input_folder", 
        type=str, 
        help="Folder containing original CSV exports"
    )
    parser.add_argument(
        "output_folder", 
        type=str, 
        help="Folder to save merged files"
    )
    parser.add_argument(
        "--min-improvement-ratio",
        type=float,
        default=1.3,
        help="Minimum improvement ratio to swap to smoother value (default: 1.3)"
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
    
    # Determine output formats
    output_csv = not args.parquet_only
    output_parquet = not args.csv_only
    
    if not output_csv and not output_parquet:
        print("Error: Cannot use both --csv-only and --parquet-only")
        return 1
    
    verbose = not args.quiet
    
    # Find CSV files
    csv_files = list(input_dir.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files in {input_dir}")
    
    total_size = sum(f.stat().st_size for f in csv_files)
    print(f"Total input size: {total_size / (1024**3):.2f} GB")
    
    output_formats = []
    if output_csv:
        output_formats.append("CSV")
    if output_parquet:
        output_formats.append("Parquet")
    print(f"Output formats: {', '.join(output_formats)}")
    
    # Group by sensor
    sensor_groups: dict[str, list[Path]] = defaultdict(list)
    unmatched = []
    
    for fpath in csv_files:
        sensor = get_sensor_name(fpath.name)
        if sensor:
            sensor_groups[sensor].append(fpath)
        else:
            unmatched.append(fpath.name)
    
    if unmatched:
        print(f"\nWarning: {len(unmatched)} files did not match naming pattern")
    
    print(f"\nFound {len(sensor_groups)} unique sensors:")
    for sensor in sorted(sensor_groups.keys()):
        group_size = sum(f.stat().st_size for f in sensor_groups[sensor])
        print(f"  - {sensor} ({len(sensor_groups[sensor])} files, {group_size / (1024**2):.1f} MB)")
    
    # Process
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    start_time = datetime.now()
    all_stats = []
    
    for idx, (sensor, files) in enumerate(sorted(sensor_groups.items()), 1):
        sensor_start = datetime.now()
        print(f"\n{'=' * 120}")
        print(f"[{idx}/{len(sensor_groups)}] Processing: {sensor}")
        print(f"{'=' * 120}")
        
        stats = process_sensor(
            sensor, files, output_dir, timestamp,
            min_improvement_ratio=args.min_improvement_ratio,
            output_csv=output_csv,
            output_parquet=output_parquet,
            verbose=verbose
        )
        all_stats.append(stats)
        
        elapsed = (datetime.now() - sensor_start).total_seconds()
        print(f"    Time: {elapsed:.1f}s")
    
    total_elapsed = (datetime.now() - start_time).total_seconds()
    
    print_summary(all_stats)
    
    print(f"\n{'=' * 120}")
    print(f"COMPLETE! Total time: {total_elapsed:.1f}s")
    print(f"Output files saved to: {output_dir}")
    print(f"{'=' * 120}")
    
    return 0


if __name__ == "__main__":
    exit(main())
    