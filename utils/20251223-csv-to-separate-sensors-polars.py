#!/usr/bin/env python3
"""
Combine sensor CSV exports - POLARS VERSION (fixed)
"""

import argparse
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import string
from typing import Iterator

import polars as pl


SENSOR_NAME_RE = re.compile(
    r"^(?P<sensor>.+?)_export_\d{12}.*\.csv$",
    re.IGNORECASE
)


def get_sensor_name(filename: str) -> str | None:
    m = SENSOR_NAME_RE.match(filename)
    if m:
        return m.group("sensor").strip()
    return None


def get_sort_key(path: Path) -> tuple[str, str]:
    m = re.search(r"_export_(\d{12})", path.name)
    ts = m.group(1) if m else "999999999999"
    return (ts, path.name)


def generate_conflict_prefixes() -> Iterator[str]:
    """Generate !A!, !B!, ..., !Z!, !AA!, !AB!, ..."""
    letters = string.ascii_uppercase
    length = 1
    while True:
        if length == 1:
            for c in letters:
                yield f"!{c}!"
        else:
            for combo in _letter_combos(letters, length):
                yield f"!{combo}!"
        length += 1


def _letter_combos(letters: str, length: int):
    if length == 1:
        yield from letters
    else:
        for first in letters:
            for rest in _letter_combos(letters, length - 1):
                yield first + rest


def find_timestamp_column(columns: list[str]) -> str:
    for col in columns:
        if "timestamp" in col.lower():
            return col
    return columns[0] if columns else "Timestamp"


def read_csv_normalized(path: Path) -> pl.DataFrame | None:
    """Read CSV and normalize timestamp column using Polars."""
    try:
        df = pl.read_csv(path, infer_schema_length=10000)
    except Exception as e:
        print(f"  Warning: Could not read {path.name}: {e}")
        return None
    
    if df.is_empty() or len(df.columns) == 0:
        return None
    
    # Find and rename timestamp column
    ts_col = find_timestamp_column(df.columns)
    if ts_col != "Timestamp":
        df = df.rename({ts_col: "Timestamp"})
    
    # Parse timestamps - try multiple formats
    try:
        df = df.with_columns(
            pl.col("Timestamp").str.to_datetime(strict=False)
        )
    except Exception:
        # Try alternative parsing
        try:
            df = df.with_columns(
                pl.col("Timestamp").cast(pl.Datetime)
            )
        except Exception as e:
            print(f"    Warning: Could not parse timestamps: {e}")
            return None
    
    # Drop null timestamps
    df = df.filter(pl.col("Timestamp").is_not_null())
    
    if df.is_empty():
        return None
    
    # Remove duplicate timestamps (keep last)
    df = df.sort("Timestamp").unique(subset=["Timestamp"], keep="last")
    
    return df


def is_numeric_dtype(dtype: pl.DataType) -> bool:
    """Check if a polars dtype is numeric."""
    return dtype.is_numeric()


def build_differ_expr(df: pl.DataFrame, col1: str, col2: str, tolerance: float = 1e-5) -> pl.Expr:
    """
    Create expression to detect where two columns have different values.
    Both must be non-null and differ (beyond tolerance for floats).
    """
    dtype1 = df.schema.get(col1)
    dtype2 = df.schema.get(col2)
    
    both_not_null = pl.col(col1).is_not_null() & pl.col(col2).is_not_null()
    
    # Check if both are numeric
    if dtype1 is not None and dtype2 is not None:
        if is_numeric_dtype(dtype1) and is_numeric_dtype(dtype2):
            # Numeric comparison with tolerance
            diff_expr = (
                (pl.col(col1).cast(pl.Float64) - pl.col(col2).cast(pl.Float64)).abs() > tolerance
            )
            return both_not_null & diff_expr
    
    # String/other comparison
    diff_expr = pl.col(col1).cast(pl.Utf8) != pl.col(col2).cast(pl.Utf8)
    return both_not_null & diff_expr


def merge_two_dataframes(
    base: pl.DataFrame, 
    new: pl.DataFrame, 
    conflict_gen: Iterator[str]
) -> tuple[pl.DataFrame, int]:
    """
    Merge two dataframes on Timestamp.
    Returns merged dataframe and count of new conflict columns created.
    """
    conflicts_created = 0
    
    # Get non-timestamp columns from new dataframe
    new_data_cols = [c for c in new.columns if c != "Timestamp"]
    
    if not new_data_cols:
        return base, 0
    
    # Perform outer join
    joined = base.join(
        new, 
        on="Timestamp", 
        how="full",
        coalesce=True,
        suffix="_NEW_"
    )
    
    expressions = []
    columns_to_drop = []
    
    for col in new_data_cols:
        new_col_name = f"{col}_NEW_"
        
        # Check if column exists in joined result (it should if it was in new)
        if new_col_name not in joined.columns:
            continue
        
        # Check if this column existed in base
        if col in base.columns:
            # Column exists in both - need to check for conflicts and coalesce
            
            # Build the differ expression using actual schema
            differ_expr = build_differ_expr(joined, col, new_col_name)
            
            # Check if there are any actual conflicts
            conflict_check = joined.select(differ_expr.any()).item()
            
            if conflict_check:
                # Create conflict column with only differing values
                prefix = next(conflict_gen)
                conflict_col_name = f"{prefix} {col.strip()}"
                conflicts_created += 1
                
                # Add conflict column (only where values differ)
                expressions.append(
                    pl.when(differ_expr)
                    .then(pl.col(new_col_name))
                    .otherwise(None)
                    .alias(conflict_col_name)
                )
            
            # Coalesce: prefer existing value, fill with new where missing
            expressions.append(
                pl.coalesce([col, new_col_name]).alias(col)
            )
            columns_to_drop.append(new_col_name)
            
        else:
            # New column - just rename from _NEW_ suffix
            if new_col_name in joined.columns:
                joined = joined.rename({new_col_name: col})
    
    # Apply all expressions at once (more efficient)
    if expressions:
        joined = joined.with_columns(expressions)
    
    # Drop temporary columns
    columns_to_drop = [c for c in columns_to_drop if c in joined.columns]
    if columns_to_drop:
        joined = joined.drop(columns_to_drop)
    
    return joined, conflicts_created


def merge_sensor_files(files: list[Path]) -> pl.DataFrame:
    """Merge multiple CSV files for one sensor."""
    files = sorted(files, key=get_sort_key)
    
    combined: pl.DataFrame | None = None
    conflict_gen = generate_conflict_prefixes()
    total_conflicts = 0
    
    for i, fpath in enumerate(files):
        print(f"  [{i+1}/{len(files)}] Processing: {fpath.name}")
        df = read_csv_normalized(fpath)
        
        if df is None or df.is_empty():
            print(f"    Skipped (empty or invalid)")
            continue
        
        if combined is None:
            combined = df
            print(f"    Initial: {len(df)} rows, {len(df.columns)} columns")
            continue
        
        rows_before = len(combined)
        combined, new_conflicts = merge_two_dataframes(combined, df, conflict_gen)
        rows_after = len(combined)
        
        if new_conflicts > 0:
            print(f"    Added {rows_after - rows_before} new rows, {new_conflicts} conflict column(s)")
            total_conflicts += new_conflicts
        else:
            print(f"    Added {rows_after - rows_before} new rows")
    
    if combined is None:
        return pl.DataFrame({"Timestamp": []})
    
    # Sort by timestamp
    combined = combined.sort("Timestamp")
    
    # Remove completely empty columns
    non_empty_cols = ["Timestamp"]
    empty_count = 0
    
    for col in combined.columns:
        if col == "Timestamp":
            continue
        if combined[col].null_count() < len(combined):
            non_empty_cols.append(col)
        else:
            empty_count += 1
    
    if empty_count > 0:
        print(f"  Removing {empty_count} empty columns")
        combined = combined.select(non_empty_cols)
    
    # Reorder: Timestamp, regular columns, then conflict columns (sorted)
    regular_cols = ["Timestamp"]
    conflict_cols = []
    
    for col in combined.columns:
        if col == "Timestamp":
            continue
        if col.startswith("!"):
            conflict_cols.append(col)
        else:
            regular_cols.append(col)
    
    conflict_cols.sort()
    combined = combined.select(regular_cols + conflict_cols)
    
    print(f"  Total conflicts: {total_conflicts}")
    
    return combined


def sanitize_filename(name: str) -> str:
    for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']:
        name = name.replace(char, '_')
    return name.strip()


def main():
    parser = argparse.ArgumentParser(
        description="Combine sensor CSV exports by sensor name (Polars - fast)"
    )
    parser.add_argument("input_folder", type=str, help="Folder containing CSV files")
    parser.add_argument("--output-folder", type=str, default=None, 
                        help="Output folder (default: same as input)")
    args = parser.parse_args()
    
    input_dir = Path(args.input_folder).expanduser().resolve()
    output_dir = Path(args.output_folder).expanduser().resolve() if args.output_folder else input_dir
    
    if not input_dir.exists():
        print(f"Error: Input folder does not exist: {input_dir}")
        return 1
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all CSV files
    csv_files = list(input_dir.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files in {input_dir}")
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in csv_files)
    print(f"Total size: {total_size / (1024**3):.2f} GB")
    
    # Group by sensor name
    sensor_groups: dict[str, list[Path]] = defaultdict(list)
    unmatched = []
    
    for fpath in csv_files:
        sensor = get_sensor_name(fpath.name)
        if sensor:
            sensor_groups[sensor].append(fpath)
        else:
            unmatched.append(fpath.name)
    
    if unmatched:
        print(f"\nWarning: {len(unmatched)} files did not match naming pattern:")
        for name in unmatched[:5]:
            print(f"  - {name}")
        if len(unmatched) > 5:
            print(f"  ... and {len(unmatched) - 5} more")
    
    print(f"\nFound {len(sensor_groups)} unique sensors:")
    for sensor in sorted(sensor_groups.keys()):
        group_size = sum(f.stat().st_size for f in sensor_groups[sensor])
        print(f"  - {sensor} ({len(sensor_groups[sensor])} files, {group_size / (1024**2):.1f} MB)")
    
    # Process each sensor group
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    start_time = datetime.now()
    
    for idx, (sensor, files) in enumerate(sorted(sensor_groups.items()), 1):
        sensor_start = datetime.now()
        print(f"\n[{idx}/{len(sensor_groups)}] Merging sensor: {sensor}")
        
        merged = merge_sensor_files(files)
        
        if merged.is_empty() or (len(merged.columns) == 1 and merged.columns[0] == "Timestamp"):
            print(f"  Warning: No data merged for {sensor}")
            continue
        
        # Create output filename
        safe_name = sanitize_filename(sensor)
        output_file = output_dir / f"{timestamp}-{safe_name}.csv"
        
        # Write CSV
        merged.write_csv(output_file)
        
        elapsed = (datetime.now() - sensor_start).total_seconds()
        print(f"  Saved: {output_file.name}")
        print(f"  Final: {len(merged)} rows, {len(merged.columns)} columns")
        print(f"  Time: {elapsed:.1f}s")
    
    total_elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nDone! Total time: {total_elapsed:.1f}s")
    print(f"Output files saved to: {output_dir}")
    return 0


if __name__ == "__main__":
    exit(main())
    