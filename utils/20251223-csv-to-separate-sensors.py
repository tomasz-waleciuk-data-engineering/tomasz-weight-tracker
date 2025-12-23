#!/usr/bin/env python3
"""
Combine sensor CSV exports from a folder - FIXED VERSION

Key fixes:
1. Only create conflict columns when values ACTUALLY differ (not just when both exist)
2. Remove empty conflict columns at the end
3. Fixed pandas FutureWarning
4. Fixed double-space in conflict column names
"""

import argparse
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import string

import pandas as pd
import numpy as np


SENSOR_NAME_RE = re.compile(
    r"^(?P<sensor>.+?)_export_\d{12}.*\.csv$",
    re.IGNORECASE
)


def get_sensor_name(filename: str) -> str | None:
    """Extract sensor name from filename."""
    m = SENSOR_NAME_RE.match(filename)
    if m:
        return m.group("sensor").strip()
    return None


def get_sort_key(path: Path) -> tuple[str, str]:
    """Sort files by embedded timestamp, then filename."""
    m = re.search(r"_export_(\d{12})", path.name)
    ts = m.group(1) if m else "999999999999"
    return (ts, path.name)


def find_timestamp_column(columns: list[str]) -> str:
    """Find the timestamp column (case-insensitive search)."""
    for col in columns:
        if "timestamp" in col.lower():
            return col
    return columns[0] if columns else "Timestamp"


def read_csv_normalized(path: Path) -> pd.DataFrame:
    """Read CSV and normalize the timestamp column."""
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"  Warning: Could not read {path.name}: {e}")
        return pd.DataFrame()
    
    if df.empty or len(df.columns) == 0:
        return pd.DataFrame()
    
    # Find and rename timestamp column
    ts_col = find_timestamp_column(list(df.columns))
    df = df.rename(columns={ts_col: "Timestamp"})
    
    # Parse timestamps
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df.dropna(subset=["Timestamp"])
    
    if df.empty:
        return pd.DataFrame()
    
    # Remove duplicate timestamps within same file (keep last)
    df = df.sort_values("Timestamp").drop_duplicates(subset=["Timestamp"], keep="last")
    
    # Convert numeric columns (fixed for newer pandas)
    for col in df.columns:
        if col != "Timestamp":
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass
    
    return df.set_index("Timestamp")


def generate_conflict_prefixes():
    """Generate !A!, !B!, ..., !Z!, !AA!, !AB!, ..."""
    letters = string.ascii_uppercase
    length = 1
    while True:
        for combo in _letter_combos(letters, length):
            yield f"!{combo}! "
        length += 1


def _letter_combos(letters: str, length: int):
    """Generate letter combinations of given length."""
    if length == 1:
        yield from letters
    else:
        for first in letters:
            for rest in _letter_combos(letters, length - 1):
                yield first + rest


def values_equal(a, b, rtol: float = 1e-5, atol: float = 1e-9) -> bool:
    """Check if two values are equal (handles NaN and numeric tolerance)."""
    # Both NaN -> equal
    if pd.isna(a) and pd.isna(b):
        return True
    # One NaN -> not equal
    if pd.isna(a) or pd.isna(b):
        return False
    # Both numeric -> compare with tolerance
    try:
        return np.isclose(float(a), float(b), rtol=rtol, atol=atol)
    except (ValueError, TypeError):
        # String comparison
        return str(a) == str(b)


def find_actual_conflicts(existing: pd.Series, incoming: pd.Series, rtol: float = 1e-5) -> pd.Series:
    """
    Return boolean mask where:
    - Both series have non-null values AND
    - The values are actually DIFFERENT
    """
    mask = pd.Series(False, index=existing.index)
    
    for idx in existing.index:
        if idx not in incoming.index:
            continue
        
        ex_val = existing.get(idx)
        in_val = incoming.get(idx)
        
        # Only flag as conflict if both have values AND they differ
        if pd.notna(ex_val) and pd.notna(in_val):
            if not values_equal(ex_val, in_val, rtol=rtol):
                mask[idx] = True
    
    return mask


def merge_sensor_files(files: list[Path]) -> pd.DataFrame:
    """Merge multiple CSV files for one sensor."""
    files = sorted(files, key=get_sort_key)
    
    combined: pd.DataFrame | None = None
    conflict_gen = generate_conflict_prefixes()
    conflict_count = 0
    
    for fpath in files:
        print(f"  Processing: {fpath.name}")
        df = read_csv_normalized(fpath)
        
        if df.empty:
            continue
        
        if combined is None:
            combined = df.copy()
            continue
        
        # Expand combined index to include new timestamps
        all_timestamps = combined.index.union(df.index)
        combined = combined.reindex(all_timestamps)
        
        for col in df.columns:
            incoming = df[col].reindex(all_timestamps)
            
            if col not in combined.columns:
                # New column (possibly from calibration) - just add it
                combined[col] = incoming
                continue
            
            existing = combined[col]
            
            # Find rows where incoming has data but existing doesn't
            missing_mask = existing.isna() & incoming.notna()
            if missing_mask.any():
                combined.loc[missing_mask, col] = incoming[missing_mask]
            
            # Find ACTUAL conflicts (both have values AND they differ)
            conflict_mask = find_actual_conflicts(combined[col], incoming)
            
            if conflict_mask.any():
                conflict_count += 1
                prefix = next(conflict_gen)
                conflict_col = f"{prefix}{col.strip()}"
                combined[conflict_col] = pd.NA
                combined.loc[conflict_mask, conflict_col] = incoming[conflict_mask]
                print(f"    Conflict #{conflict_count} in '{col.strip()}': {conflict_mask.sum()} rows -> {conflict_col}")
    
    if combined is None:
        return pd.DataFrame(columns=["Timestamp"])
    
    # Reset index and sort
    combined = combined.reset_index().sort_values("Timestamp")
    
    # Remove completely empty columns (no non-null values)
    empty_cols = [col for col in combined.columns if combined[col].isna().all()]
    if empty_cols:
        print(f"  Removing {len(empty_cols)} empty columns")
        combined = combined.drop(columns=empty_cols)
    
    # Reorder columns: Timestamp first, then regular cols, then conflict cols sorted
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
    combined = combined[regular_cols + conflict_cols]
    
    return combined


def sanitize_filename(name: str) -> str:
    """Remove or replace invalid filename characters."""
    for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']:
        name = name.replace(char, '_')
    return name.strip()


def main():
    parser = argparse.ArgumentParser(
        description="Combine sensor CSV exports by sensor name"
    )
    parser.add_argument(
        "input_folder",
        type=str,
        help="Folder containing CSV files"
    )
    parser.add_argument(
        "--output-folder",
        type=str,
        default=None,
        help="Output folder (default: same as input)"
    )
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
        print(f"  - {sensor} ({len(sensor_groups[sensor])} files)")
    
    # Process each sensor group
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    
    for sensor, files in sorted(sensor_groups.items()):
        print(f"\nMerging sensor: {sensor}")
        
        merged = merge_sensor_files(files)
        
        if merged.empty or (len(merged.columns) == 1 and merged.columns[0] == "Timestamp"):
            print(f"  Warning: No data merged for {sensor}")
            continue
        
        # Create output filename
        safe_name = sanitize_filename(sensor)
        output_file = output_dir / f"{timestamp}-{safe_name}.csv"
        
        merged.to_csv(output_file, index=False)
        print(f"  Saved: {output_file.name} ({len(merged)} rows, {len(merged.columns)} columns)")
    
    print(f"\nDone! Output files saved to: {output_dir}")
    return 0


if __name__ == "__main__":
    exit(main())
    