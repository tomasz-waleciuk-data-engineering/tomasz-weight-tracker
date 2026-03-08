#!/usr/bin/env python3
"""
Sensor CSV Processing Pipeline (Polars)

Phase 1: MERGE
    - Read all CSV files from input folder
    - Group by sensor name (everything before _export_)
    - Merge files for each sensor, creating conflict columns for differing values

Phase 2: DEDUPLICATE
    - Remove identical values across conflict columns with same base name

Phase 3: CONSOLIDATE
    - Merge non-overlapping conflict columns

Phase 4: ANALYZE
    - Per-sensor statistics on differences between conflict columns
    - Show std_dev, min, max, mean, median of differences

Output:
    - Processed CSV files
    - Per-sensor analysis report (console + optional CSV)
"""

import argparse
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import string
from typing import Iterator
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

CONFLICT_PREFIX_RE = re.compile(r'^!([A-Z]+)! (.+)$')


# =============================================================================
# DATA CLASSES FOR STATS
# =============================================================================

@dataclass
class DifferenceStats:
    """Statistics for differences between conflict columns."""
    base_name: str
    columns_compared: list[str] = field(default_factory=list)
    column_pairs: int = 0
    rows_compared: int = 0
    min_diff: float = 0.0
    max_diff: float = 0.0
    mean_diff: float = 0.0
    median_diff: float = 0.0
    std_diff: float = 0.0
    percentile_25: float = 0.0
    percentile_75: float = 0.0
    percentile_95: float = 0.0
    percentile_99: float = 0.0
    all_differences: list[float] = field(default_factory=list)
    
    def compute_stats(self):
        """Compute aggregate stats from all_differences."""
        if not self.all_differences:
            return
        
        arr = np.array(self.all_differences)
        self.min_diff = float(np.min(arr))
        self.max_diff = float(np.max(arr))
        self.mean_diff = float(np.mean(arr))
        self.median_diff = float(np.median(arr))
        self.std_diff = float(np.std(arr))
        self.percentile_25 = float(np.percentile(arr, 25))
        self.percentile_75 = float(np.percentile(arr, 75))
        self.percentile_95 = float(np.percentile(arr, 95))
        self.percentile_99 = float(np.percentile(arr, 99))
        self.rows_compared = len(arr)


@dataclass 
class SensorStats:
    """Statistics for a single sensor file."""
    sensor_name: str
    rows: int = 0
    cols_initial: int = 0
    cols_final: int = 0
    conflicts_initial: int = 0
    conflicts_final: int = 0
    deduped_values: int = 0
    difference_stats: dict[str, DifferenceStats] = field(default_factory=dict)


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


def parse_conflict_column(col_name: str) -> tuple[str | None, str]:
    """Returns (prefix, base_name) or (None, col_name) if not a conflict column."""
    m = CONFLICT_PREFIX_RE.match(col_name)
    if m:
        return m.group(1), m.group(2)
    return None, col_name


def prefix_sort_key(col_name: str) -> tuple[int, str, str]:
    """Sort key for conflict columns: by prefix length, then alphabetically."""
    prefix, base = parse_conflict_column(col_name)
    if prefix:
        return (len(prefix), prefix, base)
    return (0, "", col_name)


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


def generate_prefix(n: int) -> str:
    """Generate prefix letter(s) for position n (0-indexed)."""
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if n < 26:
        return letters[n]
    else:
        result = []
        n_copy = n
        while n_copy >= 0:
            result.append(letters[n_copy % 26])
            n_copy = n_copy // 26 - 1
            if n_copy < 0:
                break
        return ''.join(reversed(result))


def find_timestamp_column(columns: list[str]) -> str:
    for col in columns:
        if "timestamp" in col.lower():
            return col
    return columns[0] if columns else "Timestamp"


def is_numeric_dtype(dtype: pl.DataType) -> bool:
    return dtype.is_numeric()


def get_non_null_indices(df: pl.DataFrame, col: str) -> set[int]:
    """Get set of row indices where column has non-null values."""
    mask = df[col].is_not_null()
    return set(
        df.with_row_index("__idx__")
        .filter(mask)["__idx__"]
        .to_list()
    )


def sanitize_filename(name: str) -> str:
    for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']:
        name = name.replace(char, '_')
    return name.strip()


# =============================================================================
# PHASE 1: MERGE CSV FILES
# =============================================================================

def read_csv_normalized(path: Path) -> pl.DataFrame | None:
    """Read CSV and normalize timestamp column."""
    try:
        df = pl.read_csv(path, infer_schema_length=10000)
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
    
    df = df.sort("Timestamp").unique(subset=["Timestamp"], keep="last")
    
    return df


def build_differ_expr(df: pl.DataFrame, col1: str, col2: str, tolerance: float = 1e-5) -> pl.Expr:
    """Expression for where two columns have different non-null values."""
    dtype1 = df.schema.get(col1)
    dtype2 = df.schema.get(col2)
    
    both_not_null = pl.col(col1).is_not_null() & pl.col(col2).is_not_null()
    
    if dtype1 is not None and dtype2 is not None:
        if is_numeric_dtype(dtype1) and is_numeric_dtype(dtype2):
            diff_expr = (
                (pl.col(col1).cast(pl.Float64) - pl.col(col2).cast(pl.Float64)).abs() > tolerance
            )
            return both_not_null & diff_expr
    
    diff_expr = pl.col(col1).cast(pl.Utf8) != pl.col(col2).cast(pl.Utf8)
    return both_not_null & diff_expr


def merge_two_dataframes(
    base: pl.DataFrame, 
    new: pl.DataFrame, 
    conflict_gen: Iterator[str]
) -> tuple[pl.DataFrame, int]:
    """Merge two dataframes on Timestamp with conflict detection."""
    conflicts_created = 0
    
    new_data_cols = [c for c in new.columns if c != "Timestamp"]
    
    if not new_data_cols:
        return base, 0
    
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
        
        if new_col_name not in joined.columns:
            continue
        
        if col in base.columns:
            differ_expr = build_differ_expr(joined, col, new_col_name)
            conflict_check = joined.select(differ_expr.any()).item()
            
            if conflict_check:
                prefix = next(conflict_gen)
                conflict_col_name = f"{prefix} {col.strip()}"
                conflicts_created += 1
                
                expressions.append(
                    pl.when(differ_expr)
                    .then(pl.col(new_col_name))
                    .otherwise(None)
                    .alias(conflict_col_name)
                )
            
            expressions.append(
                pl.coalesce([col, new_col_name]).alias(col)
            )
            columns_to_drop.append(new_col_name)
        else:
            if new_col_name in joined.columns:
                joined = joined.rename({new_col_name: col})
    
    if expressions:
        joined = joined.with_columns(expressions)
    
    columns_to_drop = [c for c in columns_to_drop if c in joined.columns]
    if columns_to_drop:
        joined = joined.drop(columns_to_drop)
    
    return joined, conflicts_created


def merge_sensor_files(files: list[Path], verbose: bool = True) -> pl.DataFrame:
    """Phase 1: Merge multiple CSV files for one sensor."""
    files = sorted(files, key=get_sort_key)
    
    combined: pl.DataFrame | None = None
    conflict_gen = generate_conflict_prefixes()
    total_conflicts = 0
    
    for i, fpath in enumerate(files):
        if verbose:
            print(f"      [{i+1}/{len(files)}] {fpath.name}")
        
        df = read_csv_normalized(fpath)
        
        if df is None or df.is_empty():
            if verbose:
                print(f"        Skipped (empty/invalid)")
            continue
        
        if combined is None:
            combined = df
            if verbose:
                print(f"        Initial: {len(df)} rows, {len(df.columns)} cols")
            continue
        
        rows_before = len(combined)
        combined, new_conflicts = merge_two_dataframes(combined, df, conflict_gen)
        rows_after = len(combined)
        total_conflicts += new_conflicts
        
        if verbose:
            msg = f"        +{rows_after - rows_before} rows"
            if new_conflicts > 0:
                msg += f", +{new_conflicts} conflicts"
            print(msg)
    
    if combined is None:
        return pl.DataFrame({"Timestamp": []})
    
    combined = combined.sort("Timestamp")
    
    # Remove empty columns
    non_empty_cols = [
        col for col in combined.columns 
        if combined[col].null_count() < len(combined)
    ]
    combined = combined.select(non_empty_cols)
    
    # Reorder columns
    regular_cols = ["Timestamp"]
    conflict_cols = []
    
    for col in combined.columns:
        if col == "Timestamp":
            continue
        if col.startswith("!"):
            conflict_cols.append(col)
        else:
            regular_cols.append(col)
    
    conflict_cols.sort(key=prefix_sort_key)
    combined = combined.select(regular_cols + conflict_cols)
    
    return combined


# =============================================================================
# PHASE 2: DEDUPLICATE
# =============================================================================

def values_equal_expr(col1: str, col2: str, df: pl.DataFrame, tolerance: float = 1e-5) -> pl.Expr:
    """Expression for where two columns have identical non-null values."""
    both_not_null = pl.col(col1).is_not_null() & pl.col(col2).is_not_null()
    
    dtype1 = df.schema.get(col1)
    dtype2 = df.schema.get(col2)
    
    if dtype1 is not None and dtype2 is not None:
        if is_numeric_dtype(dtype1) and is_numeric_dtype(dtype2):
            values_match = (
                (pl.col(col1).cast(pl.Float64) - pl.col(col2).cast(pl.Float64)).abs() <= tolerance
            )
            return both_not_null & values_match
    
    values_match = pl.col(col1).cast(pl.Utf8) == pl.col(col2).cast(pl.Utf8)
    return both_not_null & values_match


def deduplicate_column_group(
    df: pl.DataFrame,
    base_name: str,
    conflict_cols: list[str]
) -> tuple[pl.DataFrame, int]:
    """Remove identical values, keeping only in first column."""
    if len(conflict_cols) <= 1:
        return df, 0
    
    sorted_cols = sorted(conflict_cols, key=prefix_sort_key)
    total_deduped = 0
    
    for i in range(1, len(sorted_cols)):
        col = sorted_cols[i]
        prev_cols = sorted_cols[:i]
        
        matches_any_prev = pl.lit(False)
        for prev_col in prev_cols:
            is_match = values_equal_expr(col, prev_col, df)
            matches_any_prev = matches_any_prev | is_match
        
        match_count = df.select(matches_any_prev.sum()).item()
        
        if match_count > 0:
            total_deduped += match_count
            df = df.with_columns(
                pl.when(matches_any_prev)
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )
    
    return df, total_deduped


def deduplicate_all_groups(df: pl.DataFrame, verbose: bool = True) -> tuple[pl.DataFrame, int]:
    """Phase 2: Deduplicate identical values across all conflict column groups."""
    conflict_groups: dict[str, list[str]] = defaultdict(list)
    
    for col in df.columns:
        prefix, base_name = parse_conflict_column(col)
        if prefix is not None:
            conflict_groups[base_name].append(col)
    
    if not conflict_groups:
        return df, 0
    
    total_deduped = 0
    
    for base_name, conflict_cols in sorted(conflict_groups.items()):
        if len(conflict_cols) > 1:
            df, deduped = deduplicate_column_group(df, base_name, conflict_cols)
            total_deduped += deduped
            if verbose and deduped > 0:
                print(f"        Deduped {deduped} values in '{base_name}'")
    
    return df, total_deduped


# =============================================================================
# PHASE 3: CONSOLIDATE
# =============================================================================

def consolidate_column_group(
    df: pl.DataFrame, 
    base_name: str, 
    conflict_cols: list[str]
) -> dict[str, pl.Series]:
    """Merge non-overlapping conflict columns."""
    non_empty_cols = [
        col for col in conflict_cols 
        if df[col].null_count() < len(df)
    ]
    
    if not non_empty_cols:
        return {}
    
    if len(non_empty_cols) == 1:
        new_name = f"!A! {base_name}"
        return {new_name: df[non_empty_cols[0]]}
    
    col_indices: dict[str, set[int]] = {}
    for col in non_empty_cols:
        col_indices[col] = get_non_null_indices(df, col)
    
    def sort_key(col):
        indices = col_indices[col]
        return (min(indices) if indices else float('inf'), col)
    
    sorted_cols = sorted(non_empty_cols, key=sort_key)
    
    # Greedy bin packing
    bins: list[list[str]] = []
    bin_indices: list[set[int]] = []
    
    for col in sorted_cols:
        indices = col_indices[col]
        
        if not indices:
            continue
        
        placed = False
        for i, bin_idx in enumerate(bin_indices):
            if not (indices & bin_idx):
                bins[i].append(col)
                bin_indices[i] |= indices
                placed = True
                break
        
        if not placed:
            bins.append([col])
            bin_indices.append(indices.copy())
    
    result = {}
    for i, bin_cols in enumerate(bins):
        prefix = generate_prefix(i)
        new_col_name = f"!{prefix}! {base_name}"
        
        if len(bin_cols) == 1:
            result[new_col_name] = df[bin_cols[0]]
        else:
            result[new_col_name] = df.select(
                pl.coalesce([pl.col(c) for c in bin_cols])
            ).to_series()
    
    return result


def consolidate_all_groups(df: pl.DataFrame, verbose: bool = True) -> pl.DataFrame:
    """Phase 3: Consolidate non-overlapping conflict columns."""
    regular_cols: list[str] = []
    conflict_groups: dict[str, list[str]] = defaultdict(list)
    
    for col in df.columns:
        prefix, base_name = parse_conflict_column(col)
        if prefix is not None:
            conflict_groups[base_name].append(col)
        else:
            regular_cols.append(col)
    
    if not conflict_groups:
        return df
    
    new_conflict_columns: dict[str, pl.Series] = {}
    total_before = sum(len(cols) for cols in conflict_groups.values())
    total_after = 0
    
    for base_name, conflict_cols in sorted(conflict_groups.items()):
        before_count = len(conflict_cols)
        merged = consolidate_column_group(df, base_name, conflict_cols)
        after_count = len(merged)
        
        if verbose and before_count != after_count:
            print(f"        '{base_name}': {before_count} -> {after_count}")
        
        new_conflict_columns.update(merged)
        total_after += after_count
    
    if verbose:
        print(f"        Total conflicts: {total_before} -> {total_after}")
    
    result = df.select(regular_cols)
    
    for col_name in sorted(new_conflict_columns.keys(), key=prefix_sort_key):
        result = result.with_columns(
            new_conflict_columns[col_name].alias(col_name)
        )
    
    return result


# =============================================================================
# PHASE 4: ANALYZE DIFFERENCES (PER SENSOR)
# =============================================================================

def analyze_column_pair(
    df: pl.DataFrame, 
    col1: str, 
    col2: str
) -> list[float]:
    """Get absolute differences between two columns where both have values."""
    dtype1 = df.schema.get(col1)
    dtype2 = df.schema.get(col2)
    
    if dtype1 is None or dtype2 is None:
        return []
    
    if not (is_numeric_dtype(dtype1) and is_numeric_dtype(dtype2)):
        return []
    
    both_not_null = df.filter(
        pl.col(col1).is_not_null() & pl.col(col2).is_not_null()
    )
    
    if both_not_null.is_empty():
        return []
    
    diffs = both_not_null.select(
        (pl.col(col1).cast(pl.Float64) - pl.col(col2).cast(pl.Float64)).abs()
    ).to_series().to_list()
    
    return diffs


def analyze_differences_for_sensor(
    df: pl.DataFrame, 
    sensor_name: str,
    verbose: bool = True
) -> dict[str, DifferenceStats]:
    """
    Phase 4: Analyze differences for a single sensor.
    
    Compares:
    - Base column vs conflict columns
    - Conflict columns vs each other (same base name)
    """
    # Group columns by base name
    column_groups: dict[str, list[str]] = defaultdict(list)
    
    for col in df.columns:
        if col == "Timestamp":
            continue
        prefix, base_name = parse_conflict_column(col)
        column_groups[base_name].append(col)
    
    all_stats: dict[str, DifferenceStats] = {}
    
    for base_name, columns in sorted(column_groups.items()):
        if len(columns) < 2:
            continue
        
        stats = DifferenceStats(base_name=base_name)
        stats.columns_compared = columns.copy()
        
        # Compare all pairs
        for i, col1 in enumerate(columns):
            for col2 in columns[i+1:]:
                diffs = analyze_column_pair(df, col1, col2)
                if diffs:
                    stats.column_pairs += 1
                    stats.all_differences.extend(diffs)
        
        if stats.all_differences:
            stats.compute_stats()
            all_stats[base_name] = stats
    
    return all_stats


def print_sensor_analysis(stats: dict[str, DifferenceStats], sensor_name: str) -> None:
    """Print detailed difference analysis for a single sensor."""
    if not stats:
        print("        No overlapping numeric data to analyze")
        return
    
    print(f"\n        ┌{'─' * 98}┐")
    print(f"        │ {'DIFFERENCE ANALYSIS: ' + sensor_name:<96} │")
    print(f"        ├{'─' * 98}┤")
    print(f"        │ {'Measurement':<40} {'Pairs':<6} {'Rows':<8} {'Min':<10} {'Max':<10} {'Mean':<10} {'Std':<10} │")
    print(f"        ├{'─' * 98}┤")
    
    for base_name, s in sorted(stats.items()):
        name = base_name[:39] if len(base_name) > 39 else base_name
        print(f"        │ {name:<40} {s.column_pairs:<6} {s.rows_compared:<8} "
              f"{s.min_diff:<10.4f} {s.max_diff:<10.4f} {s.mean_diff:<10.4f} {s.std_diff:<10.4f} │")
    
    print(f"        └{'─' * 98}┘")
    
    # Detailed percentile breakdown
    print(f"\n        Percentile breakdown:")
    print(f"        {'Measurement':<40} {'P25':<10} {'P50/Med':<10} {'P75':<10} {'P95':<10} {'P99':<10}")
    print(f"        {'-' * 90}")
    
    for base_name, s in sorted(stats.items()):
        name = base_name[:39] if len(base_name) > 39 else base_name
        print(f"        {name:<40} {s.percentile_25:<10.4f} {s.median_diff:<10.4f} "
              f"{s.percentile_75:<10.4f} {s.percentile_95:<10.4f} {s.percentile_99:<10.4f}")


# =============================================================================
# MAIN PROCESSING PIPELINE
# =============================================================================

def process_sensor(
    sensor_name: str,
    files: list[Path],
    output_dir: Path,
    timestamp: str,
    verbose: bool = True
) -> SensorStats:
    """Process a single sensor through all phases."""
    stats = SensorStats(sensor_name=sensor_name)
    
    # PHASE 1: Merge
    if verbose:
        print(f"    Phase 1: Merging {len(files)} files...")
    
    df = merge_sensor_files(files, verbose=verbose)
    
    if df.is_empty() or (len(df.columns) == 1 and df.columns[0] == "Timestamp"):
        print(f"    Warning: No data merged for {sensor_name}")
        return stats
    
    stats.rows = len(df)
    stats.cols_initial = len(df.columns)
    stats.conflicts_initial = sum(1 for c in df.columns if CONFLICT_PREFIX_RE.match(c))
    
    if verbose:
        print(f"    After merge: {stats.rows} rows, {stats.cols_initial} cols, "
              f"{stats.conflicts_initial} conflict cols")
    
    # PHASE 2: Deduplicate
    if stats.conflicts_initial > 0:
        if verbose:
            print(f"    Phase 2: Deduplicating...")
        
        df, deduped = deduplicate_all_groups(df, verbose=verbose)
        stats.deduped_values = deduped
    
    # PHASE 3: Consolidate
    if stats.conflicts_initial > 0:
        if verbose:
            print(f"    Phase 3: Consolidating...")
        
        df = consolidate_all_groups(df, verbose=verbose)
    
    # Remove empty columns
    non_empty_cols = [
        col for col in df.columns 
        if df[col].null_count() < len(df)
    ]
    df = df.select(non_empty_cols)
    
    stats.cols_final = len(df.columns)
    stats.conflicts_final = sum(1 for c in df.columns if CONFLICT_PREFIX_RE.match(c))
    
    # PHASE 4: Analyze (per sensor)
    if stats.conflicts_final > 0 or stats.cols_final > 2:  # Analyze if multiple data columns
        if verbose:
            print(f"    Phase 4: Analyzing differences...")
        
        stats.difference_stats = analyze_differences_for_sensor(df, sensor_name, verbose=verbose)
        
        if verbose and stats.difference_stats:
            print_sensor_analysis(stats.difference_stats, sensor_name)
    
    # Save output
    safe_name = sanitize_filename(sensor_name)
    output_file = output_dir / f"{timestamp}-{safe_name}.csv"
    df.write_csv(output_file)
    
    if verbose:
        print(f"\n    Saved: {output_file.name}")
        print(f"    Final: {stats.rows} rows, {stats.cols_final} cols, "
              f"{stats.conflicts_final} conflict cols")
    
    return stats


def print_final_summary(all_stats: list[SensorStats]) -> None:
    """Print comprehensive summary of all processing."""
    print("\n" + "=" * 120)
    print("FINAL PROCESSING SUMMARY")
    print("=" * 120)
    
    # Basic stats table
    print(f"\n{'Sensor':<40} {'Rows':<12} {'Cols':<15} {'Conflicts':<18} {'Deduped':<12}")
    print("-" * 120)
    
    total_rows = 0
    total_conflicts_before = 0
    total_conflicts_after = 0
    total_deduped = 0
    
    for s in all_stats:
        name = s.sensor_name[:39] if len(s.sensor_name) > 39 else s.sensor_name
        cols_change = f"{s.cols_initial} -> {s.cols_final}"
        conflicts_change = f"{s.conflicts_initial} -> {s.conflicts_final}"
        
        print(f"{name:<40} {s.rows:<12} {cols_change:<15} {conflicts_change:<18} {s.deduped_values:<12}")
        
        total_rows += s.rows
        total_conflicts_before += s.conflicts_initial
        total_conflicts_after += s.conflicts_final
        total_deduped += s.deduped_values
    
    print("-" * 120)
    print(f"{'TOTAL':<40} {total_rows:<12} {'':<15} "
          f"{total_conflicts_before} -> {total_conflicts_after:<7} {total_deduped:<12}")
    
    if total_conflicts_before > 0:
        reduction_pct = (total_conflicts_before - total_conflicts_after) / total_conflicts_before * 100
        print(f"\nConflict column reduction: {reduction_pct:.1f}%")
    
    # Per-sensor difference summary
    print("\n" + "=" * 120)
    print("PER-SENSOR DIFFERENCE SUMMARY")
    print("=" * 120)
    
    for s in all_stats:
        if not s.difference_stats:
            continue
        
        print(f"\n┌{'─' * 118}┐")
        print(f"│ {s.sensor_name:<116} │")
        print(f"├{'─' * 118}┤")
        print(f"│ {'Measurement':<50} {'Pairs':<8} {'Rows':<10} {'Max Diff':<12} {'Mean':<12} {'Std':<12} │")
        print(f"├{'─' * 118}┤")
        
        for base_name, diff_stats in sorted(s.difference_stats.items()):
            name = base_name[:49] if len(base_name) > 49 else base_name
            print(f"│ {name:<50} {diff_stats.column_pairs:<8} {diff_stats.rows_compared:<10} "
                  f"{diff_stats.max_diff:<12.4f} {diff_stats.mean_diff:<12.4f} {diff_stats.std_diff:<12.4f} │")
        
        print(f"└{'─' * 118}┘")


def generate_analysis_csv(all_stats: list[SensorStats], output_path: Path) -> None:
    """Generate detailed per-sensor analysis CSV."""
    rows = []
    
    for sensor_stats in all_stats:
        for base_name, diff_stats in sensor_stats.difference_stats.items():
            rows.append({
                "sensor": sensor_stats.sensor_name,
                "measurement": base_name,
                "columns_compared": ", ".join(diff_stats.columns_compared),
                "column_pairs": diff_stats.column_pairs,
                "rows_compared": diff_stats.rows_compared,
                "min_diff": diff_stats.min_diff,
                "max_diff": diff_stats.max_diff,
                "mean_diff": diff_stats.mean_diff,
                "median_diff": diff_stats.median_diff,
                "std_diff": diff_stats.std_diff,
                "percentile_25": diff_stats.percentile_25,
                "percentile_75": diff_stats.percentile_75,
                "percentile_95": diff_stats.percentile_95,
                "percentile_99": diff_stats.percentile_99,
                "sensor_total_rows": sensor_stats.rows,
                "sensor_conflicts_before": sensor_stats.conflicts_initial,
                "sensor_conflicts_after": sensor_stats.conflicts_final,
                "sensor_deduped": sensor_stats.deduped_values
            })
    
    if rows:
        df = pl.DataFrame(rows)
        df.write_csv(output_path)
        print(f"\nDetailed analysis report saved to: {output_path}")
    else:
        print("\nNo difference data to save to analysis CSV")


def main():
    parser = argparse.ArgumentParser(
        description="Process sensor CSV exports: merge, deduplicate, consolidate, analyze (per-sensor)"
    )
    parser.add_argument(
        "input_folder", 
        type=str, 
        help="Folder containing original CSV exports"
    )
    parser.add_argument(
        "output_folder", 
        type=str, 
        help="Folder to save processed CSV files"
    )
    parser.add_argument(
        "--analysis-csv",
        type=str,
        default=None,
        help="Path to save detailed analysis report CSV (optional)"
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
    
    verbose = not args.quiet
    
    # Find and group CSV files
    csv_files = list(input_dir.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files in {input_dir}")
    
    total_size = sum(f.stat().st_size for f in csv_files)
    print(f"Total size: {total_size / (1024**3):.2f} GB")
    
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
        for name in unmatched[:5]:
            print(f"  - {name}")
        if len(unmatched) > 5:
            print(f"  ... and {len(unmatched) - 5} more")
    
    print(f"\nFound {len(sensor_groups)} unique sensors:")
    for sensor in sorted(sensor_groups.keys()):
        group_size = sum(f.stat().st_size for f in sensor_groups[sensor])
        print(f"  - {sensor} ({len(sensor_groups[sensor])} files, {group_size / (1024**2):.1f} MB)")
    
    # Process each sensor
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    start_time = datetime.now()
    all_stats = []
    
    for idx, (sensor, files) in enumerate(sorted(sensor_groups.items()), 1):
        sensor_start = datetime.now()
        print(f"\n{'=' * 120}")
        print(f"[{idx}/{len(sensor_groups)}] Processing: {sensor}")
        print(f"{'=' * 120}")
        
        stats = process_sensor(sensor, files, output_dir, timestamp, verbose=verbose)
        all_stats.append(stats)
        
        elapsed = (datetime.now() - sensor_start).total_seconds()
        print(f"\n    Sensor processing time: {elapsed:.1f}s")
    
    total_elapsed = (datetime.now() - start_time).total_seconds()
    
    # Print final summary
    print_final_summary(all_stats)
    
    # Save analysis CSV if requested
    if args.analysis_csv:
        analysis_path = Path(args.analysis_csv).expanduser().resolve()
        generate_analysis_csv(all_stats, analysis_path)
    
    print(f"\n{'=' * 120}")
    print(f"COMPLETE! Total time: {total_elapsed:.1f}s")
    print(f"Output files saved to: {output_dir}")
    print(f"{'=' * 120}")
    
    return 0


if __name__ == "__main__":
    exit(main())
    