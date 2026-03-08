#!/usr/bin/env python3
"""
Post-process merged sensor files:

Step 1 - DEDUPLICATION:
    For columns with the same base name, if multiple columns have identical
    values at the same row, keep the value only in one column (null out duplicates).

Step 2 - CONSOLIDATION:
    Merge non-overlapping columns with the same base name.

Goal: Minimize conflict columns to typically 2-3 per base name.
"""

import argparse
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import polars as pl


# Pattern to match conflict column prefix: !A!, !AA!, !ABC!, etc.
CONFLICT_PREFIX_RE = re.compile(r'^!([A-Z]+)! (.+)$')


def parse_conflict_column(col_name: str) -> tuple[str | None, str]:
    """
    Parse a column name to extract prefix and base name.
    Returns (prefix, base_name) or (None, col_name) if not a conflict column.
    """
    m = CONFLICT_PREFIX_RE.match(col_name)
    if m:
        return m.group(1), m.group(2)
    return None, col_name


def prefix_sort_key(col_name: str) -> tuple[int, str]:
    """Sort key for conflict columns: by prefix length, then alphabetically."""
    prefix, _ = parse_conflict_column(col_name)
    if prefix:
        return (len(prefix), prefix)
    return (0, "")


def get_non_null_indices(df: pl.DataFrame, col: str) -> set[int]:
    """Get set of row indices where column has non-null values."""
    mask = df[col].is_not_null()
    return set(
        df.with_row_index("__idx__")
        .filter(mask)["__idx__"]
        .to_list()
    )


def generate_prefix(n: int) -> str:
    """Generate prefix letter(s) for position n (0-indexed): A, B, ..., Z, AA, AB, ..."""
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


def values_equal_expr(col1: str, col2: str, df: pl.DataFrame, tolerance: float = 1e-5) -> pl.Expr:
    """
    Create expression that returns True where both columns have identical non-null values.
    Handles numeric tolerance and different dtypes.
    """
    both_not_null = pl.col(col1).is_not_null() & pl.col(col2).is_not_null()
    
    dtype1 = df.schema.get(col1)
    dtype2 = df.schema.get(col2)
    
    # Check if both are numeric
    if dtype1 is not None and dtype2 is not None:
        if dtype1.is_numeric() and dtype2.is_numeric():
            values_match = (
                (pl.col(col1).cast(pl.Float64) - pl.col(col2).cast(pl.Float64)).abs() <= tolerance
            )
            return both_not_null & values_match
    
    # String/other comparison
    values_match = pl.col(col1).cast(pl.Utf8) == pl.col(col2).cast(pl.Utf8)
    return both_not_null & values_match


# =============================================================================
# STEP 1: DEDUPLICATION
# =============================================================================

def deduplicate_column_group(
    df: pl.DataFrame,
    base_name: str,
    conflict_cols: list[str],
    verbose: bool = True
) -> pl.DataFrame:
    """
    For rows where multiple conflict columns have identical values,
    keep the value only in the first column (by prefix order), null out others.
    """
    if len(conflict_cols) <= 1:
        return df
    
    # Sort columns by prefix (!A!, !B!, ..., !Z!, !AA!, ...)
    sorted_cols = sorted(conflict_cols, key=prefix_sort_key)
    
    total_deduped = 0
    
    # Process each column from 2nd onwards
    for i in range(1, len(sorted_cols)):
        col = sorted_cols[i]
        prev_cols = sorted_cols[:i]
        
        # Build expression: True if this column's value matches ANY previous column's value
        matches_any_prev = pl.lit(False)
        
        for prev_col in prev_cols:
            is_match = values_equal_expr(col, prev_col, df)
            matches_any_prev = matches_any_prev | is_match
        
        # Count how many values will be deduped
        match_count = df.select(matches_any_prev.sum()).item()
        
        if match_count > 0:
            total_deduped += match_count
            
            # Null out values that match a previous column
            df = df.with_columns(
                pl.when(matches_any_prev)
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )
    
    if verbose and total_deduped > 0:
        print(f"      Deduped {total_deduped} identical values in '{base_name}'")
    
    return df


def deduplicate_all_groups(df: pl.DataFrame, verbose: bool = True) -> pl.DataFrame:
    """
    Step 1: Deduplicate identical values across all conflict column groups.
    """
    # Group conflict columns by base name
    conflict_groups: dict[str, list[str]] = defaultdict(list)
    
    for col in df.columns:
        prefix, base_name = parse_conflict_column(col)
        if prefix is not None:
            conflict_groups[base_name].append(col)
    
    if not conflict_groups:
        return df
    
    if verbose:
        print("    Step 1: Deduplicating identical values...")
    
    for base_name, conflict_cols in sorted(conflict_groups.items()):
        if len(conflict_cols) > 1:
            df = deduplicate_column_group(df, base_name, conflict_cols, verbose)
    
    return df


# =============================================================================
# STEP 2: CONSOLIDATION
# =============================================================================

def consolidate_column_group(
    df: pl.DataFrame, 
    base_name: str, 
    conflict_cols: list[str]
) -> dict[str, pl.Series]:
    """
    Consolidate a group of conflict columns with the same base name.
    Merge columns that don't overlap.
    Returns dict of new column names -> series.
    """
    # Filter out completely empty columns first
    non_empty_cols = [
        col for col in conflict_cols 
        if df[col].null_count() < len(df)
    ]
    
    if not non_empty_cols:
        return {}
    
    if len(non_empty_cols) == 1:
        new_name = f"!A! {base_name}"
        return {new_name: df[non_empty_cols[0]]}
    
    # Get non-null indices for each column
    col_indices: dict[str, set[int]] = {}
    for col in non_empty_cols:
        col_indices[col] = get_non_null_indices(df, col)
    
    # Sort columns by first non-null row index (helps greedy algorithm)
    def sort_key(col):
        indices = col_indices[col]
        return (min(indices) if indices else float('inf'), col)
    
    sorted_cols = sorted(non_empty_cols, key=sort_key)
    
    # Greedy bin packing: each bin contains non-overlapping columns
    bins: list[list[str]] = []
    bin_indices: list[set[int]] = []
    
    for col in sorted_cols:
        indices = col_indices[col]
        
        if not indices:
            continue
        
        # Try to find a bin where this column doesn't overlap
        placed = False
        for i, bin_idx in enumerate(bin_indices):
            if not (indices & bin_idx):  # No overlap
                bins[i].append(col)
                bin_indices[i] |= indices
                placed = True
                break
        
        if not placed:
            bins.append([col])
            bin_indices.append(indices.copy())
    
    # Create merged columns for each bin
    result = {}
    for i, bin_cols in enumerate(bins):
        prefix = generate_prefix(i)
        new_col_name = f"!{prefix}! {base_name}"
        
        if len(bin_cols) == 1:
            result[new_col_name] = df[bin_cols[0]]
        else:
            # Merge multiple columns using coalesce
            result[new_col_name] = df.select(
                pl.coalesce([pl.col(c) for c in bin_cols])
            ).to_series()
    
    return result


def consolidate_all_groups(df: pl.DataFrame, verbose: bool = True) -> pl.DataFrame:
    """
    Step 2: Consolidate non-overlapping conflict columns.
    """
    # Separate regular columns and group conflict columns
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
    
    if verbose:
        print("    Step 2: Consolidating non-overlapping columns...")
    
    # Process each group
    new_conflict_columns: dict[str, pl.Series] = {}
    total_before = sum(len(cols) for cols in conflict_groups.values())
    total_after = 0
    
    for base_name, conflict_cols in sorted(conflict_groups.items()):
        before_count = len(conflict_cols)
        merged = consolidate_column_group(df, base_name, conflict_cols)
        after_count = len(merged)
        
        if verbose and before_count > 1:
            print(f"      '{base_name}': {before_count} -> {after_count}")
        
        new_conflict_columns.update(merged)
        total_after += after_count
    
    if verbose:
        print(f"      Total: {total_before} -> {total_after}")
    
    # Build result dataframe
    result = df.select(regular_cols)
    
    # Add consolidated conflict columns (sorted)
    for col_name in sorted(new_conflict_columns.keys(), key=prefix_sort_key):
        result = result.with_columns(
            new_conflict_columns[col_name].alias(col_name)
        )
    
    return result


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_file(input_path: Path, output_path: Path) -> dict:
    """Process a single file: deduplicate then consolidate."""
    print(f"  Reading: {input_path.name}")
    
    df = pl.read_csv(input_path)
    
    initial_cols = len(df.columns)
    conflict_cols_before = sum(1 for c in df.columns if CONFLICT_PREFIX_RE.match(c))
    
    if conflict_cols_before == 0:
        print(f"    No conflict columns, copying as-is")
        df.write_csv(output_path)
        return {
            "file": input_path.name,
            "rows": len(df),
            "cols_before": initial_cols,
            "cols_after": initial_cols,
            "conflicts_before": 0,
            "conflicts_after": 0
        }
    
    print(f"    {initial_cols} columns, {conflict_cols_before} conflict columns")
    
    # STEP 1: Deduplicate identical values
    df = deduplicate_all_groups(df, verbose=True)
    
    # STEP 2: Consolidate non-overlapping columns
    result = consolidate_all_groups(df, verbose=True)
    
    # Final cleanup: remove completely empty columns
    non_empty_cols = [
        col for col in result.columns 
        if result[col].null_count() < len(result)
    ]
    
    if len(non_empty_cols) < len(result.columns):
        removed = len(result.columns) - len(non_empty_cols)
        print(f"    Removed {removed} empty columns")
        result = result.select(non_empty_cols)
    
    conflict_cols_after = sum(1 for c in result.columns if CONFLICT_PREFIX_RE.match(c))
    
    print(f"    Final: {len(result.columns)} columns, {conflict_cols_after} conflict columns")
    
    result.write_csv(output_path)
    print(f"    Saved: {output_path.name}")
    
    return {
        "file": input_path.name,
        "rows": len(result),
        "cols_before": initial_cols,
        "cols_after": len(result.columns),
        "conflicts_before": conflict_cols_before,
        "conflicts_after": conflict_cols_after
    }


def print_summary(stats: list[dict]) -> None:
    """Print summary of all processed files."""
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    total_conflicts_before = sum(s["conflicts_before"] for s in stats)
    total_conflicts_after = sum(s["conflicts_after"] for s in stats)
    
    print(f"\n{'File':<45} {'Before':<10} {'After':<10} {'Reduction':<15}")
    print("-" * 80)
    
    for s in stats:
        if s["conflicts_before"] > 0:
            reduction = s["conflicts_before"] - s["conflicts_after"]
            pct = (reduction / s["conflicts_before"] * 100) if s["conflicts_before"] > 0 else 0
            
            # Truncate filename if too long
            fname = s['file']
            if len(fname) > 44:
                fname = fname[:41] + "..."
            
            print(f"{fname:<45} {s['conflicts_before']:<10} {s['conflicts_after']:<10} -{reduction} ({pct:.0f}%)")
    
    print("-" * 80)
    if total_conflicts_before > 0:
        total_reduction = total_conflicts_before - total_conflicts_after
        total_pct = total_reduction / total_conflicts_before * 100
        print(f"{'TOTAL':<45} {total_conflicts_before:<10} {total_conflicts_after:<10} -{total_reduction} ({total_pct:.0f}%)")
    
    print("\n✓ Most files should now have 2-3 conflict columns per measurement type")


def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate and consolidate conflict columns in merged sensor CSV files"
    )
    parser.add_argument(
        "input_folder", 
        type=str, 
        help="Folder containing merged CSV files"
    )
    parser.add_argument(
        "output_folder", 
        type=str, 
        help="Folder to save processed CSV files"
    )
    args = parser.parse_args()
    
    input_dir = Path(args.input_folder).expanduser().resolve()
    output_dir = Path(args.output_folder).expanduser().resolve()
    
    if not input_dir.exists():
        print(f"Error: Input folder does not exist: {input_dir}")
        return 1
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    csv_files = sorted(input_dir.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files in {input_dir}")
    
    if not csv_files:
        print("No CSV files to process.")
        return 0
    
    start_time = datetime.now()
    all_stats = []
    
    for i, fpath in enumerate(csv_files, 1):
        print(f"\n[{i}/{len(csv_files)}] Processing: {fpath.name}")
        file_start = datetime.now()
        
        output_path = output_dir / fpath.name
        stats = process_file(fpath, output_path)
        all_stats.append(stats)
        
        file_elapsed = (datetime.now() - file_start).total_seconds()
        print(f"    Time: {file_elapsed:.1f}s")
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print_summary(all_stats)
    
    print(f"\nDone! Total time: {elapsed:.1f}s")
    print(f"Output files saved to: {output_dir}")
    return 0


if __name__ == "__main__":
    exit(main())
    