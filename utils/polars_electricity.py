import polars as pl
from datetime import datetime, timedelta, time

# 1. Load raw data
df = pl.read_csv("For Pandas and Polars --- Electricity.csv", try_parse_dates=True)

print("Columns found:", df.columns)
print("Shape:", df.shape)
print(df.head())

# 2. BST/GMT conversion functions
def last_sunday(year: int, month: int) -> datetime:
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    days_to_sunday = (last_day.weekday() + 1) % 7
    return last_day - timedelta(days=days_to_sunday)

def is_bst(dt: datetime) -> bool:
    if dt is None:
        return False
    year = dt.year
    bst_start = last_sunday(year, 3).replace(hour=1, minute=0, second=0)
    bst_end = last_sunday(year, 10).replace(hour=2, minute=0, second=0)
    return bst_start <= dt < bst_end

def make_datetime(date_val, time_val) -> datetime:
    """Combine date and time into datetime"""
    if date_val is None or time_val is None:
        return None
    try:
        if isinstance(date_val, str):
            date_val = datetime.strptime(date_val, "%Y-%m-%d").date()
        if isinstance(time_val, str):
            time_val = datetime.strptime(time_val, "%H:%M:%S").time()
        return datetime.combine(date_val, time_val)
    except:
        return None

def convert_to_utc(dt: datetime) -> datetime:
    """Convert local UK time to UTC"""
    if dt is None:
        return None
    if is_bst(dt):
        return dt - timedelta(hours=1)
    return dt

# 3. Create DateTime column if Date and Time are separate
if 'Date' in df.columns and 'Time' in df.columns:
    dates = df['Date'].to_list()
    times = df['Time'].to_list()
    ps = df['P'].to_list()
    ops = df['OP'].to_list()
    
    local_datetimes = [make_datetime(d, t) for d, t in zip(dates, times)]
    utc_datetimes = [convert_to_utc(dt) for dt in local_datetimes]
    
    df = pl.DataFrame({
        'UTC_DateTime': utc_datetimes,
        'P': ps,
        'OP': ops
    })

print("\nAfter UTC conversion:")
print(df.head())

# 4. Sort and calculate deltas
df = df.sort("UTC_DateTime")
df = df.with_columns([
    (pl.col("P").shift(-1) - pl.col("P")).alias("Delta_P"),
    (pl.col("OP").shift(-1) - pl.col("OP")).alias("Delta_OP"),
    pl.col("UTC_DateTime").shift(-1).alias("Next_DateTime")
])

df = df.filter(pl.col("Next_DateTime").is_not_null())

print("\nAfter delta calculation:")
print(df.head())

# 5. Generate 1-minute grid and expand
def expand_minutes(row):
    """Expand a reading interval into 1-minute rows"""
    start = row["UTC_DateTime"]
    end = row["Next_DateTime"]
    delta_p = row["Delta_P"]
    delta_op = row["Delta_OP"]
    
    if end is None or start is None:
        return [], [], []
    
    peak_start = time(6, 30)
    peak_end = time(23, 30)
    
    # First pass: count P and OP minutes
    current = start + timedelta(minutes=1)
    p_mins = 0
    op_mins = 0
    
    temp = current
    while temp <= end:
        t = temp.time()
        if t > peak_start and t <= peak_end:
            p_mins += 1
        else:
            op_mins += 1
        temp += timedelta(minutes=1)
    
    # Calculate rates
    p_rate = float(delta_p) / p_mins if p_mins > 0 else 0.0
    op_rate = float(delta_op) / op_mins if op_mins > 0 else 0.0
    
    # Second pass: generate rows (as separate lists instead of dicts)
    minute_grids = []
    p_values = []
    op_values = []
    
    current = start + timedelta(minutes=1)
    
    while current <= end:
        t = current.time()
        is_peak = t > peak_start and t <= peak_end
        
        minute_grids.append(current)
        p_values.append(p_rate if is_peak else float('nan'))
        op_values.append(op_rate if not is_peak else float('nan'))
        
        current += timedelta(minutes=1)
    
    return minute_grids, p_values, op_values

# Expand all rows using separate lists
print("\nExpanding to 1-minute grid...")
all_minute_grids = []
all_p_values = []
all_op_values = []

total_rows = df.height

for i, row in enumerate(df.iter_rows(named=True)):
    if i % 100 == 0:
        print(f"  Processing row {i}/{total_rows}...")
    
    minute_grids, p_values, op_values = expand_minutes(row)
    all_minute_grids.extend(minute_grids)
    all_p_values.extend(p_values)
    all_op_values.extend(op_values)

print(f"\nTotal 1-minute rows: {len(all_minute_grids):,}")

# Create minute dataframe from separate lists (much more reliable)
minute_df = pl.DataFrame({
    "MinuteGrid": all_minute_grids,
    "P_Value": all_p_values,
    "OP_Value": all_op_values
})

# Replace NaN with null for cleaner output
minute_df = minute_df.with_columns([
    pl.when(pl.col("P_Value").is_nan())
      .then(None)
      .otherwise(pl.col("P_Value"))
      .alias("P_Value"),
    pl.when(pl.col("OP_Value").is_nan())
      .then(None)
      .otherwise(pl.col("OP_Value"))
      .alias("OP_Value")
])

print("\n1-minute data sample:")
print(minute_df.head(20))

# 6. Create 15-minute buckets (aligned to "end of minute" labeling)
minute_df = minute_df.with_columns(
    (pl.col("MinuteGrid") - pl.duration(minutes=1))
    .dt.truncate("15m")
    .alias("Bucket")
)

# 7. Aggregate to 15-minute buckets
result = minute_df.group_by("Bucket").agg([
    pl.col("P_Value").sum().alias("P_Usage"),
    pl.col("OP_Value").sum().alias("OP_Usage"),
    pl.col("MinuteGrid").min().alias("MinDateTime"),
    pl.col("MinuteGrid").max().alias("MaxDateTime"),
    pl.col("MinuteGrid").count().alias("Minutes")
]).sort("Bucket")

print("\nFinal 15-minute buckets:")
print(result.head(20))

# 8. Verify bucket alignment at boundaries
print("\nVerifying 06:30 and 23:30 boundaries...")
boundary_check = result.filter(
    ((pl.col("Bucket").dt.hour() == 6) & (pl.col("Bucket").dt.minute() == 30)) |
    ((pl.col("Bucket").dt.hour() == 23) & (pl.col("Bucket").dt.minute() == 30)) |
    ((pl.col("Bucket").dt.hour() == 6) & (pl.col("Bucket").dt.minute() == 15)) |
    ((pl.col("Bucket").dt.hour() == 23) & (pl.col("Bucket").dt.minute() == 15))
)
print(boundary_check.head(20))

# 9. Export for Power BI
result.write_csv("fifteen_minute_usage_polars.csv")
print(f"\n✓ Saved to fifteen_minute_usage_polars.csv ({result.height:,} rows)")

# 10. Summary stats
print("\n" + "=" * 50)
print("SUMMARY")
print("=" * 50)
print(f"Total P Usage:  {result['P_Usage'].sum():,.2f}")
print(f"Total OP Usage: {result['OP_Usage'].sum():,.2f}")
print(f"Date Range:     {result['Bucket'].min()} to {result['Bucket'].max()}")
print(f"Total Buckets:  {result.height:,}")
