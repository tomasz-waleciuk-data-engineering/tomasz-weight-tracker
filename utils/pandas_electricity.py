import pandas as pd
from datetime import datetime, timedelta, time
import warnings
warnings.filterwarnings('ignore')

print("=" * 50)
print("PANDAS ELECTRICITY PROCESSING")
print("=" * 50)

# 1. Load raw data
print("\n[1/8] Loading data...")
df = pd.read_csv("For Pandas and Polars --- Electricity.csv", parse_dates=['Date'])
df['Time'] = pd.to_timedelta(df['Time'].astype(str))

print(f"  Columns: {df.columns.tolist()}")
print(f"  Shape: {df.shape}")
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
    if pd.isna(dt):
        return False
    year = dt.year
    bst_start = last_sunday(year, 3).replace(hour=1, minute=0, second=0)
    bst_end = last_sunday(year, 10).replace(hour=2, minute=0, second=0)
    return bst_start <= dt < bst_end

def convert_to_utc(dt: datetime) -> datetime:
    if pd.isna(dt):
        return None
    if is_bst(dt):
        return dt - timedelta(hours=1)
    return dt

# 3. Create DateTime and convert to UTC
print("\n[2/8] Converting to UTC...")
df['LocalDateTime'] = df['Date'] + df['Time']
df['UTC_DateTime'] = df['LocalDateTime'].apply(convert_to_utc)
df = df.sort_values('UTC_DateTime').reset_index(drop=True)

print(df[['LocalDateTime', 'UTC_DateTime', 'P', 'OP']].head())

# 4. Calculate deltas
print("\n[3/8] Calculating deltas...")
df['Delta_P'] = df['P'].shift(-1) - df['P']
df['Delta_OP'] = df['OP'].shift(-1) - df['OP']
df['Next_DateTime'] = df['UTC_DateTime'].shift(-1)

# Remove last row
df = df[df['Next_DateTime'].notna()].copy()

print(df[['UTC_DateTime', 'Next_DateTime', 'Delta_P', 'Delta_OP']].head())

# 5. Generate 1-minute grid
print("\n[4/8] Expanding to 1-minute grid (this takes a while)...")

peak_start = time(6, 30)
peak_end = time(23, 30)

def expand_interval(row):
    """Expand a single interval to 1-minute rows"""
    start = row['UTC_DateTime']
    end = row['Next_DateTime']
    delta_p = row['Delta_P']
    delta_op = row['Delta_OP']
    
    if pd.isna(end):
        return []
    
    # Generate minute timestamps
    current = start + timedelta(minutes=1)
    
    # First pass: count P and OP minutes
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
    p_rate = delta_p / p_mins if p_mins > 0 else 0.0
    op_rate = delta_op / op_mins if op_mins > 0 else 0.0
    
    # Second pass: generate rows
    rows = []
    current = start + timedelta(minutes=1)
    
    while current <= end:
        t = current.time()
        is_peak = t > peak_start and t <= peak_end
        rows.append({
            'MinuteGrid': current,
            'P_Value': p_rate if is_peak else None,
            'OP_Value': op_rate if not is_peak else None
        })
        current += timedelta(minutes=1)
    
    return rows

# Process all intervals
all_minutes = []
total_rows = len(df)

for idx, row in df.iterrows():
    if idx % 100 == 0:
        print(f"  Processing interval {idx}/{total_rows}...")
    all_minutes.extend(expand_interval(row))

print(f"\n  Total 1-minute rows: {len(all_minutes):,}")

# 6. Create minute DataFrame
print("\n[5/8] Creating minute DataFrame...")
minute_df = pd.DataFrame(all_minutes)

print(minute_df.head(20))

# 7. Create 15-minute buckets
print("\n[6/8] Creating 15-minute buckets...")

def calculate_bucket(dt):
    """Calculate 15-minute bucket aligned to 'end of minute' labeling"""
    adjusted = dt - timedelta(minutes=1)
    bucket_minute = (adjusted.minute // 15) * 15
    return adjusted.replace(minute=bucket_minute, second=0, microsecond=0)

minute_df['Bucket'] = minute_df['MinuteGrid'].apply(calculate_bucket)

# 8. Aggregate to 15-minute buckets
print("\n[7/8] Aggregating to 15-minute buckets...")

result = minute_df.groupby('Bucket').agg(
    MinDateTime=('MinuteGrid', 'min'),
    MaxDateTime=('MinuteGrid', 'max'),
    Minutes=('MinuteGrid', 'count'),
    P_Usage=('P_Value', 'sum'),
    OP_Usage=('OP_Value', 'sum')
).reset_index()

result = result.sort_values('Bucket').reset_index(drop=True)

print(f"\n  Total 15-minute buckets: {len(result):,}")
print(result.head(20))

# 9. Verify boundaries
print("\n[8/8] Verifying 06:30 and 23:30 boundaries...")

boundary_check = result[
    (result['Bucket'].dt.hour.isin([6, 23])) & 
    (result['Bucket'].dt.minute.isin([15, 30]))
].head(20)

for _, row in boundary_check.iterrows():
    p_val = f"{row['P_Usage']:.4f}" if pd.notna(row['P_Usage']) else "null"
    op_val = f"{row['OP_Usage']:.4f}" if pd.notna(row['OP_Usage']) else "null"
    print(f"Bucket: {row['Bucket']}, Minutes: {row['Minutes']}, P: {p_val}, OP: {op_val}")

# 10. Export
result.to_csv("fifteen_minute_usage_pandas.csv", index=False)
print(f"\n✓ Saved to fifteen_minute_usage_pandas.csv ({len(result):,} rows)")

# Summary
print("\n" + "=" * 50)
print("SUMMARY")
print("=" * 50)
print(f"Total Buckets:  {len(result):,}")
print(f"Total P Usage:  {result['P_Usage'].sum():,.2f}")
print(f"Total OP Usage: {result['OP_Usage'].sum():,.2f}")
print(f"Date Range:     {result['Bucket'].min()} to {result['Bucket'].max()}")
