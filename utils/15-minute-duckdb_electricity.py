import duckdb

conn = duckdb.connect()

# Step 1: Create 1-minute table
conn.execute("""
    CREATE OR REPLACE TABLE minute_data AS
    
    WITH raw AS (
        SELECT 
            *,
            (Date + Time)::TIMESTAMP AS LocalDateTime
        FROM read_csv_auto('For Pandas and Polars --- Electricity.csv')
    ),
    
    years AS (
        SELECT DISTINCT YEAR(LocalDateTime) AS yr FROM raw
    ),
    
    bst_boundaries AS (
        SELECT 
            yr,
            (DATE_TRUNC('month', MAKE_DATE(yr, 4, 1)) - INTERVAL '1 day' 
             - INTERVAL '1 day' * (DAYOFWEEK(DATE_TRUNC('month', MAKE_DATE(yr, 4, 1)) - INTERVAL '1 day') % 7)
            )::DATE + TIME '01:00:00' AS bst_start,
            (DATE_TRUNC('month', MAKE_DATE(yr, 11, 1)) - INTERVAL '1 day'
             - INTERVAL '1 day' * (DAYOFWEEK(DATE_TRUNC('month', MAKE_DATE(yr, 11, 1)) - INTERVAL '1 day') % 7)
            )::DATE + TIME '02:00:00' AS bst_end
        FROM years
    ),
    
    with_utc AS (
        SELECT 
            r.*,
            CASE 
                WHEN r.LocalDateTime >= b.bst_start AND r.LocalDateTime < b.bst_end 
                THEN r.LocalDateTime - INTERVAL '1 hour'
                ELSE r.LocalDateTime
            END AS UTC_DateTime
        FROM raw r
        JOIN bst_boundaries b ON YEAR(r.LocalDateTime) = b.yr
    ),
    
    with_deltas AS (
        SELECT 
            UTC_DateTime,
            P,
            OP,
            LEAD(P) OVER (ORDER BY UTC_DateTime) - P AS Delta_P,
            LEAD(OP) OVER (ORDER BY UTC_DateTime) - OP AS Delta_OP,
            LEAD(UTC_DateTime) OVER (ORDER BY UTC_DateTime) AS Next_DateTime
        FROM with_utc
    ),
    
    filtered_deltas AS (
        SELECT * FROM with_deltas WHERE Next_DateTime IS NOT NULL
    ),
    
    minute_grid AS (
        SELECT 
            d.UTC_DateTime,
            d.Delta_P,
            d.Delta_OP,
            UNNEST(generate_series(
                d.UTC_DateTime + INTERVAL '1 minute',
                d.Next_DateTime,
                INTERVAL '1 minute'
            )) AS MinuteGrid
        FROM filtered_deltas d
    ),
    
    with_category AS (
        SELECT 
            *,
            CASE 
                WHEN CAST(MinuteGrid AS TIME) > TIME '06:30:00' 
                 AND CAST(MinuteGrid AS TIME) <= TIME '23:30:00' 
                THEN 'P' 
                ELSE 'OP' 
            END AS Category
        FROM minute_grid
    ),
    
    interval_counts AS (
        SELECT 
            UTC_DateTime,
            Delta_P,
            Delta_OP,
            SUM(CASE WHEN Category = 'P' THEN 1 ELSE 0 END) AS Total_P_Mins,
            SUM(CASE WHEN Category = 'OP' THEN 1 ELSE 0 END) AS Total_OP_Mins
        FROM with_category
        GROUP BY UTC_DateTime, Delta_P, Delta_OP
    ),
    
    with_rates AS (
        SELECT 
            c.MinuteGrid,
            DATE_TRUNC('minute', c.MinuteGrid) AS MinuteGridClean,
            CASE 
                WHEN c.Category = 'P' AND i.Total_P_Mins > 0 
                THEN c.Delta_P / i.Total_P_Mins 
                ELSE NULL 
            END AS P_Value,
            CASE 
                WHEN c.Category = 'OP' AND i.Total_OP_Mins > 0 
                THEN c.Delta_OP / i.Total_OP_Mins 
                ELSE NULL 
            END AS OP_Value
        FROM with_category c
        JOIN interval_counts i ON c.UTC_DateTime = i.UTC_DateTime
    )
    
    SELECT * FROM with_rates;
""")

print("✓ Created minute_data table")

minute_count = conn.execute("SELECT COUNT(*) FROM minute_data").fetchone()[0]
print(f"  Total 1-minute rows: {minute_count:,}")

# Step 2: Aggregate to 15-minute buckets (REMOVED the + INTERVAL '1 minute')
conn.execute("""
    COPY (
        SELECT 
            TIME_BUCKET(INTERVAL '15 minutes', MinuteGridClean - INTERVAL '1 minute') AS Bucket,
            MIN(MinuteGrid) AS MinDateTime,
            MAX(MinuteGrid) AS MaxDateTime,
            COUNT(*) AS Minutes,
            SUM(P_Value) AS P_Usage,
            SUM(OP_Value) AS OP_Usage
        FROM minute_data
        GROUP BY 1
        ORDER BY 1
    ) TO 'fifteen_minute_usage_duckdb.csv' (HEADER, DELIMITER ',');
""")

print("✓ Saved to fifteen_minute_usage_duckdb.csv")

# Get summary stats
summary = conn.execute("""
    SELECT 
        COUNT(*) AS total_buckets,
        SUM(P_Usage) AS total_p,
        SUM(OP_Usage) AS total_op,
        MIN(Bucket) AS date_start,
        MAX(Bucket) AS date_end
    FROM read_csv_auto('fifteen_minute_usage_duckdb.csv')
""").fetchone()

print("\n" + "=" * 50)
print("SUMMARY")
print("=" * 50)
print(f"Total Buckets:  {summary[0]:,}")
print(f"Total P Usage:  {summary[1]:,.2f}")
print(f"Total OP Usage: {summary[2]:,.2f}")
print(f"Date Range:     {summary[3]} to {summary[4]}")

# Verify boundary buckets
print("\nVerifying 06:30 and 23:30 boundaries...")
boundaries = conn.execute("""
    SELECT * FROM read_csv_auto('fifteen_minute_usage_duckdb.csv')
    WHERE EXTRACT(HOUR FROM Bucket) IN (6, 23)
      AND EXTRACT(MINUTE FROM Bucket) IN (15, 30)
    ORDER BY Bucket
    LIMIT 20
""").fetchall()

for row in boundaries:
    p_val = f"{row[4]:.4f}" if row[4] else "null"
    op_val = f"{row[5]:.4f}" if row[5] else "null"
    print(f"Bucket: {row[0]}, Minutes: {row[3]}, P: {p_val}, OP: {op_val}")

conn.close()
