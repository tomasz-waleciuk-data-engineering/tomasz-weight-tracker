import duckdb

conn = duckdb.connect()

result = conn.execute("""
    -- ============================================
    -- STEP 1: Load raw data and create DateTime
    -- ============================================
    WITH raw AS (
        SELECT 
            *,
            (Date + Time)::TIMESTAMP AS LocalDateTime
        FROM read_csv_auto('For Pandas and Polars --- Electricity.csv')
    ),
    
    -- ============================================
    -- STEP 2: Calculate BST boundaries for each year
    -- BST starts: Last Sunday of March at 01:00
    -- BST ends: Last Sunday of October at 02:00
    -- ============================================
    years AS (
        SELECT DISTINCT YEAR(LocalDateTime) AS yr FROM raw
    ),
    
    bst_boundaries AS (
        SELECT 
            yr,
            -- Last Sunday of March: Start from March 31, go back to find Sunday
            (DATE_TRUNC('month', MAKE_DATE(yr, 4, 1)) - INTERVAL '1 day' 
             - INTERVAL '1 day' * (DAYOFWEEK(DATE_TRUNC('month', MAKE_DATE(yr, 4, 1)) - INTERVAL '1 day') % 7)
            )::DATE + TIME '01:00:00' AS bst_start,
            -- Last Sunday of October: Start from October 31, go back to find Sunday
            (DATE_TRUNC('month', MAKE_DATE(yr, 11, 1)) - INTERVAL '1 day'
             - INTERVAL '1 day' * (DAYOFWEEK(DATE_TRUNC('month', MAKE_DATE(yr, 11, 1)) - INTERVAL '1 day') % 7)
            )::DATE + TIME '02:00:00' AS bst_end
        FROM years
    ),
    
    -- ============================================
    -- STEP 3: Convert local time to UTC
    -- If in BST period, subtract 1 hour
    -- ============================================
    with_utc AS (
        SELECT 
            r.*,
            b.bst_start,
            b.bst_end,
            CASE 
                WHEN r.LocalDateTime >= b.bst_start AND r.LocalDateTime < b.bst_end 
                THEN r.LocalDateTime - INTERVAL '1 hour'
                ELSE r.LocalDateTime
            END AS UTC_DateTime
        FROM raw r
        JOIN bst_boundaries b ON YEAR(r.LocalDateTime) = b.yr
    ),
    
    -- ============================================
    -- STEP 4: Calculate deltas between readings
    -- ============================================
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
    
    -- Filter out last row (no next reading)
    filtered_deltas AS (
        SELECT * FROM with_deltas WHERE Next_DateTime IS NOT NULL
    ),
    
    -- ============================================
    -- STEP 5: Generate 1-minute grid
    -- ============================================
    minute_grid AS (
        SELECT 
            d.UTC_DateTime,
            d.Delta_P,
            d.Delta_OP,
            d.Next_DateTime,
            UNNEST(generate_series(
                d.UTC_DateTime + INTERVAL '1 minute',
                d.Next_DateTime,
                INTERVAL '1 minute'
            )) AS MinuteGrid
        FROM filtered_deltas d
    ),
    
    -- ============================================
    -- STEP 6: Classify each minute as P or OP
    -- P if Time > 06:30 AND Time <= 23:30
    -- ============================================
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
    
    -- ============================================
    -- STEP 7: Count P and OP minutes per interval
    -- ============================================
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
    
    -- ============================================
    -- STEP 8: Calculate per-minute rates
    -- ============================================
    with_rates AS (
        SELECT 
            c.MinuteGrid,
            c.Category,
            c.UTC_DateTime,
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
    ),
    
    -- ============================================
    -- STEP 9: Create 15-minute buckets
    -- Aligned to "end of minute" labeling (subtract 1 minute first)
    -- ============================================
    bucketed AS (
        SELECT 
            DATE_TRUNC('hour', MinuteGrid - INTERVAL '1 minute') 
                + INTERVAL '1 minute' * (EXTRACT(MINUTE FROM MinuteGrid - INTERVAL '1 minute')::INT / 15 * 15) 
            AS Bucket,
            P_Value,
            OP_Value,
            MinuteGrid
        FROM with_rates
    )
    
    -- ============================================
    -- STEP 10: Final aggregation
    -- ============================================
    SELECT 
        Bucket,
        MIN(MinuteGrid) AS MinDateTime,
        MAX(MinuteGrid) AS MaxDateTime,
        COUNT(*) AS Minutes,
        SUM(P_Value) AS P_Usage,
        SUM(OP_Value) AS OP_Usage
    FROM bucketed
    GROUP BY Bucket
    ORDER BY Bucket

""").fetchdf()

# Display results
print("Shape:", result.shape)
print("\nFirst 20 rows:")
print(result.head(20))

# Verify boundaries
print("\nVerifying 06:30 and 23:30 boundaries...")
boundary_check = result[
    (result['Bucket'].dt.hour.isin([6, 23])) & 
    (result['Bucket'].dt.minute.isin([15, 30]))
].head(20)
print(boundary_check)

# Save to CSV
result.to_csv("fifteen_minute_usage_duckdb.csv", index=False)
print(f"\n✓ Saved to fifteen_minute_usage_duckdb.csv ({len(result):,} rows)")

# Summary
print("\n" + "=" * 50)
print("SUMMARY")
print("=" * 50)
print(f"Total P Usage:  {result['P_Usage'].sum():,.2f}")
print(f"Total OP Usage: {result['OP_Usage'].sum():,.2f}")
print(f"Date Range:     {result['Bucket'].min()} to {result['Bucket'].max()}")
print(f"Total Buckets:  {len(result):,}")