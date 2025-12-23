import duckdb

conn = duckdb.connect()

# Run the query and save directly to CSV (no pandas needed!)
conn.execute("""
    COPY (
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
        -- ============================================
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
        
        -- ============================================
        -- STEP 3: Convert local time to UTC
        -- ============================================
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
        ),
        
        -- ============================================
        -- STEP 10: Final aggregation
        -- ============================================
        final AS (
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
        )
        SELECT * FROM final
    ) TO 'fifteen_minute_usage_duckdb.csv' (HEADER, DELIMITER ',');
""")

print("✓ Saved to fifteen_minute_usage_duckdb.csv")

# Get summary stats (without pandas)
summary = conn.execute("""
    SELECT 
        COUNT(*) AS total_buckets,
        SUM(P_Usage) AS total_p,
        SUM(OP_Usage) AS total_op,
        MIN(Bucket) AS date_start,
        MAX(Bucket) AS date_end
    FROM read_csv_auto('fifteen_minute_usage_duckdb.csv')
""").fetchall()

print("\n" + "=" * 50)
print("SUMMARY")
print("=" * 50)
print(f"Total Buckets:  {summary[0][0]:,}")
print(f"Total P Usage:  {summary[0][1]:,.2f}")
print(f"Total OP Usage: {summary[0][2]:,.2f}")
print(f"Date Range:     {summary[0][3]} to {summary[0][4]}")

conn.close()