"""
Dukascopy Tick Data Downloader
Downloads EUR/USD tick data hour by hour with resume capability
"""

import os
import sys
import time
import json
import lzma
import struct
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============== CONFIGURATION ==============

CONFIG = {
    # What to download
    "symbol": "EURUSD",
    "start_date": "2016-06-21",
    "end_date": "2016-06-21",
    
    # Rate limiting (be respectful!)
    "delay_between_requests": 0.5,      # seconds
    "delay_between_days": 2.0,          # seconds  
    "max_retries": 3,
    "retry_delay": 5.0,
    
    # Parallel downloads (careful - don't abuse)
    "parallel_downloads": 1,            # 1 = sequential (safest)
    
    # Storage
    "output_dir": "./tick_data",
    "save_format": "parquet",           # 'parquet' (smaller) or 'csv'
    "combine_daily": True,              # Combine hourly files into daily
    
    # Progress tracking
    "progress_file": "./download_progress.json",
    
    # Dukascopy specific
    "point_value": 100000,              # For 5-digit pairs like EURUSD
}

# ============== SETUP LOGGING ==============

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============== DUKASCOPY URL BUILDER ==============

def get_dukascopy_url(symbol: str, dt: datetime, hour: int) -> str:
    """
    Build Dukascopy URL for tick data.
    Note: Month is 0-indexed in Dukascopy URLs!
    """
    base_url = "https://datafeed.dukascopy.com/datafeed"
    year = dt.year
    month = dt.month - 1  # 0-indexed!
    day = dt.day
    
    url = f"{base_url}/{symbol}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
    return url

# ============== BI5 DECODER ==============

def decode_bi5(compressed_data: bytes, hour_start: datetime, point_value: int) -> List[dict]:
    """
    Decode Dukascopy .bi5 format (LZMA compressed binary).
    
    Structure per tick (20 bytes):
    - int32: milliseconds from hour start
    - int32: ask price (needs division by point_value)
    - int32: bid price
    - float32: ask volume
    - float32: bid volume
    """
    ticks = []
    
    try:
        # Decompress LZMA
        decompressed = lzma.decompress(compressed_data)
    except lzma.LZMAError as e:
        logger.warning(f"LZMA decompression failed: {e}")
        return ticks
    
    # Each tick is 20 bytes
    tick_size = 20
    num_ticks = len(decompressed) // tick_size
    
    for i in range(num_ticks):
        offset = i * tick_size
        chunk = decompressed[offset:offset + tick_size]
        
        if len(chunk) < tick_size:
            break
            
        # Unpack: big-endian int32, int32, int32, float32, float32
        ms_offset, ask_int, bid_int, ask_vol, bid_vol = struct.unpack(
            '>IIIff', chunk
        )
        
        # Calculate timestamp
        timestamp = hour_start + timedelta(milliseconds=ms_offset)
        
        # Convert prices
        ask = ask_int / point_value
        bid = bid_int / point_value
        
        ticks.append({
            'timestamp': timestamp,
            'bid': bid,
            'ask': ask,
            'bid_volume': bid_vol,
            'ask_volume': ask_vol
        })
    
    return ticks

# ============== DOWNLOAD FUNCTIONS ==============

def download_hour(symbol: str, dt: datetime, hour: int, 
                  point_value: int, max_retries: int = 3) -> Tuple[List[dict], bool]:
    """
    Download tick data for a specific hour.
    Returns (ticks, success)
    """
    url = get_dukascopy_url(symbol, dt, hour)
    hour_start = dt.replace(hour=hour, minute=0, second=0, microsecond=0)
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                if len(response.content) == 0:
                    # Empty file = no trading (weekend/holiday)
                    return [], True
                
                ticks = decode_bi5(response.content, hour_start, point_value)
                return ticks, True
                
            elif response.status_code == 404:
                # No data for this hour (normal for weekends)
                return [], True
                
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
            time.sleep(CONFIG["retry_delay"])
    
    return [], False

def download_day(symbol: str, date: datetime, config: dict) -> Tuple[pd.DataFrame, bool]:
    """
    Download all 24 hours for a given day.
    """
    all_ticks = []
    success = True
    
    for hour in range(24):
        ticks, hour_success = download_hour(
            symbol, date, hour,
            config["point_value"],
            config["max_retries"]
        )
        
        if not hour_success:
            success = False
            logger.error(f"Failed to download {date.date()} hour {hour}")
        
        all_ticks.extend(ticks)
        
        # Rate limiting
        time.sleep(config["delay_between_requests"])
    
    if all_ticks:
        df = pd.DataFrame(all_ticks)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)
        return df, success
    
    return pd.DataFrame(), success

# ============== PROGRESS TRACKING ==============

def load_progress(filepath: str) -> dict:
    """Load download progress from file."""
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)
    return {"completed_dates": [], "failed_dates": []}

def save_progress(filepath: str, progress: dict):
    """Save download progress to file."""
    with open(filepath, 'w') as f:
        json.dump(progress, f, indent=2, default=str)

# ============== DATA STORAGE ==============

def save_daily_data(df: pd.DataFrame, date: datetime, output_dir: str, format: str):
    """Save daily tick data to file."""
    if df.empty:
        return
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    date_str = date.strftime("%Y%m%d")
    
    if format == "parquet":
        filepath = os.path.join(output_dir, f"EURUSD_{date_str}.parquet")
        df.to_parquet(filepath, index=False, compression='snappy')
    else:
        filepath = os.path.join(output_dir, f"EURUSD_{date_str}.csv")
        df.to_csv(filepath, index=False)
    
    logger.info(f"Saved {len(df):,} ticks to {filepath}")

# ============== MAIN DOWNLOADER ==============

def generate_date_range(start_date: str, end_date: str) -> List[datetime]:
    """Generate list of dates to download."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    dates = []
    current = start
    while current < end:
        # Skip weekends (Forex market closed)
        if current.weekday() < 5:  # Monday = 0, Friday = 4
            dates.append(current)
        current += timedelta(days=1)
    
    return dates

def run_downloader(config: dict):
    """Main download loop with progress tracking and resume."""
    
    logger.info("=" * 60)
    logger.info("DUKASCOPY TICK DATA DOWNLOADER")
    logger.info("=" * 60)
    logger.info(f"Symbol: {config['symbol']}")
    logger.info(f"Period: {config['start_date']} to {config['end_date']}")
    logger.info(f"Output: {config['output_dir']}")
    logger.info("=" * 60)
    
    # Load progress
    progress = load_progress(config["progress_file"])
    completed = set(progress["completed_dates"])
    
    logger.info(f"Previously completed: {len(completed)} days")
    
    # Generate dates to download
    all_dates = generate_date_range(config["start_date"], config["end_date"])
    remaining_dates = [d for d in all_dates if d.strftime("%Y-%m-%d") not in completed]
    
    logger.info(f"Remaining to download: {len(remaining_dates)} days")
    
    if not remaining_dates:
        logger.info("All dates already downloaded!")
        return
    
    # Estimate time
    est_time_hours = (len(remaining_dates) * (24 * config["delay_between_requests"] + config["delay_between_days"])) / 3600
    logger.info(f"Estimated time: {est_time_hours:.1f} hours ({est_time_hours/24:.1f} days)")
    
    input("\nPress Enter to start downloading (Ctrl+C to abort)...")
    
    # Download loop
    total_ticks = 0
    start_time = time.time()
    
    for i, date in enumerate(remaining_dates):
        date_str = date.strftime("%Y-%m-%d")
        
        logger.info(f"\n[{i+1}/{len(remaining_dates)}] Downloading {date_str}...")
        
        try:
            df, success = download_day(config["symbol"], date, config)
            
            if success:
                if not df.empty:
                    save_daily_data(df, date, config["output_dir"], config["save_format"])
                    total_ticks += len(df)
                
                # Update progress
                progress["completed_dates"].append(date_str)
                save_progress(config["progress_file"], progress)
                
                logger.info(f"✓ Completed {date_str}: {len(df):,} ticks")
            else:
                progress["failed_dates"].append(date_str)
                save_progress(config["progress_file"], progress)
                logger.error(f"✗ Failed {date_str}")
            
            # Progress stats
            elapsed = time.time() - start_time
            rate = (i + 1) / (elapsed / 3600)  # days per hour
            remaining = len(remaining_dates) - i - 1
            eta_hours = remaining / rate if rate > 0 else 0
            
            logger.info(f"Progress: {total_ticks:,} total ticks | "
                       f"Speed: {rate:.1f} days/hour | "
                       f"ETA: {eta_hours:.1f} hours")
            
            # Delay between days
            time.sleep(config["delay_between_days"])
            
        except KeyboardInterrupt:
            logger.info("\n\nDownload paused by user. Progress saved.")
            logger.info(f"Run script again to resume from {date_str}")
            save_progress(config["progress_file"], progress)
            sys.exit(0)
    
    # Final summary
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("DOWNLOAD COMPLETE!")
    logger.info(f"Total ticks: {total_ticks:,}")
    logger.info(f"Total time: {elapsed/3600:.1f} hours")
    logger.info(f"Failed dates: {len(progress['failed_dates'])}")
    logger.info("=" * 60)

# ============== UTILITY: COMBINE FILES ==============

def combine_to_monthly(input_dir: str, output_dir: str, format: str = "parquet"):
    """
    Combine daily files into monthly files to reduce file count.
    """
    from glob import glob
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Find all daily files
    pattern = os.path.join(input_dir, f"EURUSD_*.{format}")
    files = sorted(glob(pattern))
    
    # Group by month
    monthly_data = {}
    
    for filepath in files:
        filename = os.path.basename(filepath)
        date_str = filename.split('_')[1].split('.')[0]  # YYYYMMDD
        month_key = date_str[:6]  # YYYYMM
        
        if month_key not in monthly_data:
            monthly_data[month_key] = []
        monthly_data[month_key].append(filepath)
    
    # Combine each month
    for month_key, filepaths in monthly_data.items():
        logger.info(f"Combining {month_key}: {len(filepaths)} files")
        
        dfs = []
        for fp in filepaths:
            if format == "parquet":
                dfs.append(pd.read_parquet(fp))
            else:
                dfs.append(pd.read_csv(fp))
        
        combined = pd.concat(dfs, ignore_index=True)
        combined = combined.sort_values('timestamp').reset_index(drop=True)
        
        output_path = os.path.join(output_dir, f"EURUSD_{month_key}.{format}")
        
        if format == "parquet":
            combined.to_parquet(output_path, index=False, compression='snappy')
        else:
            combined.to_csv(output_path, index=False)
        
        logger.info(f"Saved {len(combined):,} ticks to {output_path}")

# ============== ENTRY POINT ==============

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Download Dukascopy tick data")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--delay", type=float, help="Delay between requests (seconds)")
    parser.add_argument("--combine", action="store_true", help="Combine daily files to monthly")
    
    args = parser.parse_args()
    
    # Override config with command line args
    if args.start:
        CONFIG["start_date"] = args.start
    if args.end:
        CONFIG["end_date"] = args.end
    if args.delay:
        CONFIG["delay_between_requests"] = args.delay
    
    if args.combine:
        combine_to_monthly(CONFIG["output_dir"], CONFIG["output_dir"] + "_monthly")
    else:
        run_downloader(CONFIG)
        