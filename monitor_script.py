#!/usr/bin/env python3
"""
Monitor script for OHLCV Data Collector
Displays status and statistics about collected data
Enhanced to show trading downtime information
"""

import os
import json
import pandas as pd
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path
from tabulate import tabulate

DATA_DIR = "ohlc_data"
STATUS_FILE = "status.json"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_downtime_info(symbol: str, timeframe: str) -> str:
    """Get information about data freshness and gaps"""
    csv_path = f"{DATA_DIR}/{symbol}/{timeframe}.csv"
    if not os.path.exists(csv_path):
        return "No data file"
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            return "Empty file"
        
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        last_date = df['Datetime'].max()
        now = datetime.now()
        age = now - last_date
        
        # Check if the gap is entirely within a scheduled break
        if _is_legit_session_gap(symbol, last_date, now):
            return "Market closed"
        
        # Format the age
        if age.days > 0:
            return f"{age.days}d {age.seconds//3600}h old"
        elif age.seconds >= 3600:
            return f"{age.seconds//3600}h {age.seconds%3600//60}m old"
        else:
            return f"{age.seconds//60}m old"
            
    except Exception as e:
        return f"Error: {str(e)}"

def format_time_ago(timestamp_str):
    """Format timestamp as 'X minutes/hours/days ago'"""
    if not timestamp_str:
        return "Never"
    
    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    now = datetime.now(timezone.utc)
    delta = now - timestamp
    
    # Check if it's weekend
    is_weekend = now.weekday() >= 5
    
    # Check if it's outside market hours (assuming 9:30-16:00 ET for equities)
    is_market_hours = 9 <= now.hour <= 16
    
    # If more than 3 hours old and (weekend or outside market hours), show "market closed"
    if delta.total_seconds() > 10800 and (is_weekend or not is_market_hours):
        return "market closed"
    
    if delta.total_seconds() < 60:
        return f"{int(delta.total_seconds())}s ago"
    elif delta.total_seconds() < 3600:
        return f"{int(delta.total_seconds() / 60)}m ago"
    elif delta.total_seconds() < 86400:
        return f"{int(delta.total_seconds() / 3600)}h ago"
    else:
        return f"{int(delta.total_seconds() / 86400)}d ago"

def get_csv_stats(csv_path):
    """Get statistics about a CSV file"""
    if not os.path.exists(csv_path):
        return {"rows": 0, "first": "N/A", "last": "N/A"}
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            return {"rows": 0, "first": "N/A", "last": "N/A"}
        
        return {
            "rows": len(df),
            "first": df['Datetime'].iloc[0],
            "last": df['Datetime'].iloc[-1]
        }
    except:
        return {"rows": 0, "first": "Error", "last": "Error"}

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Monitor OHLCV data collection status')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    
    # Set logging level based on verbose flag
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    print("=" * 80)
    print("OHLCV Data Collection Monitor")
    print("=" * 80)
    print()
    
    # Load status
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, 'r') as f:
            status = json.load(f)
    else:
        print("No status file found. Collector may not have run yet.")
        status = {}
    
    # Get all symbols
    symbols = []
    if os.path.exists(DATA_DIR):
        symbols = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]
        symbols.sort()
    
    if not symbols:
        print("No data directories found.")
        return
    
    # Timeframes
    timeframes = ["1m", "5m", "15m", "1h", "4h", "1d", "1W", "1mo", "3mo"]
    
    # Summary statistics
    total_files = 0
    total_rows = 0
    
    logger.info(f"Found {len(symbols)} symbols")
    
    # Detailed status for each symbol
    for symbol in symbols:
        logger.debug(f"\n{symbol.upper()}")
        logger.debug("-" * 40)
        
        # Check for downtime info
        downtime = get_downtime_info(symbol, "1m")
        if downtime != "No data file" and downtime != "Empty file":
            logger.debug(f"Latest data freshness: {downtime}")
        
        table_data = []
        symbol_status = status.get(symbol, {})
        
        for timeframe in timeframes:
            csv_path = f"{DATA_DIR}/{symbol}/{timeframe}.csv"
            stats = get_csv_stats(csv_path)
            last_update = symbol_status.get(timeframe, "")
            time_ago = format_time_ago(last_update)
            
            if stats["rows"] > 0:
                total_files += 1
                total_rows += stats["rows"]
            
            table_data.append([
                timeframe,
                stats["rows"],
                stats["first"][:16] if stats["first"] != "N/A" else "N/A",
                stats["last"][:16] if stats["last"] != "N/A" else "N/A",
                time_ago
            ])
        
        headers = ["Timeframe", "Rows", "First", "Last", "Updated"]
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    # Overall summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total symbols: {len(symbols)}")
    print(f"Total files: {total_files}")
    print(f"Total data rows: {total_rows:,}")
    
    # Check for errors
    if os.path.exists("errors.log"):
        # Get last 5 lines of error log
        with open("errors.log", 'r') as f:
            lines = f.readlines()
            if lines:
                print(f"\nRecent errors (last 5):")
                print("-" * 40)
                for line in lines[-5:]:
                    print(line.strip()[:100])

if __name__ == "__main__":
    main()
