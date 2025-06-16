def get_downtime_info(symbol):
    """Get latest trading downtime info for a symbol"""
    downtime_file = f"{DATA_DIR}/{symbol}/trading_downtime.csv"
    if os.path.exists(downtime_file):
        try:
            df = pd.read_csv(downtime_file)
            if not df.empty:
                # Get the most recent downtime
                df['Start'] = pd.to_datetime(df['Start'])
                df['End'] = pd.to_datetime(df['End'])
                latest = df.sort_values('End', ascending=False).iloc[0]
                return f"{latest['Start'].strftime('%Y-%m-%d %H:%M')} - {latest['End'].strftime('%H:%M')}"
        except:
            pass
    return "None"

"""
Monitor script for OHLCV Data Collector
Displays status and statistics about collected data
Enhanced to show trading downtime information
"""

import os
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from tabulate import tabulate

DATA_DIR = "ohlc_data"
STATUS_FILE = "status.json"
NEWS_FILE = "news_events.csv"

def format_time_ago(timestamp_str):
    """Format timestamp as 'X minutes/hours/days ago'"""
    if not timestamp_str:
        return "Never"
    
    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    now = datetime.now(timezone.utc)
    delta = now - timestamp
    
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

def display_news_events():
    """Display upcoming high-impact news events"""
    print("\n" + "=" * 80)
    print("UPCOMING HIGH-IMPACT NEWS")
    print("=" * 80)
    
    if os.path.exists(NEWS_FILE):
        try:
            df = pd.read_csv(NEWS_FILE)
            if not df.empty:
                df['EventTime'] = pd.to_datetime(df['EventTime']).dt.tz_localize('UTC')
                
                # Filter for events in the next 7 days
                now = pd.Timestamp.now(tz='UTC')
                future_events = df[df['EventTime'].between(now, now + timedelta(days=7))]

                if not future_events.empty:
                    print("\nUpcoming News Events (Next 7 Days):")
                    
                    # Prepare data for tabulate
                    table_data = future_events[['EventTime', 'Currency', 'Importance', 'Event']].copy()
                    table_data['EventTime'] = table_data['EventTime'].dt.strftime('%Y-%m-%d %H:%M')
                    
                    print(tabulate(table_data, headers=['Time (UTC)', 'Currency', 'Importance', 'Event'], tablefmt="grid"))
                else:
                    print("\nNo upcoming news events in the next 7 days.")
        except Exception as e:
            print(f"\nCould not read news events file: {e}")

def main():
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
    timeframes = ["1m", "5m", "15m", "1h", "4h", "1d", "1W", "1Mo", "3M"]
    
    # Summary statistics
    total_files = 0
    total_rows = 0
    
    print(f"Found {len(symbols)} symbols\n")
    
    # Detailed status for each symbol
    for symbol in symbols:
        print(f"\n{symbol.upper()}")
        print("-" * 40)
        
        # Check for downtime info
        downtime = get_downtime_info(symbol)
        if downtime != "None":
            print(f"Latest news downtime: {downtime}")
        
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
    display_news_events()
    
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
