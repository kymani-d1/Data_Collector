#!/usr/bin/env python3
"""
Data Validator for OHLCV CSV files
Checks for data integrity issues like gaps, duplicates, and invalid values
Enhanced to handle public holidays and market sessions
"""

import os
import pandas as pd
import holidays
import pytz
from datetime import datetime, timedelta, time, timezone
from pathlib import Path
from sessions import SESSIONS, closed_window

DATA_DIR = "ohlc_data"

# Holiday calendars
HOLIDAY_CALENDARS = {
    'USD': holidays.US(),
    'GBP': holidays.UK(),
    'EUR': holidays.ECB(),
    'JPY': holidays.Japan(),
    'CHF': holidays.Switzerland(),
    'AUD': holidays.Australia()
}

# Expected intervals in minutes
INTERVAL_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "1W": 10080,
    "1mo": 43200,  # Approximate
    "3mo": 129600  # Approximate
}

def get_symbol_currencies(symbol: str):
    """Extract currencies from a symbol"""
    currencies = set()
    
    # Forex pairs
    if len(symbol) == 6 and symbol.isalpha():
        currencies.add(symbol[:3].upper())
        currencies.add(symbol[3:].upper())
    # USD-based assets
    elif symbol in ['gold', 'silver', 'natgas', 'spy', 'nasdaq', 'sp500', 'oil', 'copper', 'btc', 'eth']:
        currencies.add('USD')
        
    return currencies

def is_holiday(date, symbol):
    """Check if date is a holiday for the given symbol"""
    currencies = get_symbol_currencies(symbol)
    
    for currency in currencies:
        if currency in HOLIDAY_CALENDARS:
            if date.date() in HOLIDAY_CALENDARS[currency]:
                return True
    return False

def validate_csv(csv_path, timeframe, symbol):
    """Validate a single CSV file"""
    issues = []
    
    if not os.path.exists(csv_path):
        return ["File does not exist"]
    
    try:
        df = pd.read_csv(csv_path)
        
        # Check if empty
        if df.empty:
            return ["File is empty"]
        
        # Check columns
        required_cols = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
            return issues
        
        # Parse as UTC so every stamp is tz-aware
        # (handles both "2025-06-13 20:58:00" and "2025-06-13 20:58:00+00:00")
        df['Datetime'] = pd.to_datetime(df['Datetime'], utc=True)
        
        # Check for duplicates
        duplicates = df[df.duplicated(subset=['Datetime'])].shape[0]
        if duplicates > 0:
            issues.append(f"Found {duplicates} duplicate timestamps")
        
        # Check OHLC validity
        invalid_ohlc = df[(df['High'] < df['Low']) | 
                         (df['High'] < df['Open']) | 
                         (df['High'] < df['Close']) |
                         (df['Low'] > df['Open']) |
                         (df['Low'] > df['Close'])].shape[0]
        if invalid_ohlc > 0:
            issues.append(f"Found {invalid_ohlc} bars with invalid OHLC values")
        
        # Check for negative values
        negative_values = df[(df['Open'] < 0) | (df['High'] < 0) | 
                           (df['Low'] < 0) | (df['Close'] < 0)].shape[0]
        if negative_values > 0:
            issues.append(f"Found {negative_values} bars with negative prices")
        
        # ————— Intraday gap checking —————
        is_crypto = symbol.lower() in ['btc','eth']
        if timeframe in ["1m","5m","15m","1h","4h"] and not is_crypto:
            df = df.sort_values('Datetime')

            # allow up to two intervals to slip by on 1m; otherwise exact interval
            interval = INTERVAL_MINUTES[timeframe]
            max_allowed_gap = timedelta(minutes=interval * (2 if timeframe=="1m" else 1))

            gaps = []
            retention_cutoff = datetime.now(timezone.utc) - timedelta(days=7)

            for i in range(1, len(df)):
                Δ = df['Datetime'].iloc[i] - df['Datetime'].iloc[i-1]
                # only flag holes bigger than our allowance, but shorter than 3 days
                if max_allowed_gap < Δ < timedelta(days=3):
                    start, end = df['Datetime'].iloc[i-1], df['Datetime'].iloc[i]

                    # skip holes entirely outside Yahoo's 7-day window
                    if timeframe=="1m" and end <= retention_cutoff:
                        continue

                    # skip scheduled closures (weekends, nightly breaks) or holidays
                    if closed_window(symbol, start, end, timeframe) or is_holiday(pd.Timestamp(start), symbol):
                        continue

                    gaps.append((start, end, Δ))

            if gaps:
                count = len(gaps)
                shown = gaps if count<=5 else gaps[:5]
                issues.append(f"Found {count} time gaps (showing first {min(5,count)})")
                for s,e,Δ in shown:
                    issues.append(f"  - {s} to {e} ({Δ})")
        
        # Check data freshness
        last_date = df['Datetime'].max()
        now = datetime.now(timezone.utc)  # Use UTC for comparison
        
        # Skip freshness test if the window is inside a scheduled closure
        if closed_window(symbol, last_date, now, timeframe):
            pass  # Market closed – don't complain about age
        else:
            age = now - last_date
            
            if timeframe == "1m" and age > timedelta(hours=1):
                issues.append(f"Data is {age} old (last: {last_date})")
            elif timeframe in ["5m", "15m"] and age > timedelta(hours=2):
                issues.append(f"Data is {age} old (last: {last_date})")
            elif timeframe == "1h" and age > timedelta(hours=6):
                issues.append(f"Data is {age} old (last: {last_date})")
            elif timeframe == "4h" and age > timedelta(hours=12):
                issues.append(f"Data is {age} old (last: {last_date})")
            elif timeframe == "1d" and age > timedelta(days=2):
                issues.append(f"Data is {age} old (last: {last_date})")
        
        if not issues:
            return [f"✓ Valid - {len(df)} rows, last: {last_date.strftime('%Y-%m-%d %H:%M')}"]
        
    except Exception as e:
        issues.append(f"Error reading file: {str(e)}")
    
    return issues

def main():
    print("=" * 80)
    print("OHLCV Data Validator")
    print("=" * 80)
    print()
    
    if not os.path.exists(DATA_DIR):
        print("No data directory found.")
        return
    
    symbols = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]
    symbols.sort()
    
    if not symbols:
        print("No symbol directories found.")
        return
    
    timeframes = ["1m", "5m", "15m", "1h", "4h", "1d", "1W", "1mo", "3mo"]
    
    total_files = 0
    total_issues = 0
    
    for symbol in symbols:
        print(f"\n{symbol.upper()}")
        print("-" * 40)
        
        symbol_has_issues = False
        
        for timeframe in timeframes:
            csv_path = f"{DATA_DIR}/{symbol}/{timeframe}.csv"
            if os.path.exists(csv_path):
                total_files += 1
                issues = validate_csv(csv_path, timeframe, symbol)
                
                if len(issues) == 1 and issues[0].startswith("✓"):
                    print(f"{timeframe}: {issues[0]}")
                else:
                    symbol_has_issues = True
                    total_issues += 1
                    print(f"{timeframe}: ⚠️  Issues found:")
                    for issue in issues:
                        print(f"  {issue}")
        
        if not symbol_has_issues:
            print("All timeframes valid ✓")
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total files checked: {total_files}")
    print(f"Files with issues: {total_issues}")
    print(f"Status: {'✓ All files valid' if total_issues == 0 else f'⚠️  {total_issues} files need attention'}")

if __name__ == "__main__":
    main()
