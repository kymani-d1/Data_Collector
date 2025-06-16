#!/usr/bin/env python3
"""
Data Validator for OHLCV CSV files
Checks for data integrity issues like gaps, duplicates, and invalid values
Enhanced to handle public holidays
"""

import os
import pandas as pd
import holidays
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = "ohlc_data"

# Holiday calendars
HOLIDAY_CALENDARS = {
    'USD': holidays.US(),
    'GBP': holidays.UK(),
    'EUR': holidays.ECB(),
    'JPY': holidays.Japan(),
    'CHF': holidays.Switzerland(),
    'AUD': holidays.Australia(),
    'CAD': holidays.Canada(),
    'NZD': holidays.NewZealand()
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
    "1Mo": 43200,  # Approximate
    "3M": 129600  # Approximate
}

def get_symbol_currencies(symbol: str):
    """Extract currencies from a symbol"""
    currencies = set()
    
    # Forex pairs
    if len(symbol) == 6 and symbol.isalpha():
        currencies.add(symbol[:3].upper())
        currencies.add(symbol[3:].upper())
    # USD-based assets
    elif symbol in ['btcusdt', 'ethusdt', 'xauusd', 'xagusd', 'xngusd', 'spy', 'us100', 'oil', 'copper', 'dxy', 'vix']:
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
        
        # Convert datetime
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        
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
        
        # Check for gaps (only for intraday timeframes)
        if timeframe in ["1m", "5m", "15m", "1h", "4h"]:
            df = df.sort_values('Datetime')
            expected_delta = timedelta(minutes=INTERVAL_MINUTES[timeframe])
            
            gaps = []
            for i in range(1, len(df)):
                actual_delta = df['Datetime'].iloc[i] - df['Datetime'].iloc[i-1]
                
                # Allow for weekend gaps
                if actual_delta > expected_delta and actual_delta < timedelta(days=3):
                    # Check if it's not a weekend gap
                    start_day = df['Datetime'].iloc[i-1].weekday()
                    end_day = df['Datetime'].iloc[i].weekday()
                    
                    # Check if gap includes holidays
                    gap_start = df['Datetime'].iloc[i-1]
                    gap_end = df['Datetime'].iloc[i]
                    
                    # Check each day in the gap for holidays
                    is_holiday_gap = False
                    current_date = gap_start.date() + timedelta(days=1)
                    while current_date < gap_end.date():
                        if is_holiday(pd.Timestamp(current_date), symbol):
                            is_holiday_gap = True
                            break
                        current_date += timedelta(days=1)
                    
                    if not (start_day >= 4 and end_day == 0) and not is_holiday_gap:  # Not Friday to Monday and not holiday
                        gaps.append({
                            'from': df['Datetime'].iloc[i-1],
                            'to': df['Datetime'].iloc[i],
                            'gap': actual_delta
                        })
            
            if gaps and len(gaps) <= 5:
                issues.append(f"Found {len(gaps)} time gaps:")
                for gap in gaps[:5]:
                    issues.append(f"  - {gap['from']} to {gap['to']} ({gap['gap']})")
            elif gaps:
                issues.append(f"Found {len(gaps)} time gaps (showing first 5)")
        
        # Check data freshness
        last_date = df['Datetime'].max()
        now = datetime.now()
        age = now - last_date
        
        if timeframe == "1m" and age > timedelta(hours=1):
            issues.append(f"Data is {age} old (last: {last_date})")
        elif timeframe in ["5m", "15m"] and age > timedelta(hours=2):
            issues.append(f"Data is {age} old (last: {last_date})")
        elif timeframe == "1h" and age > timedelta(hours=6):
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
    
    timeframes = ["1m", "5m", "15m", "1h", "4h", "1d", "1W", "1Mo", "3M"]
    
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
