#!/usr/bin/env python3
"""
Test script to verify holiday detection is working correctly
"""

import holidays
from datetime import datetime, timedelta

# Initialize holiday calendars
HOLIDAY_CALENDARS = {
    'USD': holidays.US(),
    'GBP': holidays.UK(),
    'EUR': holidays.ECB(),
    'JPY': holidays.Japan(),
    'CHF': holidays.Switzerland(),
    'AUD': holidays.Australia()
}

def test_holidays():
    """Test holiday detection for the next 30 days"""
    print("Holiday Detection Test")
    print("=" * 60)
    
    today = datetime.now().date()
    
    for currency, calendar in HOLIDAY_CALENDARS.items():
        print(f"\n{currency} Holidays (next 30 days):")
        print("-" * 40)
        
        found_holidays = []
        for i in range(30):
            check_date = today + timedelta(days=i)
            if check_date in calendar:
                holiday_name = calendar.get(check_date)
                found_holidays.append(f"{check_date}: {holiday_name}")
        
        if found_holidays:
            for holiday in found_holidays:
                print(f"  {holiday}")
        else:
            print("  No holidays in the next 30 days")
    
    # Test specific symbols
    print("\n\nSymbol Holiday Check Examples:")
    print("=" * 60)
    
    test_symbols = ['eurusd', 'gbpusd', 'spy', 'btcusdt']
    test_date = datetime(2025, 12, 25).date()  # Christmas
    
    for symbol in test_symbols:
        currencies = get_symbol_currencies(symbol)
        is_holiday = False
        holiday_names = []
        
        for currency in currencies:
            if currency in HOLIDAY_CALENDARS:
                if test_date in HOLIDAY_CALENDARS[currency]:
                    is_holiday = True
                    holiday_names.append(f"{currency}: {HOLIDAY_CALENDARS[currency].get(test_date)}")
        
        print(f"\n{symbol.upper()} on {test_date}:")
        print(f"  Currencies: {', '.join(currencies)}")
        print(f"  Is Holiday: {is_holiday}")
        if holiday_names:
            for name in holiday_names:
                print(f"  {name}")

def get_symbol_currencies(symbol):
    """Extract currencies from a symbol"""
    currencies = set()
    
    # Forex pairs
    if len(symbol) == 6 and symbol.isalpha():
        currencies.add(symbol[:3].upper())
        currencies.add(symbol[3:].upper())
    # USD-based assets
    elif symbol in ['btcusdt', 'ethusdt', 'xauusd', 'xagusd', 'xngusd', 'spy', 'us100', 'oil', 'copper']:
        currencies.add('USD')
        
    return currencies

if __name__ == "__main__":
    test_holidays()
