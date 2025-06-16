#!/usr/bin/env python3
"""
Quick script to check which Yahoo Finance symbols are valid
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def check_symbol(symbol_key, symbol):
    """Check if a symbol returns data from Yahoo Finance"""
    try:
        ticker = yf.Ticker(symbol)
        # Try to get recent data
        hist = ticker.history(period="5d")
        
        if not hist.empty:
            last_price = hist['Close'].iloc[-1]
            last_date = hist.index[-1]
            return True, f"✓ Valid - Last: ${last_price:.2f} on {last_date.strftime('%Y-%m-%d')}"
        else:
            return False, "✗ No data returned"
    except Exception as e:
        return False, f"✗ Error: {str(e)[:50]}"

def main():
    print("Yahoo Finance Symbol Checker")
    print("=" * 60)
    
    # Test symbols
    test_symbols = {
        # Forex
        "eurusd": "EURUSD=X",
        "gbpusd": "GBPUSD=X",
        "usdjpy": "USDJPY=X",
        "usdchf": "USDCHF=X",
        "audusd": "AUDUSD=X",
        "usdcad": "USDCAD=X",
        "nzdusd": "NZDUSD=X",
        "eurgbp": "EURGBP=X",
        "eurjpy": "EURJPY=X",
        "gbpjpy": "GBPJPY=X",
        "eurchf": "EURCHF=X",
        "audjpy": "AUDJPY=X",
        
        # Commodities
        "gold": "GC=F",
        "silver": "SI=F",
        "oil": "CL=F",
        "natgas": "NG=F",
        "copper": "HG=F",
        
        # Indices
        "spy": "SPY",
        "nasdaq": "NQ=F",
        "sp500": "ES=F",
        "dxy": "DX-Y.NYB",
        "vix": "^VIX",
        
        # Crypto
        "btc": "BTC-USD",
        "eth": "ETH-USD",
    }
    
    # Check each symbol
    valid_count = 0
    for key, symbol in test_symbols.items():
        is_valid, message = check_symbol(key, symbol)
        if is_valid:
            valid_count += 1
        print(f"{key:<12} {symbol:<12} {message}")
    
    print("\n" + "=" * 60)
    print(f"Valid symbols: {valid_count}/{len(test_symbols)}")
    
    # Test alternative formats
    print("\nTesting alternative formats:")
    print("-" * 60)
    
    alternatives = [
        ("gold_alt1", "GOLD"),
        ("gold_alt2", "XAUUSD=X"),
        ("gold_alt3", "GLD"),
        ("oil_alt1", "USOIL"),
        ("oil_alt2", "USO"),
        ("nasdaq_alt1", "QQQ"),
        ("nasdaq_alt2", "^NDX"),
    ]
    
    for key, symbol in alternatives:
        is_valid, message = check_symbol(key, symbol)
        print(f"{key:<12} {symbol:<12} {message}")

if __name__ == "__main__":
    main()
