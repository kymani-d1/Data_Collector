#!/usr/bin/env python3
"""
Shared utilities for OHLCV data collection system
"""

import pandas as pd
from typing import Set
from datetime import datetime, timezone

def get_symbol_currencies(symbol: str) -> Set[str]:
    """Extract currencies from a symbol"""
    currencies = set()
    
    # Forex pairs - extract both currencies
    forex_map = {
        'eurusd': ['EUR', 'USD'], 'gbpusd': ['GBP', 'USD'], 
        'usdjpy': ['USD', 'JPY'], 'usdchf': ['USD', 'CHF'],
        'audusd': ['AUD', 'USD'], 'eurgbp': ['EUR', 'GBP'],
        'eurjpy': ['EUR', 'JPY'], 'gbpjpy': ['GBP', 'JPY'],
        'eurchf': ['EUR', 'CHF']
    }
    
    if symbol in forex_map:
        currencies.update(forex_map[symbol])
    # USD-based assets
    elif symbol in ['gold', 'silver', 'natgas', 'spy', 'nasdaq', 'sp500', 'oil', 'copper', 'btc', 'eth']:
        currencies.add('USD')
        
    return currencies

def parse_datetime_utc(dt_str: str) -> datetime:
    """Parse datetime string to UTC-aware datetime"""
    dt = pd.to_datetime(dt_str)
    if dt.tz is None:
        return dt.tz_localize('UTC')
    return dt.tz_convert('UTC')

def is_crypto_symbol(symbol_key: str) -> bool:
    """Check if a symbol is cryptocurrency"""
    return symbol_key.lower() in ['btc', 'eth']

def get_yahoo_symbol(symbol_key: str) -> str:
    """Map internal symbol key to Yahoo Finance symbol"""
    symbol_map = {
        # Forex pairs
        'eurusd': 'EURUSD=X', 'gbpusd': 'GBPUSD=X',
        'usdjpy': 'USDJPY=X', 'usdchf': 'USDCHF=X',
        'audusd': 'AUDUSD=X', 'eurgbp': 'EURGBP=X',
        'eurjpy': 'EURJPY=X', 'gbpjpy': 'GBPJPY=X',
        'eurchf': 'EURCHF=X',
        # Metals & energies
        'gold': 'GC=F', 'silver': 'SI=F', 'oil': 'CL=F',
        'natgas': 'NG=F', 'copper': 'HG=F',
        # Index futures & ETF
        'sp500': 'ES=F', 'nasdaq': 'NQ=F', 'spy': 'SPY',
        # Crypto
        'btc': 'BTC-USD', 'eth': 'ETH-USD'
    }
    return symbol_map.get(symbol_key.lower(), symbol_key)
