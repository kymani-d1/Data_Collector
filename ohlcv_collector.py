#!/usr/bin/env python3
"""
Production-Ready OHLCV Data Collector for Yahoo Finance
Continuously collects and stores OHLCV data for multiple symbols and timeframes
Enhanced with holiday detection and news event tracking
"""

import os
import sys
import json
import logging
import asyncio
import pandas as pd
import yfinance as yf
import holidays
# import aiohttp
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Set
from pathlib import Path
import signal
import traceback
from itertools import combinations
import warnings

# Suppress pandas warnings
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

# Configuration
DATA_DIR = "ohlc_data"
STATUS_FILE = "status.json"
ERROR_LOG = "errors.log"
MAIN_LOG = "collector.log"
# NEWS_FILE = "news_events.csv"

# Try to import custom config
# try:
#     from config import TE_API_KEY as CUSTOM_TE_API_KEY
#     TE_API_KEY = CUSTOM_TE_API_KEY
# except ImportError:
#     # If no config file, disable news fetching
#     TE_API_KEY = None

# TE_API_URL = "https://api.tradingeconomics.com/calendar"

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

# Timeframe mappings for yfinance
TIMEFRAME_MAP = {
    "1m": {"interval": "1m", "days": 7, "seconds": 60},
    "5m": {"interval": "5m", "days": 7, "seconds": 300},
    "15m": {"interval": "15m", "days": 7, "seconds": 900},
    "1h": {"interval": "1h", "days": 7, "seconds": 3600},
    "4h": {"interval": "1h", "days": 7, "seconds": 14400},  # yfinance doesn't support 4h directly
    "1d": {"interval": "1d", "days": 7, "seconds": 86400},
    "1W": {"interval": "1wk", "days": 7, "seconds": 604800},
    "1Mo": {"interval": "1mo", "days": 30, "seconds": 2592000},
    "3M": {"interval": "3mo", "days": 90, "seconds": 7776000}
}

# Setup logging
def setup_logging():
    """Configure logging for errors and main operations"""
    # Error logger
    error_handler = logging.FileHandler(ERROR_LOG)
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(error_formatter)
    
    # Main logger
    main_handler = logging.FileHandler(MAIN_LOG)
    main_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    main_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    main_handler.setFormatter(main_formatter)
    console_handler.setFormatter(main_formatter)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(error_handler)
    logger.addHandler(main_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

class OHLCVCollector:
    def __init__(self):
        self.symbols = self._generate_symbols()
        self.status = self._load_status()
        self.running = True
        # self.news_last_fetch = None
        # self.session = None
        # self._ensure_news_file()
        
        # if not TE_API_KEY or TE_API_KEY == "your_api_key_here":
        #     logger.warning("Trading Economics API key not configured. News fetching will be disabled.")
        #     logger.warning("Get a free key from https://developer.tradingeconomics.com for full news coverage.")
        #     self.news_enabled = False
        # else:
        #     self.news_enabled = True
        
    # def _ensure_news_file(self):
    #     """Ensure the news events file exists with correct headers"""
    #     if not os.path.exists(NEWS_FILE):
    #         pd.DataFrame(columns=['EventTime', 'Country', 'Currency', 'Event', 'Importance', 'DowntimeStart', 'DowntimeEnd']) \
    #           .to_csv(NEWS_FILE, index=False)

    def _get_symbol_currencies(self, symbol_key: str) -> Set[str]:
        """Extract currencies from a symbol key"""
        currencies = set()
        
        # Forex pairs - extract both currencies
        forex_map = {
            'eurusd': ['EUR', 'USD'], 'gbpusd': ['GBP', 'USD'], 
            'usdjpy': ['USD', 'JPY'], 'usdchf': ['USD', 'CHF'],
            'audusd': ['AUD', 'USD'], 'usdcad': ['USD', 'CAD'],
            'nzdusd': ['NZD', 'USD'], 'eurgbp': ['EUR', 'GBP'],
            'eurjpy': ['EUR', 'JPY'], 'gbpjpy': ['GBP', 'JPY'],
            'chfjpy': ['CHF', 'JPY'], 'gbpchf': ['GBP', 'CHF'],
            'euraud': ['EUR', 'AUD'], 'eurcad': ['EUR', 'CAD'],
            'gbpaud': ['GBP', 'AUD'], 'gbpcad': ['GBP', 'CAD'],
            'eurchf': ['EUR', 'CHF'], 'audcad': ['AUD', 'CAD'],
            'nzdcad': ['NZD', 'CAD'], 'audchf': ['AUD', 'CHF'],
            'audjpy': ['AUD', 'JPY'], 'audnzd': ['AUD', 'NZD']
        }
        
        if symbol_key in forex_map:
            currencies.update(forex_map[symbol_key])
        # USD-based assets
        elif symbol_key in ['btcusd', 'ethusd', 'xauusd', 'xagusd', 'xngusd', 'spy', 'us100', 'oil', 'copper', 'dxy', 'vix']:
            currencies.add('USD')
            
        return currencies
    
    def _is_holiday(self, date: datetime, symbol_key: str) -> bool:
        """Check if date is a holiday for the given symbol"""
        currencies = self._get_symbol_currencies(symbol_key)
        
        for currency in currencies:
            if currency in HOLIDAY_CALENDARS:
                if date.date() in HOLIDAY_CALENDARS[currency]:
                    return True
        return False
    
    # async def _fetch_news_events(self):
    #     """Fetch high-impact economic events from TradingEconomics"""
    #     if not self.news_enabled:
    #         return # Do not fetch news if not enabled
            
    #     try:
    #         if self.session is None:
    #             self.session = aiohttp.ClientSession()
                
    #         # Only fetch once per day
    #         now = datetime.now(timezone.utc)
    #         if self.news_last_fetch and (now - self.news_last_fetch).total_seconds() < 86400:
    #             return
            
    #         # Append API key to URL
    #         url = f"{TE_API_URL}?c={TE_API_KEY}"
                
    #         async with self.session.get(url) as response:
    #             if response.status == 200:
    #                 events = await response.json()
                    
    #                 # Process all events
    #                 if events:
    #                     for event in events:
    #                         await self._process_news_event(event)
    #                 else:
    #                     logger.info("No news events found in the latest fetch.")
                            
    #                 self.news_last_fetch = now
    #                 logger.info("Fetched and processed economic calendar events")
    #             else:
    #                 logger.error(f"Failed to fetch news events: HTTP {response.status}")
                    
    #     except Exception as e:
    #         logger.error(f"Error fetching news events: {str(e)}")
    #         logger.error(traceback.format_exc())
    
    # async def _process_news_event(self, event: Dict):
    #     """Process a high-impact news event and save downtime windows"""
    #     try:
    #         # Parse event details
    #         event_time = datetime.fromisoformat(event['Date'].replace('Z', '+00:00'))
    #         country = event.get('Country', '')
    #         event_name = event.get('Event', 'Unknown Event')
    #         importance = event.get('Importance', 0)
            
    #         # Map country to currency
    #         currency_map = {
    #             'United States': 'USD',
    #             'United Kingdom': 'GBP',
    #             'Euro Area': 'EUR',
    #             'Japan': 'JPY',
    #             'Switzerland': 'CHF',
    #             'Australia': 'AUD',
    #             'New Zealand': 'NZD',
    #             'Canada': 'CAD'
    #         }
            
    #         currency = currency_map.get(country)
    #         if not currency:
    #             return
                
    #         # Calculate downtime window (1 hour before to 1 hour after)
    #         start_time = event_time - timedelta(hours=1)
    #         end_time = event_time + timedelta(hours=1)
            
    #         # Save the event to the central news file
    #         new_row = pd.DataFrame([{
    #             'EventTime': event_time.strftime('%Y-%m-%d %H:%M:%S'),
    #             'Country': country,
    #             'Currency': currency,
    #             'Event': event_name,
    #             'Importance': importance,
    #             'DowntimeStart': start_time.strftime('%Y-%m-%d %H:%M:%S'),
    #             'DowntimeEnd': end_time.strftime('%Y-%m-%d %H:%M:%S')
    #         }])

    #         try:
    #             existing_df = pd.read_csv(NEWS_FILE)
    #             # Check if this event (by time and name) already exists
    #             if not ((pd.to_datetime(existing_df['EventTime']) == event_time) & (existing_df['Event'] == event_name)).any():
    #                 combined_df = pd.concat([existing_df, new_row], ignore_index=True)
    #                 combined_df.sort_values('EventTime', inplace=True)
    #                 combined_df.to_csv(NEWS_FILE, index=False)
    #                 logger.info(f"Logged new event (Importance: {importance}): {currency} - {event_name}")
    #         except pd.errors.EmptyDataError:
    #             new_row.to_csv(NEWS_FILE, index=False)
    #             logger.info(f"Logged new event (Importance: {importance}): {currency} - {event_name}")

    #     except Exception as e:
    #         logger.error(f"Error processing news event: {str(e)}")
    #         logger.error(traceback.format_exc())
    
    def _generate_symbols(self) -> Dict[str, str]:
        """Generate all required symbols with Yahoo Finance format"""
        symbols = {}
        
        # Forex pairs - Yahoo Finance typically uses these specific formats
        forex_pairs = {
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
            "chfjpy": "CHFJPY=X",
            "gbpchf": "GBPCHF=X",
            "euraud": "EURAUD=X",
            "eurcad": "EURCAD=X",
            "gbpaud": "GBPAUD=X",
            "gbpcad": "GBPCAD=X",
            "eurchf": "EURCHF=X",
            "audcad": "AUDCAD=X",
            "nzdcad": "NZDCAD=X",
            "audchf": "AUDCHF=X",
            "audjpy": "AUDJPY=X",
            "audnzd": "AUDNZD=X",
        }
        
        # Additional assets with correct Yahoo Finance symbols
        additional = {
            "xauusd": "GC=F",      # Gold futures
            "xagusd": "SI=F",      # Silver futures
            "us100": "NQ=F",       # NASDAQ 100 futures
            "spy": "SPY",          # S&P 500 ETF
            "xngusd": "NG=F",      # Natural Gas futures
            "oil": "CL=F",         # Crude Oil futures
            "copper": "HG=F",      # Copper futures
            "btcusd": "BTC-USD",   # Bitcoin
            "ethusd": "ETH-USD",   # Ethereum
            "dxy": "DX-Y.NYB",     # US Dollar Index
            "vix": "^VIX"          # Volatility Index
        }
        
        # Combine all symbols
        symbols.update(forex_pairs)
        symbols.update(additional)
        
        return symbols
    
    def _sanitize_filename(self, symbol: str) -> str:
        """Sanitize symbol for filesystem"""
        return symbol.replace("=", "").replace("-", "").replace("^", "").lower()
    
    def _load_status(self) -> Dict:
        """Load or create status tracking file"""
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_status(self):
        """Save status to file"""
        with open(STATUS_FILE, 'w') as f:
            json.dump(self.status, f, indent=2)
    
    def _ensure_directories(self):
        """Create necessary directories"""
        Path(DATA_DIR).mkdir(exist_ok=True)
        for symbol_key in self.symbols.keys():
            Path(f"{DATA_DIR}/{symbol_key}").mkdir(exist_ok=True)
    
    def _get_csv_path(self, symbol_key: str, timeframe: str) -> str:
        """Get CSV file path for symbol and timeframe"""
        return f"{DATA_DIR}/{symbol_key}/{timeframe}.csv"
    
    def _load_existing_data(self, csv_path: str) -> Optional[pd.DataFrame]:
        """Load existing CSV data if available"""
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                df['Datetime'] = pd.to_datetime(df['Datetime'])
                return df
            except Exception as e:
                logger.error(f"Error loading {csv_path}: {e}")
        return None
    
    def _save_data(self, df: pd.DataFrame, csv_path: str):
        """Save dataframe to CSV"""
        df.to_csv(csv_path, index=False)
    
    def _validate_ohlc(self, df: pd.DataFrame, symbol_key: str, timeframe: str) -> pd.DataFrame:
        """Validate OHLC data, drop and log invalid rows"""
        if df.empty:
            return df

        # Ensure numeric types
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Drop rows with NaN in essential columns
        df.dropna(subset=['Datetime', 'Open', 'High', 'Low', 'Close'], inplace=True)

        # Validate OHLC consistency
        invalid_mask = (df['High'] < df[['Open', 'Low', 'Close']].max(axis=1)) | \
                       (df['Low'] > df[['Open', 'High', 'Close']].min(axis=1))
        
        invalid_rows = df[invalid_mask]
        if not invalid_rows.empty:
            logger.warning(f"Found {len(invalid_rows)} invalid OHLC bars for {symbol_key} {timeframe}. Invalid data:\n{invalid_rows}")
            df = df[~invalid_mask]
            
        return df

    def _process_4h_data(self, df_1h: pd.DataFrame) -> pd.DataFrame:
        """Convert 1h data to 4h bars"""
        if df_1h.empty:
            return pd.DataFrame()
            
        df = df_1h.copy()
        
        # Ensure Datetime is in the index for resampling
        if 'Datetime' in df.columns:
            df.set_index('Datetime', inplace=True)
        
        # Resample to 4H
        df_4h = df.resample('4H', label='left', closed='left').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()
        
        df_4h.reset_index(inplace=True)
        return df_4h
    
    async def fetch_historical_data(self, symbol: str, symbol_key: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Fetch historical data for a symbol and timeframe"""
        try:
            config = TIMEFRAME_MAP[timeframe]
            ticker = yf.Ticker(symbol)
            
            # For 4h, fetch 1h data and resample
            if timeframe == "4h":
                end_date = datetime.now(timezone.utc)
                start_date = end_date - timedelta(days=config["days"] * 2)  # Get extra data for 4h
                
                df = ticker.history(
                    start=start_date,
                    end=end_date,
                    interval="1h"
                )
                
                if df.empty:
                    logger.warning(f"No 1h data for {symbol} to create 4h bars")
                    return None
                
                # Reset index to have Datetime as a column
                df.reset_index(inplace=True)
                df.rename(columns={'Date': 'Datetime', 'index': 'Datetime'}, inplace=True)
                
                # Convert to GMT
                if df['Datetime'].dt.tz is None:
                    df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize('UTC')
                else:
                    df['Datetime'] = df['Datetime'].dt.tz_convert('GMT')
                
                # Select required columns before resampling
                df = df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
                
                df = self._process_4h_data(df)
            else:
                df = ticker.history(
                    period=f"{config['days']}d",
                    interval=config["interval"]
                )
            
            if df.empty:
                logger.warning(f"No data returned for {symbol} {timeframe}")
                return None
            
            # Convert to GMT and format
            if 'Datetime' not in df.columns:
                df.reset_index(inplace=True)
                df.rename(columns={'Date': 'Datetime', 'index': 'Datetime'}, inplace=True)
            
            # Ensure timezone conversion
            if df['Datetime'].dt.tz is None:
                df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize('UTC')
            else:
                df['Datetime'] = df['Datetime'].dt.tz_convert('GMT')
                
            df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Select only required columns
            df = df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
            
            # Validate OHLC data
            df = self._validate_ohlc(df, symbol_key, timeframe)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching {symbol} {timeframe}: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    async def initial_load(self):
        """Perform initial data load for all symbols and timeframes"""
        logger.info("Starting initial data load...")
        logger.info("Attempting to load data for %d symbols", len(self.symbols))
        
        tasks = []
        for symbol_key, symbol in self.symbols.items():
            logger.info("Queuing %s -> %s", symbol_key, symbol)
            for timeframe in TIMEFRAME_MAP.keys():
                tasks.append(self._load_symbol_timeframe(symbol, symbol_key, timeframe))
        
        # Process in batches to avoid overwhelming the API
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            await asyncio.sleep(1)  # Small delay between batches
            
        logger.info("Initial data load completed")
    
    async def _load_symbol_timeframe(self, symbol: str, symbol_key: str, timeframe: str):
        """Load data for a specific symbol and timeframe"""
        csv_path = self._get_csv_path(symbol_key, timeframe)
        existing_df = self._load_existing_data(csv_path)
        
        df = await self.fetch_historical_data(symbol, symbol_key, timeframe)
        if df is None:
            return
        
        if existing_df is not None:
            # Merge with existing data, avoiding duplicates
            combined_df = pd.concat([existing_df, df])
            combined_df['Datetime'] = pd.to_datetime(combined_df['Datetime'])
            combined_df = combined_df.drop_duplicates(subset=['Datetime'], keep='last')
            combined_df = combined_df.sort_values('Datetime')
            combined_df['Datetime'] = combined_df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df = combined_df
        
        self._save_data(df, csv_path)
        
        # Update status
        if symbol_key not in self.status:
            self.status[symbol_key] = {}
        self.status[symbol_key][timeframe] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Loaded {len(df)} bars for {symbol_key} {timeframe}")
    
    async def fetch_latest_bar(self, symbol: str, symbol_key: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Fetch the latest closed bar for a symbol and timeframe"""
        try:
            config = TIMEFRAME_MAP[timeframe]
            ticker = yf.Ticker(symbol)
            
            # Calculate how many bars to fetch to ensure we get the latest closed one
            if timeframe in ["1m", "5m", "15m"]:
                # For intraday, fetch last 2 hours
                period = "1d"
            else:
                # For longer timeframes, fetch appropriate period
                period = f"{min(config['days'] * 2, 60)}d"
            
            if timeframe == "4h":
                df = ticker.history(period=period, interval="1h")
                if not df.empty:
                    # Reset index to have Datetime as a column
                    df.reset_index(inplace=True)
                    df.rename(columns={'Date': 'Datetime', 'index': 'Datetime'}, inplace=True)
                    
                    # Convert to GMT
                    if df['Datetime'].dt.tz is None:
                        df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize('UTC')
                    else:
                        df['Datetime'] = df['Datetime'].dt.tz_convert('GMT')
                    
                    # Select required columns
                    df = df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
                    df = self._process_4h_data(df)
            else:
                df = ticker.history(period=period, interval=config["interval"])
            
            if df.empty:
                return None
            
            # Ensure we have Datetime column
            if 'Datetime' not in df.columns:
                df.reset_index(inplace=True)
                df.rename(columns={'Date': 'Datetime', 'index': 'Datetime'}, inplace=True)
            
            # Convert to GMT
            if isinstance(df['Datetime'].iloc[0], str):
                df['Datetime'] = pd.to_datetime(df['Datetime'])
            
            if df['Datetime'].dt.tz is None:
                df['Datetime'] = df['Datetime'].dt.tz_localize('UTC')
            else:
                df['Datetime'] = df['Datetime'].dt.tz_convert('GMT')
            
            # Get the last complete bar (not the current incomplete one)
            now = datetime.now(timezone.utc)
            df = df[df['Datetime'] < now]
            
            if df.empty:
                return None
            
            # Take only the last row
            df = df.tail(1).copy()
            df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df = df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
            
            # Validate OHLC data
            df = self._validate_ohlc(df, symbol_key, timeframe)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching latest bar for {symbol} {timeframe}: {str(e)}")
            return None
    
    async def update_symbol_timeframe(self, symbol: str, symbol_key: str, timeframe: str):
        """Update data for a specific symbol and timeframe"""
        csv_path = self._get_csv_path(symbol_key, timeframe)
        existing_df = self._load_existing_data(csv_path)
        
        latest_bar = await self.fetch_latest_bar(symbol, symbol_key, timeframe)
        if latest_bar is None or latest_bar.empty:
            return
        
        if existing_df is not None:
            # Append new bar and remove any duplicates
            combined_df = pd.concat([existing_df, latest_bar], ignore_index=True)
            combined_df['Datetime'] = pd.to_datetime(combined_df['Datetime'])
            combined_df = combined_df.drop_duplicates(subset=['Datetime'], keep='last')
            combined_df = combined_df.sort_values('Datetime')
            combined_df['Datetime'] = combined_df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            self._save_data(combined_df, csv_path)
        else:
            self._save_data(latest_bar, csv_path)
        
        # Update status
        if symbol_key not in self.status:
            self.status[symbol_key] = {}
        self.status[symbol_key][timeframe] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Updated {symbol_key} {timeframe} - {latest_bar['Datetime'].iloc[0]}")
    
    async def continuous_update_loop(self):
        """Main loop to continuously update data"""
        
        # Fetch news events periodically
        # await self._fetch_news_events()

        while self.running:
            now = datetime.now(timezone.utc)
            update_tasks = []
            for timeframe, config in TIMEFRAME_MAP.items():
                # Calculate next update time based on timeframe
                seconds = config["seconds"]
                
                if timeframe == "1m":
                    next_update = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                elif timeframe == "5m":
                    minutes = (now.minute // 5 + 1) * 5
                    next_update = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=minutes)
                elif timeframe == "15m":
                    minutes = (now.minute // 15 + 1) * 15
                    next_update = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=minutes)
                elif timeframe == "1h":
                    next_update = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                elif timeframe == "4h":
                    hours = (now.hour // 4 + 1) * 4
                    next_update = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=hours)
                elif timeframe == "1d":
                    next_update = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                elif timeframe == "1W":
                    days_until_monday = (7 - now.weekday()) % 7
                    if days_until_monday == 0:
                        days_until_monday = 7
                    next_update = (now + timedelta(days=days_until_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
                elif timeframe == "1Mo":
                    if now.month == 12:
                        next_update = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                    else:
                        next_update = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
                else:  # 3M
                    month = ((now.month - 1) // 3 + 1) * 3 + 1
                    year = now.year
                    if month > 12:
                        month = 1
                        year += 1
                    next_update = now.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)
                
                if now >= next_update:
                    # Update all symbols for this timeframe
                    for symbol_key, symbol in self.symbols.items():
                        update_tasks.append(self.update_symbol_timeframe(symbol, symbol_key, timeframe))
                    
                    # Calculate next update time
                    config = TIMEFRAME_MAP[timeframe]
                    next_update = next_update + timedelta(seconds=config["seconds"])
            
            if update_tasks:
                await asyncio.gather(*update_tasks)
                self._save_status()
            
            # Sleep for a short time before checking again
            sleep_duration = 10
            await asyncio.sleep(sleep_duration)
    
    async def run(self):
        """Run the data collector"""
        self.running = True
        logger.info("OHLCV Data Collector starting...")
        
        # Perform initial data load
        await self.initial_load()
        
        # Start continuous update loop
        await self.continuous_update_loop()
        
    def stop(self):
        self.running = False
        # if self.session:
        #     await self.session.close()
        logger.info("OHLCV Data Collector stopped")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Received shutdown signal")
    sys.exit(0)

async def main():
    """Main function"""
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run collector
    collector = OHLCVCollector()
    await collector.run()

if __name__ == "__main__":
    asyncio.run(main())
