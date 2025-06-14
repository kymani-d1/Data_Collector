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
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Set
from pathlib import Path
import signal
import traceback
from itertools import combinations
import warnings
from sessions import closed_window   # for detecting scheduled closures in back-fill

# Suppress pandas warnings
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

# Configuration
DATA_DIR = "ohlc_data"
STATUS_FILE = "status.json"
ERROR_LOG = "errors.log"
MAIN_LOG = "collector.log"

# Try to import custom config
try:
    from config import TE_API_KEY as CUSTOM_TE_API_KEY
    TE_API_KEY = CUSTOM_TE_API_KEY
except ImportError:
    # Use default if no custom config
    TE_API_KEY = "68951cae12474dd:0zn4h4vjkr9cs1h"

TE_API_URL = f"https://api.tradingeconomics.com/calendar?c={TE_API_KEY}"

# Holiday calendars
HOLIDAY_CALENDARS = {
    'USD': holidays.US(),
    'GBP': holidays.UK(),
    'EUR': holidays.ECB(),
    'JPY': holidays.Japan(),
    'CHF': holidays.Switzerland(),
    'AUD': holidays.Australia()
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
    "1mo": {"interval": "1mo", "days": 30, "seconds": 2592000},
    "3mo": {"interval": "3mo", "days": 90, "seconds": 7776000}
}

# Expected intervals in minutes for gap detection
EXPECTED_DELTA = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4, minutes=0),  # Ensure exact 4-hour intervals
    "1d": timedelta(days=1),
    "1W": timedelta(weeks=1),
    "1mo": timedelta(days=30),
    "3mo": timedelta(days=90)
}

# Yahoo Finance interval mapping
YF_INTERVAL = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "1h",  # Will be resampled
    "1d": "1d",
    "1W": "1wk",
    "1mo": "1mo",
    "3mo": "3mo"
}

# Maximum bars Yahoo Finance provides per request for each timeframe
MAX_BARS = {
    "1m": 8000,    # ~5 days of 1-min data
    "5m": 8000,    # ~28 days of 5-min data
    "15m": 8000,   # ~83 days of 15-min data
    "1h": 8000,    # ~333 days of hourly data
    "4h": 8000,    # ~1333 days of 4h data
    "1d": 8000,    # ~22 years of daily data
    "1W": 8000,    # ~154 years of weekly data
    "1mo": 8000,   # ~667 years of monthly data
    "3mo": 8000    # ~2000 years of quarterly data
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
        self.news_last_fetch = None
        self.session = None
        # limit concurrency so 1m cycle always <55s
        self.semaphore = asyncio.Semaphore(5)
        
    def _get_symbol_currencies(self, symbol_key: str) -> Set[str]:
        """Extract currencies from a symbol key"""
        currencies = set()
        
        # Forex pairs - extract both currencies
        forex_map = {
            'eurusd': ['EUR', 'USD'], 'gbpusd': ['GBP', 'USD'], 
            'usdjpy': ['USD', 'JPY'], 'usdchf': ['USD', 'CHF'],
            'audusd': ['AUD', 'USD'], 'eurgbp': ['EUR', 'GBP'],
            'eurjpy': ['EUR', 'JPY'], 'gbpjpy': ['GBP', 'JPY'],
            'eurchf': ['EUR', 'CHF']
        }
        
        if symbol_key in forex_map:
            currencies.update(forex_map[symbol_key])
        # USD-based assets
        elif symbol_key in ['gold', 'silver', 'natgas', 'spy', 'nasdaq', 'sp500', 'oil', 'copper', 'btc', 'eth']:
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
    
    async def _fetch_news_events(self):
        """Fetch high-impact economic events from TradingEconomics"""
        try:
            if self.session is None:
                self.session = aiohttp.ClientSession()
                
            # Only fetch once per day
            now = datetime.now(timezone.utc)
            if self.news_last_fetch and (now - self.news_last_fetch).total_seconds() < 86400:
                return
                
            async with self.session.get(TE_API_URL) as response:
                if response.status == 200:
                    events = await response.json()
                    
                    # Process high-impact events
                    for event in events:
                        if event.get('Importance', 0) >= 3:  # High importance
                            await self._process_news_event(event)
                            
                    self.news_last_fetch = now
                    logger.info("Fetched and processed economic calendar events")
                else:
                    logger.error(f"Failed to fetch news events: HTTP {response.status}")
                    
        except Exception as e:
            logger.error(f"Error fetching news events: {str(e)}")
            logger.error(traceback.format_exc())
    
    async def _process_news_event(self, event: Dict):
        """Process a high-impact news event and save downtime windows"""
        try:
            # Parse event details
            event_time = datetime.fromisoformat(event['Date'].replace('Z', '+00:00'))
            country = event.get('Country', '')
            event_name = event.get('Event', 'Unknown Event')
            
            # Map country to currency
            currency_map = {
                'United States': 'USD',
                'United Kingdom': 'GBP',
                'Euro Area': 'EUR',
                'Japan': 'JPY',
                'Switzerland': 'CHF',
                'Australia': 'AUD'
            }
            
            currency = currency_map.get(country)
            if not currency:
                return
                
            # Calculate downtime window (1 hour before to 1 hour after)
            start_time = event_time - timedelta(hours=1)
            end_time = event_time + timedelta(hours=1)
            
            # Find affected symbols
            affected_symbols = []
            for symbol_key in self.symbols.keys():
                if currency in self._get_symbol_currencies(symbol_key):
                    affected_symbols.append(symbol_key)
            
            # Save downtime for each affected symbol
            for symbol_key in affected_symbols:
                downtime_file = f"{DATA_DIR}/{symbol_key}/trading_downtime.csv"
                
                # Create or append to downtime file
                new_row = pd.DataFrame([{
                    'Start': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'End': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'Reason': f"High-Impact {currency} News: {event_name}"
                }])
                
                if os.path.exists(downtime_file):
                    existing_df = pd.read_csv(downtime_file)
                    # Check if this event already exists
                    if not ((existing_df['Start'] == new_row['Start'].iloc[0]) & 
                           (existing_df['End'] == new_row['End'].iloc[0])).any():
                        combined_df = pd.concat([existing_df, new_row], ignore_index=True)
                        combined_df.to_csv(downtime_file, index=False)
                else:
                    new_row.to_csv(downtime_file, index=False)
                    
        except Exception as e:
            logger.error(f"Error processing news event: {str(e)}")
        
    def _generate_symbols(self) -> Dict[str, str]:
        """Generate all required symbols with Yahoo Finance format"""
        symbols = {}
        
        # Define all symbols with their Yahoo Finance mappings
        symbol_list = [
            {"label": "eurusd", "yahoo": "EURUSD=X"},
            {"label": "gbpusd", "yahoo": "GBPUSD=X"},
            {"label": "usdjpy", "yahoo": "USDJPY=X"},
            {"label": "usdchf", "yahoo": "USDCHF=X"},
            {"label": "audusd", "yahoo": "AUDUSD=X"},
            {"label": "eurgbp", "yahoo": "EURGBP=X"},
            {"label": "eurjpy", "yahoo": "EURJPY=X"},
            {"label": "gbpjpy", "yahoo": "GBPJPY=X"},
            {"label": "eurchf", "yahoo": "EURCHF=X"},
            {"label": "gold", "yahoo": "GC=F"},
            {"label": "silver", "yahoo": "SI=F"},
            {"label": "oil", "yahoo": "CL=F"},
            {"label": "natgas", "yahoo": "NG=F"},
            {"label": "copper", "yahoo": "HG=F"},
            {"label": "spy", "yahoo": "SPY"},
            {"label": "nasdaq", "yahoo": "NQ=F"},
            {"label": "sp500", "yahoo": "ES=F"},
            {"label": "btc", "yahoo": "BTC-USD"},
            {"label": "eth", "yahoo": "ETH-USD"}
        ]
        
        # Create the symbols dictionary
        for symbol in symbol_list:
            symbols[symbol["label"]] = symbol["yahoo"]
        
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
            f.flush()
            os.fsync(f.fileno())
    
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
                # Parse as UTC so every stamp is tz-aware
                # (handles both "2025-06-13 20:58:00" and "2025-06-13 20:58:00+00:00")
                df['Datetime'] = pd.to_datetime(df['Datetime'], utc=True)
                return df
            except Exception as e:
                logger.error(f"Error loading {csv_path}: {e}")
        return None
    
    def _save_data(self, df: pd.DataFrame, csv_path: str):
        """Save dataframe to CSV"""
        # Ensure Datetime is datetime type for proper comparison
        # Parse as UTC so every stamp is tz-aware
        # (handles both "2025-06-13 20:58:00" and "2025-06-13 20:58:00+00:00")
        df['Datetime'] = pd.to_datetime(df['Datetime'], utc=True)
        
        # Drop duplicates based on Datetime, keeping the last occurrence
        df = df.drop_duplicates(subset=['Datetime'], keep='last')
        
        # Final sanity check for invalid OHLC values
        bad = df[(df['High'] < df['Low']) | (df['High'] == 0) | (df['Low'] == 0)]
        if not bad.empty:
            logger.warning("Dropped %s bad rows for %s", len(bad), csv_path)
            df = df.drop(bad.index)
        
        # Sort by Datetime
        df = df.sort_values('Datetime')
        
        # Convert Datetime back to string format
        df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        df.to_csv(csv_path, index=False)
    
    def _process_4h_data(self, df_1h: pd.DataFrame) -> pd.DataFrame:
        """Convert 1h data to 4h bars"""
        if df_1h.empty:
            return pd.DataFrame()
            
        df = df_1h.copy()
        
        # Ensure Datetime is in the index for resampling
        if 'Datetime' in df.columns:
            df.set_index('Datetime', inplace=True)
        
        # Resample to 4H with right-aligned labels and closed intervals
        df_4h = df.resample('4H', label='right', closed='right').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        })
        # drop bars with missing OHLC but keep if only Volume was NaN
        df_4h = df_4h.dropna(subset=['Open','High','Low','Close'], how='any')
        
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
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching {symbol} {timeframe}: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    async def initial_load(self):
        """Perform initial data load for all symbols and timeframes"""
        logger.info("Starting initial data load...")
        logger.info(f"Attempting to load data for {len(self.symbols)} symbols")
        
        tasks = []
        for symbol_key, symbol in self.symbols.items():
            logger.info(f"Queuing {symbol_key} -> {symbol}")
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
            # Check for holidays before making the API call
            now = datetime.now(timezone.utc)
            if self._is_holiday(now, symbol_key):
                logger.debug(f"Skipping {symbol_key} {timeframe} update due to holiday")
                return None

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
            
            # Validate OHLC values
            if (df['High'] < df['Low']).any() or (df['High'] < df['Open']).any() or (df['High'] < df['Close']).any() or \
               (df['Low'] > df['Open']).any() or (df['Low'] > df['Close']).any():
                logger.warning(f"Invalid OHLC values detected for {symbol_key} {timeframe}, discarding bar")
                return None
            
            df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df = df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching latest bar for {symbol} {timeframe}: {str(e)}")
            return None
    
    async def update_symbol_timeframe(self, symbol: str, symbol_key: str, timeframe: str):
        """Update data for a specific symbol and timeframe"""
        csv_path = self._get_csv_path(symbol_key, timeframe)
        existing_df = self._load_existing_data(csv_path)
        
        if existing_df is not None:
            # Convert Datetime to datetime for comparison
            existing_df['Datetime'] = pd.to_datetime(existing_df['Datetime'])
            last_dt = existing_df['Datetime'].iloc[-1]
            now = datetime.now(timezone.utc)
            
            # Calculate how many bars should exist since the last one we saved
            expected_bars = int((now - last_dt) / EXPECTED_DELTA[timeframe])
            
            if expected_bars > 1:  # We missed some bars
                logger.info(f"Detected {expected_bars} missing bars for {symbol_key} {timeframe}, back-filling...")
                
                # Calculate start time for back-fill
                start = last_dt + EXPECTED_DELTA[timeframe]
                
                try:
                    # Fetch missing data in chunks
                    all_new_data = []
                    while expected_bars > 0:
                        # Calculate chunk size and end time
                        chunk_size = min(expected_bars, MAX_BARS[timeframe])
                        end = start + chunk_size * EXPECTED_DELTA[timeframe]
                        
                        # --- skip back-fill over scheduled closures (weekend/holidays) ---
                        try:
                            if timeframe in ["1m","5m","15m","1h","4h"] and closed_window(symbol_key, start, end, timeframe):
                                logger.info(f"Skipping back-fill for {symbol_key} {timeframe} {start}->{end} (market closed)")
                                expected_bars -= chunk_size
                                start = end
                                continue
                        except Exception:
                            # any issue detecting closure, we'll let the fetch try
                            pass

                        logger.debug(f"Fetching chunk of {chunk_size} bars from {start} to {end}")
                        
                        # Fetch data for this chunk
                        if timeframe == "4h":
                            # For 4h, fetch 1h data and resample
                            await asyncio.sleep(0.3)  # Throttle request
                            try:
                                df = yf.download(
                                    symbol,
                                    start=start,
                                    end=end,
                                    interval="1h"
                                )
                            except Exception as e:
                                if "YFPricesMissingError" in e.__class__.__name__:
                                    logger.info(f"Skipping crypto back-fill for {symbol_key} {timeframe} {start}->{end}")
                                    expected_bars -= chunk_size
                                    start = end
                                    continue
                                else:
                                    raise
                            if df is not None and not df.empty:
                                df.reset_index(inplace=True)
                                df.rename(columns={'Date': 'Datetime'}, inplace=True)
                                df = self._process_4h_data(df)
                        else:
                            await asyncio.sleep(0.3)  # Throttle request
                            try:
                                df = yf.download(
                                    symbol,
                                    start=start,
                                    end=end,
                                    interval=YF_INTERVAL[timeframe]
                                )
                            except Exception as e:
                                if "YFPricesMissingError" in e.__class__.__name__:
                                    logger.info(f"Skipping crypto back-fill for {symbol_key} {timeframe} {start}->{end}")
                                    expected_bars -= chunk_size
                                    start = end
                                    continue
                                else:
                                    raise
                            if df is not None and not df.empty:
                                df.reset_index(inplace=True)
                                df.rename(columns={'Date': 'Datetime'}, inplace=True)
                        
                        if not df.empty:
                            # Convert to GMT
                            if df['Datetime'].dt.tz is None:
                                df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize('UTC')
                            else:
                                df['Datetime'] = df['Datetime'].dt.tz_convert('GMT')
                            
                            # Drop invalid bars (High < Low or High/Low is 0)
                            df = df[~((df['High'] < df['Low']) | (df['High'] == 0) | (df['Low'] == 0))]
                            
                            if not df.empty:
                                all_new_data.append(df)
                        
                        # Update counters for next chunk
                        expected_bars -= chunk_size
                        start = end
                        
                        # Small delay between chunks
                        await asyncio.sleep(0.2)
                    
                    if all_new_data:
                        # Combine all chunks
                        new_df = pd.concat(all_new_data, ignore_index=True)
                        
                        # Drop duplicates with existing data
                        new_df = new_df[~new_df['Datetime'].isin(existing_df['Datetime'])]
                        
                        if not new_df.empty:
                            # Append new data
                            combined_df = pd.concat([existing_df, new_df])
                            combined_df = combined_df.sort_values('Datetime')
                            combined_df['Datetime'] = combined_df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
                            
                            self._save_data(combined_df, csv_path)
                            
                            # Update status
                            if symbol_key not in self.status:
                                self.status[symbol_key] = {}
                            self.status[symbol_key][timeframe] = datetime.now(timezone.utc).isoformat()
                            
                            logger.info(f"Back-filled {len(new_df)} bars for {symbol_key} {timeframe}")
                            return
                
                except Exception as e:
                    logger.error(f"Error back-filling {symbol_key} {timeframe}: {str(e)}")
                    logger.error(traceback.format_exc())
            
            # If back-fill failed or wasn't needed, try the single-bar update
            latest_bar = await self.fetch_latest_bar(symbol, symbol_key, timeframe)
            if latest_bar is None:
                return
            
            # Convert both to datetime for comparison
            latest_datetime = pd.to_datetime(latest_bar['Datetime'].iloc[0])
            if latest_datetime in existing_df['Datetime'].values:
                return  # Bar already exists
            
            # Drop invalid bars (High < Low or High/Low is 0)
            latest_bar = latest_bar[~((latest_bar['High'] < latest_bar['Low']) | 
                                    (latest_bar['High'] == 0) | 
                                    (latest_bar['Low'] == 0))]
            
            if latest_bar.empty:
                return  # No valid bars to add
            
            # Append new bar
            combined_df = pd.concat([existing_df, latest_bar], ignore_index=True)
            combined_df['Datetime'] = pd.to_datetime(combined_df['Datetime'])
            combined_df = combined_df.sort_values('Datetime')
            combined_df['Datetime'] = combined_df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            self._save_data(combined_df, csv_path)
            
            # Update status only if we actually added a new bar
            if symbol_key not in self.status:
                self.status[symbol_key] = {}
            self.status[symbol_key][timeframe] = datetime.now(timezone.utc).isoformat()
            
            logger.info(f"Updated {symbol_key} {timeframe} - {latest_bar['Datetime'].iloc[0]}")
        else:
            # No existing data, just save the latest bar
            latest_bar = await self.fetch_latest_bar(symbol, symbol_key, timeframe)
            if latest_bar is not None:
                # Drop invalid bars (High < Low or High/Low is 0)
                latest_bar = latest_bar[~((latest_bar['High'] < latest_bar['Low']) | 
                                        (latest_bar['High'] == 0) | 
                                        (latest_bar['Low'] == 0))]
                
                if not latest_bar.empty:
                    self._save_data(latest_bar, csv_path)
                    
                    # Update status for new file
                    if symbol_key not in self.status:
                        self.status[symbol_key] = {}
                    self.status[symbol_key][timeframe] = datetime.now(timezone.utc).isoformat()
                    
                    logger.info(f"Created new file for {symbol_key} {timeframe} - {latest_bar['Datetime'].iloc[0]}")
    
    async def continuous_update_loop(self):
        """Main update loop that runs continuously"""
        logger.info("Starting continuous update loop...")
        
        # Track next update time for each timeframe
        next_updates = {}
        now = datetime.now(timezone.utc)
        
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
            elif timeframe == "1mo":
                if now.month == 12:
                    next_update = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                else:
                    next_update = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
            else:  # 3mo
                month = ((now.month - 1) // 3 + 1) * 3 + 1
                year = now.year
                if month > 12:
                    month = 1
                    year += 1
                next_update = now.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)
            
            next_updates[timeframe] = next_update + timedelta(seconds=5)  # Add 5 seconds buffer
        
        while self.running:
            now = datetime.now(timezone.utc)
            
            # Fetch news events once per day
            await self._fetch_news_events()
            
            # Check which timeframes need updating
            tasks = []
            for timeframe, next_update in next_updates.items():
                if now >= next_update:
                    # Skip intraday frames on weekends for Monday-Friday markets
                    is_weekend = now.weekday() >= 5
                    is_intraday = timeframe in ["1m", "5m", "15m", "1h", "4h"]
                    
                    if is_weekend and is_intraday:
                        # Skip intraday updates for FX, metals, and indices on weekends
                        continue
                    
                    # Update all symbols for this timeframe
                    for symbol_key, symbol in self.symbols.items():
                        tasks.append(self.limited_update(symbol, symbol_key, timeframe))
                    
                    # Calculate next update time
                    config = TIMEFRAME_MAP[timeframe]
                    next_updates[timeframe] = next_update + timedelta(seconds=config["seconds"])
            
            if tasks:
                await asyncio.gather(*tasks)
                self._save_status()
            
            # Sleep for a short time before checking again
            await asyncio.sleep(10)
    
    async def run(self):
        """Main entry point"""
        try:
            logger.info("OHLCV Data Collector starting...")
            self._ensure_directories()
            
            # Initial data load
            await self.initial_load()
            self._save_status()

            # On startup, back-fill any 1m bars still within Yahoo's 7d window
            logger.info("Back-filling missing 1m bars on startup...")
            tasks = [
                self.limited_update(self.symbols[sym_key], sym_key, '1m')
                for sym_key in self.symbols
            ]
            await asyncio.gather(*tasks)
            self._save_status()
            
            # Start continuous updates
            await self.continuous_update_loop()
            
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            self.running = False
            if self.session:
                await self.session.close()
            logger.info("OHLCV Data Collector stopped")

    async def limited_update(self, symbol: str, symbol_key: str, timeframe: str):
        """Wrap update_symbol_timeframe with a semaphore to bound concurrency."""
        async with self.semaphore:
            await self.update_symbol_timeframe(symbol, symbol_key, timeframe)

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
