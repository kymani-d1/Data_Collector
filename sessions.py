"""
Session definitions for different trading instruments
Defines market hours, weekend breaks, and daily maintenance periods
"""

import pytz
from datetime import datetime, timedelta, timezone

# Map timeframe strings to their duration in minutes
TF_TO_MINUTES = {
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

def tf_minutes(timeframe: str) -> int:
    """Convert timeframe string to minutes, default to huge number for unknown timeframes"""
    return TF_TO_MINUTES.get(timeframe, 10**9)  # default huge so you skip breaks

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

def closed_window(sym: str, start: datetime, end: datetime, timeframe: str = None) -> bool:
    """Check if a time window overlaps a scheduled market closure.
       FX -> 24×5 Fri 22:00–Sun 22:00 UTC; Crypto -> always open; else use SESSIONS."""
    # normalize UTC
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    # identify FX vs crypto vs others
    currencies = get_symbol_currencies(sym)
    is_fx = len(currencies) == 2
    is_crypto = sym.lower() in {'btc', 'eth'}

    # 1) explicit sessions config (cash/futures)
    # map directory name to Yahoo symbol for lookup
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
        'sp500': 'ES=F', 'nasdaq': 'NQ=F', 'spy': 'SPY'
    }
    yahoo = symbol_map.get(sym.lower())
    cfg = SESSIONS.get(yahoo) if yahoo else None
    if cfg:
        tz = pytz.timezone(cfg["timezone"])
        # localize start/end
        local_start = start.astimezone(tz)
        local_end = end.astimezone(tz)
        # weekend interval
        wk_from_day, wk_from_time = cfg["weekend"][0].split()
        wk_to_day, wk_to_time   = cfg["weekend"][1].split()
        # parse weekdays
        wd_map = {'Mon':0,'Tue':1,'Wed':2,'Thu':3,'Fri':4,'Sat':5,'Sun':6}
        from_wd = wd_map[wk_from_day]
        to_wd   = wd_map[wk_to_day]
        # compute local weekend start
        days_back = (local_start.weekday() - from_wd) % 7
        ws_local = local_start - timedelta(days=days_back)
        ws_local = ws_local.replace(hour=int(wk_from_time[:2]), minute=int(wk_from_time[3:]), second=0, microsecond=0)
        # compute local weekend end
        span = (to_wd - from_wd) % 7
        we_local = ws_local + timedelta(days=span)
        we_local = we_local.replace(hour=int(wk_to_time[:2]), minute=int(wk_to_time[3:]), second=0, microsecond=0)
        # convert back to UTC
        ws_utc = ws_local.astimezone(timezone.utc)
        we_utc = we_local.astimezone(timezone.utc)
        # if intervals overlap
        if start < we_utc and end > ws_utc:
            return True

        # daily maintenance breaks (only skip if timeframe is shorter than the break)
        for brk in cfg.get("daily_breaks", []):
            t0, t1 = brk
            bh, bm = map(int, t0.split(':'))
            eh, em = map(int, t1.split(':'))
            # compute break interval in local tz
            db_start = local_start.replace(hour=bh, minute=bm, second=0, microsecond=0)
            db_end   = local_start.replace(hour=eh, minute=em, second=0, microsecond=0)
            # compute break duration (in minutes), wrap around midnight if needed
            raw_minutes = ((eh * 60 + em) - (bh * 60 + bm)) % (24 * 60)
            # if user passed a timeframe, skip maintenance-break logic only when the bar length
            # is _longer_ than the break itself
            if timeframe is not None:
                tf_min = tf_minutes(timeframe)
                if tf_min > raw_minutes:
                    continue
            # convert back to UTC
            db_utc_start = db_start.astimezone(timezone.utc)
            db_utc_end   = db_end.astimezone(timezone.utc)
            # if our gap overlaps the maintenance window, treat it as closed
            if start < db_utc_end and end > db_utc_start:
                return True
        return False

    # 2) FX fallback: 24×5 Fri 22:00–Sun 22:00 UTC
    if is_fx:
        # find last Fri at 22:00 UTC before start
        day = start.weekday()
        delta = (day - 4) % 7
        fri = start - timedelta(days=delta)
        ws = fri.replace(hour=22, minute=0, second=0, microsecond=0)
        we = ws + timedelta(days=2)  # Sun 22:00
        if start < we and end > ws:
            return True
        return False

    # 3) crypto or anything else: always open
    return False

SESSIONS = {
    # ---------- FX pairs (24h Mon–Fri, but Yahoo has a daily maintenance window) ----------
    "EURUSD=X": {
        "timezone": "UTC",
        "weekend": ("Fri 22:00", "Sun 22:00"),
        "daily_breaks": [("05:00", "06:00")],
    },
    "GBPUSD=X": {
        "timezone": "UTC",
        "weekend": ("Fri 22:00", "Sun 22:00"),
        "daily_breaks": [("05:00", "06:00")],
    },
    "USDJPY=X": {
        "timezone": "UTC",
        "weekend": ("Fri 22:00", "Sun 22:00"),
        "daily_breaks": [("05:00", "06:00")],
    },
    "USDCHF=X": {
        "timezone": "UTC",
        "weekend": ("Fri 22:00", "Sun 22:00"),
        "daily_breaks": [("05:00", "06:00")],
    },
    "AUDUSD=X": {
        "timezone": "UTC",
        "weekend": ("Fri 22:00", "Sun 22:00"),
        "daily_breaks": [("05:00", "06:00")],
    },
    "EURGBP=X": {
        "timezone": "UTC",
        "weekend": ("Fri 22:00", "Sun 22:00"),
        "daily_breaks": [("05:00", "06:00")],
    },
    "EURJPY=X": {
        "timezone": "UTC",
        "weekend": ("Fri 22:00", "Sun 22:00"),
        "daily_breaks": [("05:00", "06:00")],
    },
    "GBPJPY=X": {
        "timezone": "UTC",
        "weekend": ("Fri 22:00", "Sun 22:00"),
        "daily_breaks": [("05:00", "06:00")],
    },
    "EURCHF=X": {
        "timezone": "UTC",
        "weekend": ("Fri 22:00", "Sun 22:00"),
        "daily_breaks": [("05:00", "06:00")],
    },

    # ---------- CME Globex metals & energies ----------
    "GC=F": {          # Gold
        "timezone": "UTC",
        "weekend": ("Fri 21:00", "Sun 21:00"),
        "daily_breaks": [("21:00", "22:00")],
    },
    "CL=F": {          # Crude Oil
        "timezone": "UTC",
        "weekend": ("Fri 21:00", "Sun 21:00"),
        "daily_breaks": [("21:00", "22:00")],
    },
    "SI=F": {          # Silver
        "timezone": "UTC",
        "weekend": ("Fri 21:00", "Sun 21:00"),
        "daily_breaks": [("21:00", "22:00")],
    },
    "HG=F": {          # Copper
        "timezone": "UTC",
        "weekend": ("Fri 21:00", "Sun 21:00"),
        "daily_breaks": [("21:00", "22:00")],
    },
    "NG=F": {          # Natural Gas
        "timezone": "UTC",
        "weekend": ("Fri 21:00", "Sun 21:00"),
        "daily_breaks": [("21:00", "22:00")],
    },

    # ---------- Index futures ----------
    "ES=F": {          # S&P 500 Futures
        "timezone": "UTC",
        "weekend": ("Fri 21:00", "Sun 21:00"),
        "daily_breaks": [("21:00", "22:00")],
    },
    "NQ=F": {          # Nasdaq Futures
        "timezone": "UTC",
        "weekend": ("Fri 21:00", "Sun 21:00"),
        "daily_breaks": [("21:00", "22:00")],
    },

    # ---------- Cash equities / ETFs ----------
    "SPY": {           # S&P 500 ETF
        "timezone": "UTC",
        "weekend": ("Fri 20:00", "Sun 20:00"),
        # Market open 13:30–20:00 UTC
        "daily_breaks": [
            ("00:00", "13:30"),
            ("20:00", "23:59"),
        ],
    },
    
    # ---------- Crypto ----------
    "BTC-USD": {
        "timezone": "UTC",
    },
    "ETH-USD": {
        "timezone": "UTC",
    },
} 