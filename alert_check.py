#!/usr/bin/env python3
"""
Alert Check Script for OHLCV Data Collector
Sends email alerts when data hasn't been updated for 7+ days
"""

import os
import json
import smtplib
import logging
from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
from typing import Dict, List

# Configuration
STATUS_FILE = "status.json"
LOG_FILE = "alert_check.log"

# Try to import custom config
try:
    from config import EMAIL_CONFIG, STALE_THRESHOLD_DAYS
except ImportError:
    # Use defaults if no custom config
    STALE_THRESHOLD_DAYS = 7
    
    # Email Configuration - Update these with your settings
    EMAIL_CONFIG = {
        "EMAIL_FROM": "your_email@example.com",
        "EMAIL_TO": "your_email@example.com",
        "SMTP_SERVER": "smtp.gmail.com",
        "SMTP_PORT": 587,
        "EMAIL_PASSWORD": "your_app_password"  # Use app-specific password for Gmail
    }

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_status() -> Dict:
    """Load status file"""
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
    return {}

def check_stale_data(status: Dict) -> List[str]:
    """Check for stale data entries"""
    stale_entries = []
    now = datetime.now(timezone.utc)
    threshold = now - timedelta(days=STALE_THRESHOLD_DAYS)
    
    for symbol, timeframes in status.items():
        for timeframe, last_update_str in timeframes.items():
            try:
                last_update = datetime.fromisoformat(last_update_str.replace('Z', '+00:00'))
                if last_update < threshold:
                    days_old = (now - last_update).days
                    stale_entries.append(f"{symbol}/{timeframe} - {days_old} days old")
            except Exception as e:
                logger.error(f"Error parsing timestamp for {symbol}/{timeframe}: {e}")
                stale_entries.append(f"{symbol}/{timeframe} - Invalid timestamp")
    
    return stale_entries

def send_alert_email(stale_entries: List[str]):
    """Send email alert for stale data"""
    if not stale_entries:
        return
    
    try:
        # Create email message
        msg = EmailMessage()
        msg['Subject'] = f'OHLCV Data Alert: {len(stale_entries)} Stale Entries'
        msg['From'] = EMAIL_CONFIG['EMAIL_FROM']
        msg['To'] = EMAIL_CONFIG['EMAIL_TO']
        
        # Create email body
        body = "The following symbol/timeframe combinations have not been updated in 7+ days:\n\n"
        for entry in stale_entries:
            body += f"- {entry}\n"
        
        body += f"\n\nTotal stale entries: {len(stale_entries)}"
        body += f"\nChecked at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        msg.set_content(body)
        
        # Send email
        with smtplib.SMTP(EMAIL_CONFIG['SMTP_SERVER'], EMAIL_CONFIG['SMTP_PORT']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['EMAIL_FROM'], EMAIL_CONFIG['EMAIL_PASSWORD'])
            server.send_message(msg)
        
        logger.info(f"Alert email sent successfully for {len(stale_entries)} stale entries")
        
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")

def main():
    """Main function"""
    logger.info("Starting stale data check...")
    
    # Load status
    status = load_status()
    if not status:
        logger.warning("No status file found or status file is empty")
        return
    
    # Check for stale data
    stale_entries = check_stale_data(status)
    
    if stale_entries:
        logger.warning(f"Found {len(stale_entries)} stale entries")
        for entry in stale_entries:
            logger.warning(f"Stale: {entry}")
        
        # Send alert email
        send_alert_email(stale_entries)
    else:
        logger.info("No stale entries found - all data is up to date")

if __name__ == "__main__":
    main()
