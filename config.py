"""
Configuration for OHLCV Data Collector

Store sensitive information and configurable parameters here.
"""

# Email Configuration for alert_check.py
EMAIL_CONFIG = {
    "EMAIL_FROM": "dcollector631@gmail.com",    # Sender email
    "EMAIL_TO": "ihavetekkit@gmail.com",      # Recipient email (can be same as FROM)
    "SMTP_SERVER": "smtp.gmail.com",           # SMTP server
    "SMTP_PORT": 587,                          # SMTP port (587 for TLS, 465 for SSL)
    "EMAIL_PASSWORD": "iatg nmkp eaow toev"      # App-specific password
}

STALE_THRESHOLD_DAYS = 7

# Trading Economics API Key
# -------------------------
# Get your free API key from https://developer.tradingeconomics.com/
#
# The default guest key provides a limited sample of data. For comprehensive
# news coverage (USD, EUR, GBP, etc.), you need to register for a key.
#
# Example: TE_API_KEY = "your_api_key_here"
TE_API_KEY = "your_api_key_here"

# Email Password: DataCollector1234321!!