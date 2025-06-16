"""
Configuration template for OHLCV Data Collector
Copy this to config.py and update with your settings
"""

# Email Configuration for alert_check.py
EMAIL_CONFIG = {
    "EMAIL_FROM": "your_email@example.com",    # Sender email
    "EMAIL_TO": "your_email@example.com",      # Recipient email (can be same as FROM)
    "SMTP_SERVER": "smtp.gmail.com",           # SMTP server
    "SMTP_PORT": 587,                          # SMTP port (587 for TLS, 465 for SSL)
    "EMAIL_PASSWORD": "your_app_password"      # App-specific password
}

# Gmail Setup Instructions:
# 1. Enable 2-factor authentication: https://myaccount.google.com/security
# 2. Generate app password: https://myaccount.google.com/apppasswords
# 3. Use the 16-character app password as EMAIL_PASSWORD

# Other Email Providers:
# Outlook/Hotmail: smtp-mail.outlook.com (port 587)
# Yahoo: smtp.mail.yahoo.com (port 587 or 465)
# ProtonMail: Requires ProtonMail Bridge
# Custom/Corporate: Contact your IT department for SMTP details

# Optional: Override stale threshold (default is 7 days)
STALE_THRESHOLD_DAYS = 7

# Optional: Override TradingEconomics API key
# (Default key is provided, but you can use your own)
TE_API_KEY = "68951cae12474dd:0zn4h4vjkr9cs1h"
