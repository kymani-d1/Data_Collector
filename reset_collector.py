#!/usr/bin/env python3
"""
Reset script to clean up and prepare for fresh data collection
"""

import os
import shutil
import json

def reset_collector(keep_data=True):
    """Reset the collector state"""
    print("OHLCV Collector Reset Tool")
    print("=" * 60)
    
    # Remove status.json
    if os.path.exists("status.json"):
        os.remove("status.json")
        print("✓ Removed status.json")
    
    # Clear logs
    log_files = ["collector.log", "errors.log", "alert_check.log"]
    for log_file in log_files:
        if os.path.exists(log_file):
            os.remove(log_file)
            print(f"✓ Removed {log_file}")
    
    if not keep_data:
        # Remove all data
        if os.path.exists("ohlc_data"):
            shutil.rmtree("ohlc_data")
            print("✓ Removed all OHLC data")
    else:
        # Just clean up invalid directories
        if os.path.exists("ohlc_data"):
            valid_symbols = {
                'eurusd', 'gbpusd', 'usdjpy', 'usdchf', 'audusd', 'usdcad', 'nzdusd',
                'eurgbp', 'eurjpy', 'gbpjpy', 'chfjpy', 'gbpchf', 'euraud', 'eurcad',
                'gbpaud', 'gbpcad', 'eurchf', 'audcad', 'nzdcad', 'audchf', 'audjpy',
                'audnzd', 'xauusd', 'xagusd', 'us100', 'spy', 'xngusd', 'oil', 'copper',
                'btcusd', 'ethusd'
            }
            
            removed = 0
            for symbol_dir in os.listdir("ohlc_data"):
                if symbol_dir not in valid_symbols:
                    path = os.path.join("ohlc_data", symbol_dir)
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                        removed += 1
            
            if removed > 0:
                print(f"✓ Removed {removed} invalid symbol directories")
    
    print("\nReset complete!")
    print("You can now run: python ohlcv_collector.py")

if __name__ == "__main__":
    print("This will reset the OHLCV collector state.")
    print("\nOptions:")
    print("1. Keep existing data (remove only logs and status)")
    print("2. Remove everything (full reset)")
    
    choice = input("\nYour choice (1 or 2): ").strip()
    
    if choice == "1":
        reset_collector(keep_data=True)
    elif choice == "2":
        confirm = input("Are you sure you want to delete all data? (yes/no): ").lower()
        if confirm == "yes":
            reset_collector(keep_data=False)
        else:
            print("Reset cancelled.")
    else:
        print("Invalid choice. Reset cancelled.")
