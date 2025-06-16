#!/usr/bin/env python3
"""
Migration script to clean up duplicate forex pairs and rename folders
"""

import os
import shutil
from pathlib import Path

DATA_DIR = "ohlc_data"

# Mapping of what to keep and what to remove
KEEP_SYMBOLS = {
    'eurusd', 'gbpusd', 'usdjpy', 'usdchf', 'audusd', 'usdcad', 'nzdusd',
    'eurgbp', 'eurjpy', 'gbpjpy', 'chfjpy', 'gbpchf', 'euraud', 'eurcad',
    'gbpaud', 'gbpcad', 'eurchf', 'audcad', 'nzdcad', 'audchf', 'audjpy',
    'audnzd', 'xauusd', 'xagusd', 'us100', 'spy', 'xngusd', 'oil', 'copper',
    'btcusd', 'ethusd'
}

# Mapping for merging duplicate data
MERGE_MAP = {
    'usdgbp': 'gbpusd',  # USDGBP data should go to GBPUSD
    'jpygbp': 'gbpjpy',  # JPYGBP data should go to GBPJPY
    'gbpeur': 'eurgbp',  # GBPEUR data should go to EURGBP
    'btcusdt': 'btcusd', # Rename BTCUSDT to BTCUSD
    'ethusdt': 'ethusd', # Rename ETHUSDT to ETHUSD
}

def migrate_data():
    """Migrate data to new structure"""
    if not os.path.exists(DATA_DIR):
        print("No data directory found.")
        return
    
    print("Data Migration Tool")
    print("=" * 60)
    
    # Get all existing symbol directories
    existing_dirs = [d for d in os.listdir(DATA_DIR) 
                    if os.path.isdir(os.path.join(DATA_DIR, d))]
    
    print(f"Found {len(existing_dirs)} symbol directories")
    
    # Process each directory
    migrated = 0
    removed = 0
    renamed = 0
    
    for symbol_dir in existing_dirs:
        symbol_path = os.path.join(DATA_DIR, symbol_dir)
        
        # Check if this needs to be merged
        if symbol_dir in MERGE_MAP:
            target_dir = MERGE_MAP[symbol_dir]
            target_path = os.path.join(DATA_DIR, target_dir)
            
            print(f"\nMerging {symbol_dir} -> {target_dir}")
            
            # Create target directory if it doesn't exist
            os.makedirs(target_path, exist_ok=True)
            
            # Move all files
            for file in os.listdir(symbol_path):
                src_file = os.path.join(symbol_path, file)
                dst_file = os.path.join(target_path, file)
                
                if os.path.isfile(src_file):
                    # If destination exists and source has data, keep the one with more data
                    if os.path.exists(dst_file):
                        src_size = os.path.getsize(src_file)
                        dst_size = os.path.getsize(dst_file)
                        if src_size > dst_size:
                            shutil.move(src_file, dst_file)
                            print(f"  Replaced {file} (larger file)")
                        else:
                            print(f"  Kept existing {file}")
                    else:
                        shutil.move(src_file, dst_file)
                        print(f"  Moved {file}")
            
            # Remove the old directory
            shutil.rmtree(symbol_path)
            migrated += 1
            
        # Check if this should be removed
        elif symbol_dir not in KEEP_SYMBOLS:
            print(f"\nRemoving duplicate/invalid symbol: {symbol_dir}")
            shutil.rmtree(symbol_path)
            removed += 1
        
        # Check if needs renaming (e.g., BTCUSDT -> btcusd)
        elif symbol_dir.lower() != symbol_dir:
            new_name = symbol_dir.lower()
            new_path = os.path.join(DATA_DIR, new_name)
            print(f"\nRenaming {symbol_dir} -> {new_name}")
            shutil.move(symbol_path, new_path)
            renamed += 1
    
    print("\n" + "=" * 60)
    print("Migration Summary:")
    print(f"  Merged: {migrated} directories")
    print(f"  Removed: {removed} directories")
    print(f"  Renamed: {renamed} directories")
    print(f"  Remaining: {len(os.listdir(DATA_DIR))} directories")
    
    # Clean up status.json if it exists
    if os.path.exists("status.json"):
        print("\nClearing status.json for fresh start...")
        os.remove("status.json")
        print("  Removed status.json")

if __name__ == "__main__":
    # Confirm before proceeding
    print("This will reorganize your OHLC data directory.")
    print("It will merge duplicate forex pairs and remove invalid symbols.")
    response = input("\nProceed? (yes/no): ").lower()
    
    if response == 'yes':
        migrate_data()
        print("\nMigration complete!")
        print("You can now run the updated ohlcv_collector.py")
    else:
        print("Migration cancelled.")
