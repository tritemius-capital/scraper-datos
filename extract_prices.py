#!/usr/bin/env python3
"""
Extract prices from Uniswap V2 - Interactive entry point

Usage:
    python3 extract_prices.py
"""

import os
import sys
import logging
from dotenv import load_dotenv
from src.pricing.price_extractor import PriceExtractor

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_user_input():
    """Get token and pool addresses from user input."""
    print("🚀 Uniswap V2 Price Extractor")
    print("=" * 40)
    print()
    
    # Get token address
    while True:
        token_address = input("📍 Address of the token to fetch: ").strip()
        if token_address.startswith('0x') and len(token_address) == 42:
            break
        print("❌ Invalid token address. Must start with '0x' and be 42 characters long.")
        print("   Example: 0x9dd5f960e6d87d004047d15ef97de0c027cc8aaa")
        print()
    
    print()
    
    # Get pool address
    while True:
        pool_address = input("🏊 Address of the pool to fetch: ").strip()
        if pool_address.startswith('0x') and len(pool_address) == 42:
            break
        print("❌ Invalid pool address. Must start with '0x' and be 42 characters long.")
        print("   Example: 0xa9e456df0457df1218118d3909cd580af82a8152")
        print()
    
    print()
    
    # Get number of blocks to extract
    while True:
        try:
            blocks_input = input("📦 Number of blocks to extract from latest block (default: 10000 ~1.4 days): ").strip()
            if blocks_input == "":
                blocks_to_extract = 10000
                break
            blocks_to_extract = int(blocks_input)
            if blocks_to_extract > 0 and blocks_to_extract <= 50000:
                break
            print("❌ Invalid number. Must be between 1 and 50,000 blocks.")
        except ValueError:
            print("❌ Invalid input. Please enter a number.")
    
    return token_address, pool_address, blocks_to_extract

def main():
    # Get addresses from user input
    token_address, pool_address, blocks_to_extract = get_user_input()
    
    etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")
    
    if not etherscan_api_key:
        print("❌ Error: ETHERSCAN_API_KEY not found in .env file")
        print("   Add ETHERSCAN_API_KEY=your_key to the .env file")
        sys.exit(1)
    
    print()
    print(f"🔍 Token: {token_address}")
    print(f"🏊 Pool: {pool_address}")
    print()
    
    try:
        # Create extractor
        extractor = PriceExtractor(etherscan_api_key)
        
        # Get latest block and calculate range (last 10,000 blocks)
        print("📡 Getting latest block...")
        latest_block = extractor.etherscan_client.get_latest_block()
        end_block = latest_block
        start_block = end_block - blocks_to_extract
        
        # Calculate approximate time period
        hours = blocks_to_extract * 12 / 3600  # 12 seconds per block
        days = hours / 24
        
        if days >= 1:
            time_period = f"~{days:.1f} days"
        else:
            time_period = f"~{hours:.1f} hours"
        
        print(f"📦 Extracting prices from blocks {start_block} to {end_block} ({time_period} of data)")
        print()
        
        # Extract prices
        print("🔄 Extracting prices...")
        prices = extractor.extract_prices(token_address, pool_address, start_block, end_block)
        
        if not prices:
            print("❌ No prices found")
            print("   Possible causes:")
            print("   - No swap events in the block range")
            print("   - Incorrect pool address")
            print("   - Problems with the API key")
            sys.exit(1)
        
        print(f"✅ Extracted {len(prices)} price points")
        print()
        
        # Save in object format (2 cells: address + all objects)
        base_filename = f"prices_{token_address}_{start_block}_{end_block}"
        object_csv_file = f"data/{base_filename}_objects.csv"
        
        print("💾 Saving in object format...")
        extractor.save_prices_to_object_csv(prices, object_csv_file, token_address, pool_address)
        print(f"✅ Saved in: {object_csv_file}")
        print()
        
        # Calculate and show price statistics
        stats = extractor.calculate_price_stats(prices)
        print("📊 Price Statistics:")
        print(f"  💰 Lowest Price: ${stats['lowest_price_usd']:.8f} (Block {stats['lowest_price_block']})")
        print(f"  📈 Current Price: ${stats['current_price_usd']:.8f} (Block {stats['current_price_block']})")
        print(f"  🚀 Highest Price: ${stats['highest_price_usd']:.8f} (Block {stats['highest_price_block']})")
        print(f"  📊 Change from Low: {stats['price_change_from_low']:.2f}%")
        print(f"  📉 Change from High: {stats['price_change_from_high']:.2f}%")
        print(f"  🔄 Total Swaps: {stats['total_swaps']}")
        print()
        print("🎉 Extraction completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 