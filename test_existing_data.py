#!/usr/bin/env python3
"""
Test script for Big Buy Analysis from existing CSV data
"""

import os
import sys
import logging
import json
from dotenv import load_dotenv
from src.pricing.big_buy_analyzer import BigBuyAnalyzer

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_prices_from_csv(csv_file):
    """Load price data from existing CSV file."""
    import csv
    
    # Increase field size limit to handle large CSV fields
    csv.field_size_limit(2**31 - 1)  # Maximum field size
    
    prices = []
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Parse the all_blocks column to extract individual price data
                all_blocks = row.get('all_blocks', '')
                if all_blocks:
                    # Split by spaces to get individual blocks
                    blocks = all_blocks.split()
                    for block in blocks:
                        if block.startswith('bloque'):
                            # Extract JSON data from block
                            try:
                                json_start = block.find('{')
                                if json_start != -1:
                                    json_data = block[json_start:]
                                    price_data = json.loads(json_data)
                                    
                                    # Convert to our format
                                    price = {
                                        'timestamp': int(price_data['timestamp']),
                                        'block_number': int(price_data['block_number']),
                                        'token_price_eth': float(price_data['token_price_eth']),
                                        'token_price_usd': float(price_data['token_price_usd']),
                                        'eth_price_usd': float(price_data['eth_price_usd'])
                                    }
                                    prices.append(price)
                            except json.JSONDecodeError:
                                continue
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return []
    
    return prices

def main():
    # Get API key
    etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")
    if not etherscan_api_key:
        print("❌ Error: ETHERSCAN_API_KEY not found in .env file")
        sys.exit(1)
    
    # Use the existing CSV file we already created
    csv_file = "data/prices_0x9dd5f960e6d87d004047d15ef97de0c027cc8aaa_22917588_22932588_objects.csv"
    
    print(f"🔍 Testing Big Buy Analysis from Existing CSV Data")
    print(f"📁 CSV File: {csv_file}")
    print()
    
    # Load prices from existing CSV
    print("📂 Loading prices from existing CSV...")
    prices = load_prices_from_csv(csv_file)
    
    if not prices:
        print("❌ No prices found in CSV file")
        sys.exit(1)
    
    print(f"✅ Loaded {len(prices)} price points from CSV")
    print()
    
    # Create analyzer
    analyzer = BigBuyAnalyzer(etherscan_api_key)
    
    # Analyze big buys from price data
    print("🔍 Analyzing big buys from price data...")
    big_buy_analysis = analyzer.get_big_buy_analysis_from_prices(prices, threshold_eth=0.1)
    
    if big_buy_analysis and big_buy_analysis['big_buys']:
        print(f"✅ Found {len(big_buy_analysis['big_buys'])} big buys >= 0.1 ETH")
        print()
        
        # Show details of each big buy
        for i, big_buy in enumerate(big_buy_analysis['big_buys'], 1):
            print(f"Big Buy #{i}:")
            print(f"  📦 Block: {big_buy['big_buy_block']}")
            print(f"  ⏰ Timestamp: {big_buy['big_buy_timestamp']}")
            print(f"  💰 Price at Big Buy: ${big_buy['price_at_big_buy']:.8f}")
            print(f"  📈 Max Price 5d: ${big_buy['max_price_5d']:.8f}")
            print(f"  📉 Min Price 5d: ${big_buy['min_price_5d']:.8f}")
            print(f"  🚀 Max Change: {big_buy['price_change_max_5d']:.2f}%")
            print(f"  📉 Min Change: {big_buy['price_change_min_5d']:.2f}%")
            print(f"  💸 Estimated ETH: {big_buy['eth_amount']:.4f} ETH")
            print(f"  📊 Price Change: {big_buy['price_change_percent']:.2f}%")
            print(f"  ⚠️  Estimated: {'Yes' if big_buy.get('estimated', False) else 'No'}")
            print()
    else:
        print("❌ No big buys found")
        print("   This could mean:")
        print("   - No significant price movements in this range")
        print("   - Threshold too high (try lowering it)")
        print("   - Need to analyze more blocks")

if __name__ == "__main__":
    main() 