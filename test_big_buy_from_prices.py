#!/usr/bin/env python3
"""
Test script for Big Buy Analysis from existing price data
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

def main():
    # Get API key
    etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")
    if not etherscan_api_key:
        print("❌ Error: ETHERSCAN_API_KEY not found in .env file")
        sys.exit(1)
    
    # Test parameters
    token_address = "0x9dd5f960e6d87d004047d15ef97de0c027cc8aaa"
    pool_address = "0xa9e456df0457df1218118d3909cd580af82a8152"
    
    print(f"🔍 Testing Big Buy Analysis from Price Data")
    print(f"📍 Token: {token_address}")
    print(f"🏊 Pool: {pool_address}")
    print()
    
    # Create extractor
    extractor = PriceExtractor(etherscan_api_key)
    
    # Get latest block and test with last 5000 blocks
    latest_block = extractor.etherscan_client.get_latest_block()
    end_block = latest_block
    start_block = end_block - 5000  # Test with last 5000 blocks
    
    print(f"📦 Extracting prices from blocks {start_block} to {end_block}")
    print()
    
    # Extract prices
    print("🔄 Extracting prices...")
    prices = extractor.extract_prices(token_address, pool_address, start_block, end_block)
    
    if not prices:
        print("❌ No prices found")
        sys.exit(1)
    
    print(f"✅ Extracted {len(prices)} price points")
    print()
    
    # Analyze big buys from price data
    print("🔍 Analyzing big buys from price data...")
    big_buy_analysis = extractor.big_buy_analyzer.get_big_buy_analysis_from_prices(prices, threshold_eth=0.1)
    
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