#!/usr/bin/env python3
"""
Test script for Big Buy Analyzer
"""

import os
import sys
import logging
from dotenv import load_dotenv
from src.pricing.big_buy_analyzer import BigBuyAnalyzer

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
    
    # Get latest block and test with last 1000 blocks
    from src.client.etherscan_client import EtherscanClient
    client = EtherscanClient(etherscan_api_key)
    latest_block = client.get_latest_block()
    end_block = latest_block
    start_block = end_block - 1000  # Test with last 1000 blocks
    
    print(f"🔍 Testing Big Buy Analyzer")
    print(f"📍 Token: {token_address}")
    print(f"🏊 Pool: {pool_address}")
    print(f"📦 Block range: {start_block} to {end_block}")
    print()
    
    # Create analyzer
    analyzer = BigBuyAnalyzer(etherscan_api_key)
    
    # Find big buys
    print("🔄 Searching for big buys...")
    big_buys = analyzer.find_big_buys_in_history(
        token_address, pool_address, start_block, end_block, threshold_eth=0.1
    )
    
    if big_buys:
        print(f"✅ Found {len(big_buys)} big buys >= 0.1 ETH")
        print()
        
        # Show details of each big buy
        for i, big_buy in enumerate(big_buys, 1):
            print(f"Big Buy #{i}:")
            print(f"  📄 TX Hash: {big_buy['tx_hash']}")
            print(f"  📦 Block: {big_buy['block_number']}")
            print(f"  ⏰ Timestamp: {big_buy['timestamp']}")
            print(f"  💰 ETH Amount: {big_buy['eth_amount']:.4f} ETH")
            print(f"  🎯 Token Amount: {big_buy['token_amount_out']:.8f}")
            print(f"  👤 From: {big_buy['from_address']}")
            print(f"  📍 To: {big_buy['to_address']}")
            print()
    else:
        print("❌ No big buys found")
        print("   This could mean:")
        print("   - No purchases >= 0.1 ETH in this block range")
        print("   - Pool address is incorrect")
        print("   - Token doesn't have WETH pair")

if __name__ == "__main__":
    main() 