#!/usr/bin/env python3
"""
Test script for big buy detection
Tests with a small number of blocks to see if WETH purchases are detected correctly
"""

import sys
import os
sys.path.append('src')

from uniswap import UniswapExtractorFactory
from pricing.object_csv_writer import ObjectCSVWriter

def test_big_buy_detection():
    """Test big buy detection with a small sample"""
    
    print("🔍 Testing Big Buy Detection")
    print("=" * 50)
    
    # Test parameters - using a known active period
    token_address = "0x8cdda18f0fd28096c839efc487456b50702f7d09"  # EMMANUEL
    pool_address = "0xcf072d3e71a7799b235f126dd7c1afbbf65c3555"   # V2 Pool
    api_key = "N2YPVNFGPTAJD7B9KEVKWSU2VEVBTQJV5Z"  # Etherscan API key
    
    # Analyze around known activity (1000 blocks should be manageable)
    blocks_to_analyze = 1000  # More manageable range
    known_active_block = 23287652  # Where we found activity
    
    print(f"📍 Token: {token_address}")
    print(f"🏊 Pool: {pool_address}")
    print(f"📦 Blocks to analyze: {blocks_to_analyze} around known activity")
    
    try:
        # Create extractor with API key
        factory = UniswapExtractorFactory()
        extractor = factory.create_extractor("v2", api_key)
        
        # Use range around known activity
        start_block = known_active_block - 500
        end_block = known_active_block + 500
        
        print(f"📦 Block range: {start_block} to {end_block}")
        print("⚠️  This should take about 1-2 minutes...")
        
        print("\n🔄 Analyzing with big buy detection enabled...")
        
        # Run analysis with correct parameters
        result = extractor.analyze_token_complete(
            token_address=token_address,
            pool_address=pool_address,
            start_block=start_block,
            end_block=end_block,
            threshold_eth=0.01  # Lower threshold to catch smaller buys
        )
        
        if result:
            print(f"📊 Result keys: {list(result.keys())}")
            
            # Show any errors first
            if 'error' in result:
                print(f"❌ Error in result: {result['error']}")
            
            # Check prices
            if 'prices' in result:
                prices = result['prices']
                print(f"💰 Prices found: {len(prices)}")
                if prices:
                    print(f"    Sample price: {prices[0]}")
            
            # Check big buys
            if 'big_buy_analysis' in result:
                big_buy_data = result['big_buy_analysis']
                print(f"🔍 Big buy analysis keys: {list(big_buy_data.keys()) if isinstance(big_buy_data, dict) else 'Not a dict'}")
                
                if isinstance(big_buy_data, dict):
                    big_buys = big_buy_data.get('big_buys', [])
                    print(f"✅ Big buys found: {len(big_buys)}")
                    
                    if big_buys:
                        print("\n📊 Big Buy Details:")
                        for i, buy in enumerate(big_buys[:5]):  # Show first 5
                            print(f"  {i+1}. Block {buy.get('blockNumber', 'N/A')}: "
                                  f"{buy.get('ethAmount', 0):.6f} ETH "
                                  f"(${buy.get('usdValue', 0):.2f})")
                    else:
                        print("❌ No big buys detected")
                        
                        # Show some analysis details
                        if 'summary' in big_buy_data:
                            print(f"📋 Analysis summary: {big_buy_data['summary']}")
                        if 'total_big_buys' in big_buy_data:
                            print(f"📊 Total big buys: {big_buy_data['total_big_buys']}")
                        if 'total_eth_volume' in big_buy_data:
                            print(f"💎 Total ETH volume: {big_buy_data['total_eth_volume']}")
                
            # Let's also check the swap events directly
            print("\n🔍 Checking swap events directly...")
            swap_events = extractor.get_swap_events(pool_address, start_block, end_block)
            print(f"📊 Total swap events: {len(swap_events)}")
            
            if swap_events:
                print("\n📋 Sample swap events:")
                for i, event in enumerate(swap_events[:3]):
                    print(f"  Event {i+1}: Block {event.get('blockNumber', 'N/A')}")
                    print(f"    TX: {event.get('transactionHash', 'N/A')}")
                    if 'amount0In' in event:
                        print(f"    V2 - amount0In: {event.get('amount0In', 0)}, amount1In: {event.get('amount1In', 0)}")
                        print(f"    V2 - amount0Out: {event.get('amount0Out', 0)}, amount1Out: {event.get('amount1Out', 0)}")
        else:
            print("❌ No result returned")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_big_buy_detection() 