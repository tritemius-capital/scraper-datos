#!/usr/bin/env python3
"""
Test script for historical ETH price calculation
"""

import sys
sys.path.append('.')

from src.pricing.enhanced_usdt_oracle import EnhancedUSDTOracle
from src.client.web3_client import Web3Client

def test_historical_eth_price():
    """Test historical ETH price fetching"""
    print("🧪 Testing historical ETH price calculation...")
    
    try:
        # Create web3 client
        web3_client = Web3Client()
        oracle = EnhancedUSDTOracle(web3_client)
        
        # Test with recent blocks from our data
        test_blocks = [23296543, 23296649, 23296668]  # Blocks with big buys
        
        for block in test_blocks:
            print(f"\n📦 Block {block}:")
            
            # Get ETH price for this block
            eth_price_scaled = oracle.get_eth_usdt_price_scaled(block)
            if eth_price_scaled:
                eth_price_usd = eth_price_scaled / 1_000_000
                print(f"   ETH Price: ${eth_price_usd:.2f}")
                
                # Test a sample WETH amount (0.1 ETH)
                test_weth_amount = int(0.1 * 1e18)  # 0.1 ETH in wei
                usdt_value = (test_weth_amount * eth_price_scaled) // (10 ** 18)
                usdt_dollars = usdt_value / 1_000_000
                
                print(f"   0.1 ETH = ${usdt_dollars:.2f} USDT")
                print(f"   Big buy threshold (≥0.1 ETH): {'✅ YES' if usdt_dollars >= 100 else '❌ NO'}")
            else:
                print(f"   ❌ Failed to get ETH price")
        
        print(f"\n🎯 Cache status: {len(oracle.eth_usdt_price_cache)} entries")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_historical_eth_price() 