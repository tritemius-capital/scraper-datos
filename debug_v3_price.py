#!/usr/bin/env python3
"""
Debug V3 price calculation
"""

import sys
sys.path.append('.')

from src.client.web3_client import Web3Client
from web3 import Web3

def debug_v3_price():
    """Debug V3 WETH/USDC price calculation"""
    print("🔍 Debugging V3 WETH/USDC price calculation...")
    
    try:
        # Create web3 client
        web3_client = Web3Client()
        w3 = web3_client.w3
        
        # WETH/USDC 0.05% pool
        pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
        test_block = 23296543
        
        print(f"📦 Testing block: {test_block}")
        print(f"🏊 Pool address: {pool_address}")
        
        # Check if pool exists (use checksum address)
        pool_checksum = Web3.to_checksum_address(pool_address)
        pool_code = w3.eth.get_code(pool_checksum)
        print(f"📄 Pool has code: {len(pool_code) > 0}")
        
        # Basic pool info
        slot0_abi = [
            {
                "inputs": [],
                "name": "slot0",
                "outputs": [
                    {"type": "uint160", "name": "sqrtPriceX96"},
                    {"type": "int24", "name": "tick"},
                    {"type": "uint16", "name": "observationIndex"},
                    {"type": "uint16", "name": "observationCardinality"},
                    {"type": "uint16", "name": "observationCardinalityNext"},
                    {"type": "uint8", "name": "feeProtocol"},
                    {"type": "bool", "name": "unlocked"}
                ],
                "stateMutability": "view",
                "type": "function"
            }
        ]
        
        # Get token addresses
        token_abi = [
            {"inputs": [], "name": "token0", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "token1", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "fee", "outputs": [{"type": "uint24"}], "stateMutability": "view", "type": "function"}
        ]
        
        pool_contract = w3.eth.contract(
            address=Web3.to_checksum_address(pool_address),
            abi=token_abi + slot0_abi
        )
        
        # Get pool info
        token0 = pool_contract.functions.token0().call()
        token1 = pool_contract.functions.token1().call()
        fee = pool_contract.functions.fee().call()
        
        print(f"🪙 Token0: {token0}")
        print(f"🪙 Token1: {token1}")
        print(f"💰 Fee: {fee}")
        
        # Expected addresses (correct mainnet addresses)
        WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        USDC = "0xA0b86a33E6417c24b5e8d2d6c28b67c6e3a8b1e2f"  # Circle USDC
        
        print(f"\n🔍 Address verification:")
        print(f"   Expected WETH: {WETH}")
        print(f"   Expected USDC: {USDC}")
        print(f"   Token0 == WETH: {token0.lower() == WETH.lower()}")
        print(f"   Token1 == USDC: {token1.lower() == USDC.lower()}")
        print(f"   Token0 == USDC: {token0.lower() == USDC.lower()}")
        print(f"   Token1 == WETH: {token1.lower() == WETH.lower()}")
        
        # Get current slot0 data
        print(f"\n📊 Current slot0 data:")
        slot0_current = pool_contract.functions.slot0().call()
        print(f"   sqrtPriceX96: {slot0_current[0]}")
        print(f"   tick: {slot0_current[1]}")
        
        # Get historical slot0 data
        print(f"\n📊 Historical slot0 data (block {test_block}):")
        try:
            slot0_historical = pool_contract.functions.slot0().call(block_identifier=test_block)
            sqrt_price_x96 = slot0_historical[0]
            tick = slot0_historical[1]
            
            print(f"   sqrtPriceX96: {sqrt_price_x96}")
            print(f"   tick: {tick}")
            
            if sqrt_price_x96 > 0:
                # Try different price calculation methods
                print(f"\n🧮 Price calculations:")
                
                # Method 1: Direct calculation
                price_raw = (sqrt_price_x96 ** 2) / (2 ** 192)
                print(f"   Raw price ratio: {price_raw}")
                
                # Method 2: With decimal adjustment
                usdc_per_weth = price_raw / (10 ** 12)  # 18-6 decimals
                print(f"   USDC per WETH: {usdc_per_weth}")
                
                # Method 3: Inverse
                weth_per_usdc = 1 / price_raw * (10 ** 12)
                print(f"   WETH per USDC (inverse): {weth_per_usdc}")
                
                # Method 4: Using tick
                price_from_tick = 1.0001 ** tick
                print(f"   Price from tick: {price_from_tick}")
                
            else:
                print(f"   ❌ sqrtPriceX96 is 0!")
                
        except Exception as e:
            print(f"   ❌ Error getting historical data: {e}")
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_v3_price() 