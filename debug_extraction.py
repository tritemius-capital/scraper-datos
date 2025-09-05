#!/usr/bin/env python3
"""
Debug script to test token extraction step by step
"""

import logging
from src.uniswap import UniswapExtractorFactory
from src.client.web3_client import Web3Client

# Set up logging to see all messages
logging.basicConfig(level=logging.DEBUG)

def debug_extraction():
    try:
        # Test with first token from CSV
        token_address = '0x6d44ddba07a9373d665ce636f639e1c46565a349'
        pool_address = '0x7c557cb92b0f496c19d40579f5fafbd539472727'
        version = 'v3'
        
        print(f"🔍 Debugging extraction for:")
        print(f"Token: {token_address}")
        print(f"Pool: {pool_address}")
        print(f"Version: {version}")
        
        # Create extractor with Archive Node
        web3_client = Web3Client()
        factory = UniswapExtractorFactory()
        extractor = factory.create_extractor_with_node(version, web3_client)
        
        # Get latest block and set range
        latest_block = extractor.get_latest_block()
        start_block = latest_block - 1000  # Larger range for testing
        end_block = latest_block
        
        print(f"📦 Block range: {start_block} to {end_block}")
        
        # Step 1: Get pool info
        print("\n🏊 Step 1: Getting pool info...")
        try:
            pool_info = extractor.get_pool_info(pool_address)
            print(f"Pool info: {pool_info}")
        except Exception as e:
            print(f"❌ Error getting pool info: {e}")
            return
        
        # Step 2: Get swap events
        print("\n📊 Step 2: Getting swap events...")
        try:
            events = extractor.get_swap_events(pool_address, start_block, end_block)
            print(f"Found {len(events)} events")
            
            if events:
                first_event = events[0]
                print(f"First event keys: {list(first_event.keys())}")
                print(f"First event data sample: {first_event}")
            else:
                print("No events found!")
                return
        except Exception as e:
            print(f"❌ Error getting events: {e}")
            import traceback
            traceback.print_exc()
            return
        
        # Step 3: Try to decode first event
        print("\n🔍 Step 3: Decoding first event...")
        try:
            first_event = events[0]
            decoded = extractor.decode_swap_event(log=first_event, pool_info=pool_info)
            print(f"Decoded event: {decoded}")
            
            if not decoded:
                print("❌ Decoding returned None!")
        except Exception as e:
            print(f"❌ Error decoding event: {e}")
            import traceback
            traceback.print_exc()
        
        # Step 4: Try full price extraction
        print("\n💰 Step 4: Full price extraction...")
        try:
            prices = extractor.extract_prices(token_address, pool_address, start_block, end_block)
            print(f"Extracted {len(prices)} prices")
            
            if prices:
                print(f"First price: {prices[0]}")
            else:
                print("❌ No prices extracted!")
        except Exception as e:
            print(f"❌ Error extracting prices: {e}")
            import traceback
            traceback.print_exc()
            
    except Exception as e:
        print(f"❌ Main error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_extraction() 