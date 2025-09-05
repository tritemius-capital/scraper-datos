#!/usr/bin/env python3
"""
Debug script to verify pool information and recent activity
"""

import sys
sys.path.append('src')

from client.etherscan_client import EtherscanClient

def debug_pool():
    """Debug pool information and recent activity"""
    
    print("🔍 Pool Debug Information")
    print("=" * 50)
    
    token_address = "0x9dd5f960e6d87d004047d15ef97de0c027cc8aaa"  # EMMANUEL
    pool_address = "0xa9e456df0457df1218118d3909cd580af82a8152"   # Supposed V2 Pool
    api_key = "N2YPVNFGPTAJD7B9KEVKWSU2VEVBTQJV5Z"
    
    print(f"📍 Token: {token_address}")
    print(f"🏊 Pool: {pool_address}")
    
    try:
        client = EtherscanClient(api_key)
        
        # Get latest block
        latest_block = client.get_latest_block()
        print(f"📦 Latest block: {latest_block}")
        
        # Check for recent activity in a larger range
        start_block = latest_block - 10000  # Last ~10k blocks
        end_block = latest_block
        
        print(f"\n🔍 Searching for swap events in blocks {start_block} to {end_block}")
        
        # V2 Swap event signature
        swap_topic_v2 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
        
        # Try to get logs directly
        print("📡 Querying Etherscan for swap events...")
        
        logs = client.get_logs(
            address=pool_address,
            from_block=start_block,
            to_block=end_block,
            topic0=swap_topic_v2  # Use topic0 instead of topics list
        )
        
        print(f"✅ Found {len(logs)} swap events in last 10k blocks")
        
        if logs:
            print("\n📋 Recent swap events:")
            for i, log in enumerate(logs[-5:]):  # Show last 5
                block = int(log.get('blockNumber', '0'), 16)
                tx = log.get('transactionHash', '')
                print(f"  {i+1}. Block {block}: {tx}")
        else:
            print("❌ No swap events found")
            
            # Let's try getting ANY events from this address
            print("\n🔍 Checking for ANY events from this address...")
            all_logs = client.get_logs(
                address=pool_address,
                from_block=start_block,
                to_block=end_block
                # No topics filter - get all events
            )
            print(f"📊 Total events from this address: {len(all_logs)}")
            
            if all_logs:
                print("📋 Sample events:")
                for i, log in enumerate(all_logs[:3]):
                    block = int(log.get('blockNumber', '0'), 16)
                    topics = log.get('topics', [])
                    topic0 = topics[0] if topics else 'No topics'
                    print(f"  {i+1}. Block {block}: Topic0 = {topic0}")
            
            # Maybe this isn't a Uniswap V2 pool? Let's check if it's a contract
            print(f"\n🔍 Let's verify this is actually a contract...")
            
            # We can't easily check if it's a contract via Etherscan API,
            # but we can see if it has any transaction history
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_pool() 