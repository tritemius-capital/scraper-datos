#!/usr/bin/env python3
"""
Ethereum Token Price Extractor & Big Buy Analyzer

This tool extracts historical price data for ERC-20 tokens from Uniswap pools
and analyzes big buy patterns. Supports both Uniswap V2 and V3.
"""

import sys
import os
from src.uniswap import UniswapExtractorFactory
from src.pricing.object_csv_writer import ObjectCSVWriter
from src.client.web3_client import Web3Client
from src.config import USE_LOCAL_NODE

def get_data_source_choice():
    """Get user's choice for data source"""
    print("=== Data Source Selection ===")
    print("1. Etherscan API (slower, rate limited)")
    print("2. Local Ethereum Archive Node (faster, no limits)")
    
    while True:
        choice = input("\nSelect data source (1 or 2): ").strip()
        if choice == "1":
            return "etherscan"
        elif choice == "2":
            return "node"
        else:
            print("Invalid choice. Please enter 1 or 2.")

def main():
    print("=== Ethereum Token Price Extractor & Big Buy Analyzer ===")
    print("This tool will extract price data and analyze big buys for a token")
    print("Supports both Uniswap V2 and V3 pools")
    
    # Get data source choice
    data_source = get_data_source_choice()
    
    if data_source == "node":
        # Check if node is properly configured
        if not USE_LOCAL_NODE:
            print("❌ Archive node is not properly configured.")
            print("Please check your .env file for NODE_RPC_URL and NODE_API_KEY")
            return
        
        print("✅ Using Archive Node for data extraction")
        print("📈 ETH prices will be fetched live from Chainlink oracle")
        use_node = True
    else:
        print("✅ Using Etherscan API for data extraction")
        print("📊 ETH prices will be read from CSV file")
        use_node = False
    
    # Get user inputs
    print("\n" + "="*60)
    token_address = input("Enter the token address (0x...): ").strip()
    pool_address = input("Enter the Uniswap pool address (0x...): ").strip()
    
    # Get Uniswap version
    version_input = input("Enter Uniswap version (v2/v3) or press Enter for auto-detect: ").strip().lower()
    if version_input in ['v2', 'v3']:
        version = version_input
        print(f"Using Uniswap {version.upper()}")
    else:
        print("Auto-detecting Uniswap version...")
        version = "auto"
    
    # Get number of blocks to analyze
    default_blocks = 1000
    blocks_input = input(f"Enter number of blocks to analyze (default {default_blocks}): ").strip()
    
    if blocks_input:
        try:
            num_blocks = int(blocks_input)
        except ValueError:
            print(f"Invalid input, using default: {default_blocks}")
            num_blocks = default_blocks
    else:
        num_blocks = default_blocks
    
    # Calculate time estimate
    hours = (num_blocks * 12) / 3600  # 12 seconds per block
    if hours < 1:
        minutes = hours * 60
        time_estimate = f"~{minutes:.0f} minutes"
    elif hours < 24:
        time_estimate = f"~{hours:.1f} hours"
    else:
        days = hours / 24
        time_estimate = f"~{days:.1f} days"
    
    print(f"Time estimate: {time_estimate} of blockchain data")
    
    proceed = input("Proceed with analysis? (y/n): ").strip().lower()
    if proceed != 'y':
        print("Analysis cancelled.")
        return
    
    try:
        print(f"\n=== Extracting data for token {token_address} ===")
        print(f"Pool: {pool_address}")
        print(f"Blocks to analyze: {num_blocks}")
        print(f"Using Uniswap {version.upper() if version != 'auto' else 'Auto-detect'}")
        print(f"Data source: {'Archive Node' if use_node else 'Etherscan API'}")
        
        # Initialize Web3Client if using node
        web3_client = None
        if use_node:
            try:
                print("🔗 Connecting to Archive Node...")
                web3_client = Web3Client()
                print(f"✅ Connected to Archive Node")
            except Exception as e:
                print(f"❌ Failed to connect to Archive Node: {e}")
                print("Falling back to Etherscan API...")
                use_node = False
        
        # Create extractor
        factory = UniswapExtractorFactory()
        
        if use_node and web3_client:
            # Use archive node
            extractor = factory.create_extractor_with_node(version, web3_client)
        else:
            # Use Etherscan API - need API key
            api_key = os.getenv("ETHERSCAN_API_KEY")
            if not api_key:
                print("❌ ETHERSCAN_API_KEY not found in environment variables")
                return
            extractor = factory.create_extractor(version, api_key)
        
        # Get latest block and calculate range
        latest_block = extractor.get_latest_block()
        start_block = latest_block - num_blocks
        end_block = latest_block
        
        print(f"\nAnalyzing blocks {start_block} to {end_block}")
        print("Extracting swap events and analyzing big buys...")
        
        # Run the complete analysis
        result = extractor.analyze_token_complete(
            token_address=token_address,
            pool_address=pool_address,
            start_block=start_block,
            end_block=end_block,
            threshold_eth=0.1  # Big buys >= 0.1 ETH
        )
        
        if not result or 'error' in result:
            print(f"❌ Analysis failed. Check the error messages above.")
            if result and 'error' in result:
                print(f"Error: {result['error']}")
            return
        
        # Display results
        prices = result.get('prices', [])
        big_buy_analysis = result.get('big_buy_analysis', {})
        
        print(f"\n✅ Analysis completed successfully!")
        print(f"📊 Price points extracted: {len(prices)}")
        
        if isinstance(big_buy_analysis, dict):
            big_buys = big_buy_analysis.get('big_buys', [])
            total_eth_volume = big_buy_analysis.get('total_eth_amount', 0)
            
            print(f"🔥 Big buys found: {len(big_buys)}")
            print(f"💎 Total big buy volume: {total_eth_volume:.6f} ETH")
            
            if big_buys:
                print("\n📈 Top big buys:")
                for i, buy in enumerate(big_buys[:5]):
                    eth_amount = buy.get('ethAmount', 0)
                    usd_value = buy.get('usdValue', 0)
                    block = buy.get('blockNumber', 'N/A')
                    print(f"  {i+1}. Block {block}: {eth_amount:.6f} ETH (${usd_value:.2f})")
        
        # Save to CSV
        print(f"\n💾 Saving results to data/token_analysis.csv...")
        
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Use ObjectCSVWriter to save in the specified format
        csv_writer = ObjectCSVWriter('data/token_analysis.csv')
        
        # Prepare data for CSV writer
        analysis_data = {
            'token_address': token_address,
            'token_name': token_address,  # Could be enhanced to get actual name
            'pool_address': pool_address,
            'uniswap_version': extractor.get_version(),
            'start_block': start_block,
            'end_block': end_block,
            'prices': prices,
            'big_buys': big_buy_analysis.get('big_buys', []) if isinstance(big_buy_analysis, dict) else [],
            'price_stats': result.get('price_stats', {}),
            'data_source': 'archive_node' if use_node else 'etherscan_api'
        }
        
        csv_writer.save_prices_to_object_csv([analysis_data], append_mode=True)
        
        print("✅ Results saved successfully!")
        print(f"📁 Check the file: data/token_analysis.csv")
        
    except KeyboardInterrupt:
        print("\n⚠️ Analysis interrupted by user")
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
