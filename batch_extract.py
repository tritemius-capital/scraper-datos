#!/usr/bin/env python3
"""
Batch Token Extractor

Processes multiple tokens from a CSV file and extracts price data and big buys.
"""

import sys
import os
import csv
import pandas as pd
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

def process_tokens_from_csv(csv_file: str, data_source: str, num_blocks: int = 1000):
    """Process all tokens from CSV file"""
    
    print(f"\n=== Processing tokens from {csv_file} ===")
    
    # Initialize data source
    use_node = (data_source == "node")
    web3_client = None
    
    if use_node:
        if not USE_LOCAL_NODE:
            print("❌ Archive node is not properly configured.")
            return
        
        try:
            print("🔗 Connecting to Archive Node...")
            web3_client = Web3Client()
            print(f"✅ Connected to Archive Node")
        except Exception as e:
            print(f"❌ Failed to connect to Archive Node: {e}")
            print("Falling back to Etherscan API...")
            use_node = False
    
    # Read CSV file
    try:
        df = pd.read_csv(csv_file)
        print(f"📊 Found {len(df)} tokens to process")
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return
    
    # Create output directory
    os.makedirs('data', exist_ok=True)
    
    # Initialize CSV writer
    output_file = 'data/batch_token_analysis.csv'
    csv_writer = ObjectCSVWriter()
    
    # Process each token
    factory = UniswapExtractorFactory()
    results = []
    
    for index, row in df.iterrows():
        try:
            version = row['version'].lower()
            token_address = row['nombre']  # Token address
            pool_address = row['par']      # Pool address
            
            print(f"\n{'='*60}")
            print(f"Processing token {index + 1}/{len(df)}")
            print(f"🪙 Token: {token_address}")
            print(f"🏊 Pool: {pool_address}")
            print(f"📦 Version: {version.upper()}")
            
            # Create extractor
            if use_node and web3_client:
                extractor = factory.create_extractor_with_node(version, web3_client)
            else:
                api_key = os.getenv("ETHERSCAN_API_KEY")
                if not api_key:
                    print("❌ ETHERSCAN_API_KEY not found")
                    continue
                extractor = factory.create_extractor(version, api_key)
            
            # Get latest block and calculate range
            latest_block = extractor.get_latest_block()
            start_block = latest_block - num_blocks
            end_block = latest_block
            
            print(f"📦 Analyzing blocks {start_block} to {end_block}")
            
            # Run analysis
            result = extractor.analyze_token_complete(
                token_address=token_address,
                pool_address=pool_address,
                start_block=start_block,
                end_block=end_block,
                threshold_eth=0.1  # Big buys >= 0.1 ETH
            )
            
            if not result or 'error' in result:
                print(f"❌ Analysis failed for token {token_address}")
                if result and 'error' in result:
                    print(f"Error: {result['error']}")
                continue
            
            # Display results
            prices = result.get('prices', [])
            big_buy_analysis = result.get('big_buy_analysis', {})
            
            print(f"✅ Analysis completed!")
            print(f"📊 Price points: {len(prices)}")
            
            if isinstance(big_buy_analysis, dict):
                big_buys = big_buy_analysis.get('big_buys', [])
                total_eth_volume = big_buy_analysis.get('total_eth_amount', 0)
                
                print(f"🔥 Big buys: {len(big_buys)}")
                print(f"💎 Total volume: {total_eth_volume:.6f} ETH")
                
                if big_buys:
                    print("📈 Top big buys:")
                    for i, buy in enumerate(big_buys[:3]):  # Show top 3
                        eth_amount = buy.get('ethAmount', 0)
                        usd_value = buy.get('usdValue', 0)
                        block = buy.get('blockNumber', 'N/A')
                        print(f"  {i+1}. Block {block}: {eth_amount:.6f} ETH (${usd_value:.2f})")
            
            # Prepare data for CSV
            analysis_data = {
                'token_address': token_address,
                'token_name': token_address,
                'pool_address': pool_address,
                'uniswap_version': version,
                'start_block': start_block,
                'end_block': end_block,
                'prices': prices,
                'big_buys': big_buy_analysis.get('big_buys', []) if isinstance(big_buy_analysis, dict) else [],
                'price_stats': result.get('price_stats', {}),
                'data_source': 'archive_node' if use_node else 'etherscan_api'
            }
            
            results.append(analysis_data)
            
        except Exception as e:
            print(f"❌ Error processing token {index + 1}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Save all results
    if results:
        print(f"\n💾 Saving {len(results)} results to {output_file}")
        csv_writer.save_prices_to_object_csv(results, append_mode=False)  # Overwrite file
        print(f"✅ Results saved successfully!")
        print(f"📁 Check the file: {output_file}")
    else:
        print("❌ No results to save")

def main():
    print("=== Batch Token Price Extractor & Big Buy Analyzer ===")
    print("This tool processes multiple tokens from a CSV file")
    
    # Get CSV file
    csv_file = "tokens_ejemplo_real.csv"
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        return
    
    # Get data source choice
    data_source = get_data_source_choice()
    
    if data_source == "node":
        print("✅ Using Archive Node for data extraction")
        print("📈 ETH prices will be fetched live from Chainlink oracle")
    else:
        print("✅ Using Etherscan API for data extraction")
        print("📊 ETH prices will be read from CSV file")
    
    # Get number of blocks
    default_blocks = 1000
    blocks_input = input(f"\nEnter number of blocks to analyze per token (default {default_blocks}): ").strip()
    
    if blocks_input:
        try:
            num_blocks = int(blocks_input)
        except ValueError:
            print(f"Invalid input, using default: {default_blocks}")
            num_blocks = default_blocks
    else:
        num_blocks = default_blocks
    
    # Calculate time estimate per token
    hours = (num_blocks * 12) / 3600
    if hours < 1:
        minutes = hours * 60
        time_estimate = f"~{minutes:.0f} minutes"
    else:
        time_estimate = f"~{hours:.1f} hours"
    
    print(f"Time estimate per token: {time_estimate} of blockchain data")
    print(f"Data source: {'Archive Node' if data_source == 'node' else 'Etherscan API'}")
    
    proceed = input("Proceed with batch analysis? (y/n): ").strip().lower()
    if proceed != 'y':
        print("Analysis cancelled.")
        return
    
    # Process tokens
    process_tokens_from_csv(csv_file, data_source, num_blocks)

if __name__ == "__main__":
    main() 