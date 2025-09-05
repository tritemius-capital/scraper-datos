#!/usr/bin/env python3
"""
Ethereum Token Price Extractor & Big Buy Analyzer

This tool extracts historical price data for ERC-20 tokens from Uniswap pools
and analyzes big buy patterns. Supports both Uniswap V2 and V3.

Usage:
  python3 main.py                    # Interactive CLI mode
  python3 main.py tokens.csv         # Batch process tokens from CSV
"""

import sys
import os
import pandas as pd
from src.uniswap import UniswapExtractorFactory
from src.pricing.object_csv_writer import ObjectCSVWriter
from src.pricing.swap_jsonl_writer import SwapJSONLWriter
from src.pricing.enhanced_csv_writer import EnhancedCSVWriter
from src.pricing.pools_csv_writer import PoolsCSVWriter
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

def process_single_token_interactive(data_source: str):
    """Process a single token interactively"""
    use_node = (data_source == "node")
    
    if data_source == "node":
        # Check if node is properly configured
        if not USE_LOCAL_NODE:
            print("❌ Archive node is not properly configured.")
            print("Please check your .env file for NODE_RPC_URL and NODE_API_KEY")
            return
        
        print("✅ Using Archive Node for data extraction")
        print("📈 ETH prices will be fetched live from Chainlink oracle")
    else:
        print("✅ Using Etherscan API for data extraction")
        print("📊 ETH prices will be read from CSV file")
    
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
    
    # Process the token
    process_token(token_address, pool_address, version, num_blocks, data_source, 1, 1)

def process_tokens_from_csv(csv_file: str, data_source: str, num_blocks: int, save_swaps: bool = False):
    """Process all tokens from CSV file"""
    
    print(f"\n=== Processing tokens from {csv_file} ===")
    
    # Read CSV file
    try:
        df = pd.read_csv(csv_file)
        print(f"📊 Found {len(df)} tokens to process")
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return
    
    # Create output directory
    os.makedirs('data', exist_ok=True)
    
    # Process each token
    results = []
    
    for index, row in df.iterrows():
        try:
            version = row['version'].lower()
            token_address = row['nombre']  # Token address
            pool_address = row['par']      # Pool address
            
            print(f"\n{'='*60}")
            print(f"Processing token {index + 1}/{len(df)}")
            
            result = process_token(token_address, pool_address, version, num_blocks, data_source, index + 1, len(df))
            if result:
                results.append(result)
                
        except Exception as e:
            print(f"❌ Error processing token {index + 1}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Save all results using enhanced CSV writer
    if results:
        output_file = 'data/pr0y3kto_kp0p_XyZ.csv'
        print(f"\n💾 Saving {len(results)} results to {output_file}")
        
        # Use enhanced CSV writer for clean aggregated data
        enhanced_writer = EnhancedCSVWriter()
        enhanced_writer.save_enhanced_analysis_csv(results, output_file)
        
        print(f"✅ Results saved successfully!")
        print(f"📁 Check the file: {output_file}")
        
        # Save pools metadata
        pools_file = 'data/pools.csv'
        print(f"\n💾 Saving pool metadata to {pools_file}")
        pools_writer = PoolsCSVWriter()
        pools_writer.save_pools_csv(results, pools_file)
        print(f"📄 Pool metadata saved to {pools_file}")
        
        # Save individual swaps if requested
        if save_swaps and results:
            print(f"\n💾 Saving individual swaps to JSONL...")
            jsonl_writer = SwapJSONLWriter()
            
            for result in results:
                try:
                    # Get all swap events for this token
                    swap_events = result.get('prices', [])  # prices contains individual swaps
                    
                    if swap_events:
                        # Create filename for this token's swaps
                        token_short = result['token_address'][:8]
                        version = result['uniswap_version'].lower()
                        swaps_file = f"data/swaps_{token_short}_{version}.jsonl"
                        
                        success = jsonl_writer.write_swaps_to_jsonl(
                            swaps=swap_events,
                            output_file=swaps_file,
                            pool_address=result['pool_address'],
                            version=result['uniswap_version'],
                            compress=True,
                            pool_info=result.get('pool_info', {})
                        )
                        
                        if success:
                            print(f"📄 {len(swap_events)} swaps saved to {swaps_file}.gz")
                        else:
                            print(f"❌ Failed to save swaps for {token_short}")
                    
                except Exception as e:
                    print(f"❌ Error saving swaps: {e}")
                    continue
    else:
        print("❌ No results to save")

def process_token(token_address: str, pool_address: str, version: str, num_blocks: int, 
                 data_source: str, current_index: int = 1, total_count: int = 1):
    """Process a single token and return the analysis data"""
    
    use_node = (data_source == "node")
    web3_client = None
    
    print(f"🪙 Token: {token_address}")
    print(f"🏊 Pool: {pool_address}")
    print(f"📦 Version: {version.upper()}")
    
    try:
        # Initialize Web3Client if using node
        if use_node:
            try:
                if current_index == 1:  # Only show connection message for first token
                    print("🔗 Connecting to Archive Node...")
                web3_client = Web3Client()
                if current_index == 1:
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
                return None
            extractor = factory.create_extractor(version, api_key)
        
        # Get latest block and calculate range
        latest_block = extractor.get_latest_block()
        start_block = latest_block - num_blocks
        end_block = latest_block
        
        print(f"📦 Analyzing blocks {start_block} to {end_block}")
        
        # Run the complete analysis
        result = extractor.analyze_token_complete(
            token_address=token_address,
            pool_address=pool_address,
            start_block=start_block,
            end_block=end_block,
            threshold_eth=0.1  # Big buys >= 0.1 ETH
        )
        
        if not result or result.get('error'):
            print(f"❌ Analysis failed for token {token_address}")
            if result and result.get('error'):
                print(f"Error: {result['error']}")
            return None
        
        # Display results
        prices = result.get('prices', [])
        big_buy_analysis = result.get('big_buy_analysis', {})
        
        print(f"✅ Analysis completed!")
        print(f"📊 Price points extracted: {len(prices)}")
        
        if isinstance(big_buy_analysis, dict):
            big_buys = big_buy_analysis.get('big_buys', [])
            total_eth_volume = big_buy_analysis.get('total_eth_amount', 0)
            
            print(f"🔥 Big buys found: {len(big_buys)}")
            print(f"💎 Total big buy volume: {total_eth_volume:.6f} ETH")
            
            if big_buys:
                print("📈 Top big buys:")
                for i, buy in enumerate(big_buys[:3]):  # Show top 3
                    eth_amount = buy.get('ethAmount', 0)
                    usd_value = buy.get('usdValue', 0)
                    block = buy.get('blockNumber', 'N/A')
                    print(f"  {i+1}. Block {block}: {eth_amount:.6f} ETH (${usd_value:.2f})")
        
        # For single token mode, save immediately
        if total_count == 1:
            print(f"\n💾 Saving results to data/token_analysis.csv...")
            os.makedirs('data', exist_ok=True)
            csv_writer = ObjectCSVWriter()
            
            analysis_data = {
                'token_address': token_address,
                'token_name': token_address,
                'pool_address': pool_address,
                'uniswap_version': version.upper(),
                'start_block': start_block,
                'end_block': end_block,
                'prices': prices,
                'big_buys': big_buy_analysis.get('big_buys', []) if isinstance(big_buy_analysis, dict) else [],
                'price_stats': result.get('price_stats', {}),
                'data_source': 'archive_node' if use_node else 'etherscan_api'
            }
            
            csv_writer.save_prices_to_object_csv(
                prices=analysis_data['prices'],
                output_file='data/token_analysis.csv',
                token_address=analysis_data['token_address'],
                pool_address=analysis_data['pool_address'],
                uniswap_version=analysis_data['uniswap_version'],
                stats=analysis_data.get('price_stats', {}),
                big_buy_analysis={'big_buys': analysis_data.get('big_buys', [])},
                append=True
            )
            print("✅ Results saved successfully!")
            print(f"📁 Check the file: data/token_analysis.csv")
        
        # Return data for batch processing
        return {
            'token_address': token_address,
            'token_name': token_address,
            'pool_address': pool_address,
            'uniswap_version': version.upper(),
            'start_block': start_block,
            'end_block': end_block,
            'prices': prices,
            'big_buys': big_buy_analysis.get('big_buys', []) if isinstance(big_buy_analysis, dict) else [],
            'price_stats': result.get('price_stats', {}),
            'data_source': 'archive_node' if use_node else 'etherscan_api'
        }
        
    except KeyboardInterrupt:
        print("\n⚠️ Analysis interrupted by user")
        return None
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    print("=== Ethereum Token Price Extractor & Big Buy Analyzer ===")
    
    # Check if CSV file is provided as argument
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        
        if not os.path.exists(csv_file):
            print(f"❌ CSV file not found: {csv_file}")
            return
        
        print(f"📁 Batch mode: Processing tokens from {csv_file}")
        print("Supports both Uniswap V2 and V3 pools")
        
        # Get data source choice
        data_source = get_data_source_choice()
        
        # Get number of blocks
        default_blocks = 1000
        blocks_input = input(f"\nEnter number of blocks to analyze per token (default {default_blocks}): ").strip()
        
        # Save individual swaps by default (no user prompt)
        save_swaps = True
        
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
        elif hours < 24:
            time_estimate = f"~{hours:.1f} hours"
        else:
            days = hours / 24
            time_estimate = f"~{days:.1f} days"
        
        print(f"Time estimate per token: {time_estimate} of blockchain data")
        print(f"Data source: {'Archive Node' if data_source == 'node' else 'Etherscan API'}")
        
        proceed = input("Proceed with batch analysis? (y/n): ").strip().lower()
        if proceed != 'y':
            print("Analysis cancelled.")
            return
        
        # Process tokens from CSV
        process_tokens_from_csv(csv_file, data_source, num_blocks, save_swaps)
        
    else:
        # Interactive mode
        print("This tool will extract price data and analyze big buys for a token")
        print("Supports both Uniswap V2 and V3 pools")
        
        # Get data source choice
        data_source = get_data_source_choice()
        
        # Process single token interactively
        process_single_token_interactive(data_source)

if __name__ == "__main__":
    main()
