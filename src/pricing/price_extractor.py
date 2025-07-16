"""
Price Extractor for Uniswap V2 Pools

Main orchestrator that coordinates all modules to extract historical token prices.
"""

import logging
from typing import List, Dict, Optional
from tqdm import tqdm
from web3 import Web3
from src.client.etherscan_client import EtherscanClient
from src.pricing.event_decoder import EventDecoder
from src.pricing.price_calculator import PriceCalculator
from src.pricing.eth_price_reader import ETHPriceReader
from src.pricing.csv_writer import CSVWriter
from src.pricing.object_csv_writer import ObjectCSVWriter
from src.pricing.big_buy_analyzer import BigBuyAnalyzer

class PriceExtractor:
    """Main orchestrator for extracting token prices from Uniswap V2 pool swap events."""
    
    # Uniswap V2 Pair ABI (minimal for token0/token1/decimals)
    PAIR_ABI = [
        {"constant":True,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},
        {"constant":True,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"},
        {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"},
    ]
    ERC20_ABI = [
        {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}
    ]

    def __init__(self, etherscan_api_key: str, eth_price_file: str = "historical_price_eth/eth_historical_prices_complete.csv"):
        self.etherscan_client = EtherscanClient(etherscan_api_key)
        self.logger = logging.getLogger(__name__)
        self.event_decoder = EventDecoder()
        self.price_calculator = PriceCalculator()
        self.eth_price_reader = ETHPriceReader(eth_price_file)
        self.csv_writer = CSVWriter()
        self.object_csv_writer = ObjectCSVWriter()
        self.big_buy_analyzer = BigBuyAnalyzer(etherscan_api_key)
        self.w3 = Web3(Web3.HTTPProvider("https://eth.llamarpc.com"))

    def get_token_info(self, pool_address: str) -> Dict:
        contract = self.w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=self.PAIR_ABI)
        token0 = contract.functions.token0().call()
        token1 = contract.functions.token1().call()
        # Get decimals for both tokens
        token0_contract = self.w3.eth.contract(address=token0, abi=self.ERC20_ABI)
        token1_contract = self.w3.eth.contract(address=token1, abi=self.ERC20_ABI)
        decimals0 = token0_contract.functions.decimals().call()
        decimals1 = token1_contract.functions.decimals().call()
        return {"token0": token0, "token1": token1, "decimals0": decimals0, "decimals1": decimals1}

    def extract_prices(self, token_address: str, pool_address: str, start_block: int, end_block: int) -> List[Dict]:
        prices = []
        try:
            self.logger.info(f"Extracting prices for {token_address} from blocks {start_block} to {end_block}")
            # Get token0/token1 and decimals
            info = self.get_token_info(pool_address)
            token0 = info["token0"].lower()
            token1 = info["token1"].lower()
            decimals0 = info["decimals0"]
            decimals1 = info["decimals1"]
            token_address = token_address.lower()
            # Get swap events (all)
            swap_events = self.etherscan_client.get_swap_events(pool_address, start_block, end_block)
            self.logger.info(f"Found {len(swap_events)} swap events")
            for event in tqdm(swap_events, desc="Processing swap events", unit="event"):
                try:
                    decoded_event = self.event_decoder.decode_swap_event(event['data'], event['topics'])
                    if not decoded_event:
                        continue
                    # Get block/timestamp
                    block_number = int(event['blockNumber'], 16)
                    timestamp = int(event['timeStamp'], 16)
                    eth_price_usd = self.eth_price_reader.get_eth_price_at_timestamp(timestamp)
                    # Get raw amounts
                    a0in = decoded_event['amount0In'] / (10 ** decimals0)
                    a1in = decoded_event['amount1In'] / (10 ** decimals1)
                    a0out = decoded_event['amount0Out'] / (10 ** decimals0)
                    a1out = decoded_event['amount1Out'] / (10 ** decimals1)
                    # To determine if the token is token0 or token1, we need to check if the token address is the same as the token0 or token1 address
                    if token_address == token0:
                        # If the token address is the same as the token0 address, then the token is token0
                        if a0in > 0 and a1out > 0:
                            price_eth = a1out / a0in
                        elif a1in > 0 and a0out > 0:
                            price_eth = a1in / a0out
                        else:
                            continue
                    elif token_address == token1:
                        # If the token address is the same as the token1 address, then the token is token1
                        if a1in > 0 and a0out > 0:
                            price_eth = a0out / a1in
                        elif a0in > 0 and a1out > 0:
                            price_eth = a0in / a1out
                        else:
                            continue
                    else:
                        continue
                    price_usd = price_eth * eth_price_usd if eth_price_usd else None
                    price_data = {
                        'timestamp': timestamp,
                        'block_number': block_number,
                        'token_price_eth': price_eth,
                        'token_price_usd': price_usd,
                        'eth_price_usd': eth_price_usd
                    }
                    prices.append(price_data)
                except Exception as e:
                    self.logger.warning(f"Error processing event: {e}")
                    continue
            self.logger.info(f"Successfully extracted {len(prices)} price points")
            return prices
        except Exception as e:
            self.logger.error(f"Error extracting prices: {e}")
            return prices
    
    def save_prices_to_csv(self, prices: List[Dict], output_file: str):
        """
        Save price data to CSV file.
        
        Args:
            prices: List of price data points
            output_file: Output CSV file path
        """
        return self.csv_writer.save_prices_to_csv(prices, output_file)
    
    def calculate_price_stats(self, prices: List[Dict]) -> Dict:
        """
        Calculate price statistics from extracted prices.
        
        Args:
            prices: List of price data points
            
        Returns:
            Dictionary with price statistics
        """
        if not prices:
            return {}
        
        # Sort by timestamp to get chronological order
        sorted_prices = sorted(prices, key=lambda x: x['timestamp'])
        
        # Get current price (latest)
        current_price = sorted_prices[-1]
        
        # Find lowest and highest prices in USD
        lowest_usd = min(prices, key=lambda x: x['token_price_usd'] if x['token_price_usd'] else float('inf'))
        highest_usd = max(prices, key=lambda x: x['token_price_usd'] if x['token_price_usd'] else 0)
        
        # Calculate price change from lowest to current
        if lowest_usd['token_price_usd'] and current_price['token_price_usd']:
            price_change_low = ((current_price['token_price_usd'] - lowest_usd['token_price_usd']) / lowest_usd['token_price_usd']) * 100
        else:
            price_change_low = 0
        
        # Calculate price change from highest to current
        if highest_usd['token_price_usd'] and current_price['token_price_usd']:
            price_change_high = ((current_price['token_price_usd'] - highest_usd['token_price_usd']) / highest_usd['token_price_usd']) * 100
        else:
            price_change_high = 0
        
        stats = {
            'lowest_price_usd': lowest_usd['token_price_usd'],
            'lowest_price_timestamp': lowest_usd['timestamp'],
            'lowest_price_block': lowest_usd['block_number'],
            'current_price_usd': current_price['token_price_usd'],
            'current_price_timestamp': current_price['timestamp'],
            'current_price_block': current_price['block_number'],
            'highest_price_usd': highest_usd['token_price_usd'],
            'highest_price_timestamp': highest_usd['timestamp'],
            'highest_price_block': highest_usd['block_number'],
            'price_change_from_low': price_change_low,
            'price_change_from_high': price_change_high,
            'total_swaps': len(prices)
        }
        
        return stats
    
    def analyze_token_complete(self, token_address: str, pool_address: str, 
                             start_block: int, end_block: int, 
                             threshold_eth: float = 0.1) -> Dict:
        """
        Complete token analysis including prices and big buy analysis.
        
        Args:
            token_address: Token address to analyze
            pool_address: Pool address
            start_block: Starting block number
            end_block: Ending block number
            threshold_eth: Minimum ETH amount for big buy analysis
            
        Returns:
            Dictionary with complete analysis
        """
        # Extract prices
        prices = self.extract_prices(token_address, pool_address, start_block, end_block)
        
        if not prices:
            return {
                'prices': [],
                'price_stats': {},
                'big_buy_analysis': {},
                'error': 'No prices found'
            }
        
        # Calculate price statistics
        price_stats = self.calculate_price_stats(prices)
        
        # Analyze big buys
        big_buy_analysis = self.big_buy_analyzer.get_big_buy_analysis_from_prices(
            prices, threshold_eth
        )
        
        return {
            'prices': prices,
            'price_stats': price_stats,
            'big_buy_analysis': big_buy_analysis,
            'error': None
        }
    
    def save_prices_to_object_csv(self, prices: List[Dict], output_file: str, token_address: str, pool_address: str = ""):
        """
        Save price data to CSV file with objects in single cells.
        
        Args:
            prices: List of price data points
            output_file: Output CSV file path
            token_address: Token address
            pool_address: Pool address
        """
        # Calculate price statistics
        stats = self.calculate_price_stats(prices)
        return self.object_csv_writer.save_prices_to_object_csv(prices, output_file, token_address, pool_address, stats) 