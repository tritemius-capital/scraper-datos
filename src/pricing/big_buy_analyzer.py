"""
Big Buy Analyzer for Uniswap V2 Price Data

Analyzes big buy transactions and their price impact over 5 days.
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from src.client.etherscan_client import EtherscanClient

class BigBuyAnalyzer:
    """Analyzes big buy transactions and their price impact."""
    
    def __init__(self, etherscan_api_key: str):
        self.etherscan_client = EtherscanClient(etherscan_api_key)
        self.logger = logging.getLogger(__name__)
    
    def find_big_buys_in_history(self, token_address: str, pool_address: str, 
                                start_block: int, end_block: int, 
                                threshold_eth: float = 0.1) -> List[Dict]:
        """
        Find all big buy transactions in the historical data.
        
        Args:
            token_address: Token address to analyze
            pool_address: Pool address
            start_block: Starting block number
            end_block: Ending block number
            threshold_eth: Minimum ETH amount to consider a big buy
            
        Returns:
            List of big buy transactions with details
        """
        big_buys = []
        
        try:
            self.logger.info(f"Searching for big buys >= {threshold_eth} ETH in blocks {start_block} to {end_block}")
            
            # Get all swap events from the pool
            swap_events = self.etherscan_client.get_swap_events(pool_address, start_block, end_block)
            
            if not swap_events:
                self.logger.info("No swap events found")
                return big_buys
            
            self.logger.info(f"Found {len(swap_events)} swap events to analyze")
            
            # Get pool info to understand token0/token1
            from web3 import Web3
            w3 = Web3(Web3.HTTPProvider("https://eth.llamarpc.com"))
            
            # Minimal ABI for token0/token1/decimals
            PAIR_ABI = [
                {"constant":True,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},
                {"constant":True,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"},
            ]
            ERC20_ABI = [
                {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}
            ]
            
            try:
                # Get token0/token1 addresses
                contract = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=PAIR_ABI)
                token0 = contract.functions.token0().call().lower()
                token1 = contract.functions.token1().call().lower()
                
                self.logger.info(f"Pool tokens: token0={token0}, token1={token1}")
                
                # Get decimals for both tokens
                token0_contract = w3.eth.contract(address=Web3.to_checksum_address(token0), abi=ERC20_ABI)
                token1_contract = w3.eth.contract(address=Web3.to_checksum_address(token1), abi=ERC20_ABI)
                decimals0 = token0_contract.functions.decimals().call()
                decimals1 = token1_contract.functions.decimals().call()
                
                self.logger.info(f"Token decimals: token0={decimals0}, token1={decimals1}")
                
            except Exception as e:
                self.logger.error(f"Error getting pool info: {e}")
                return big_buys
            
            # Determine which token is ETH (WETH) and which is our target token
            # WETH address: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
            weth_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
            
            eth_token = None
            target_token = None
            eth_decimals = None
            target_decimals = None
            
            if token0 == weth_address:
                eth_token = token0
                target_token = token1
                eth_decimals = decimals0
                target_decimals = decimals1
            elif token1 == weth_address:
                eth_token = token1
                target_token = token0
                eth_decimals = decimals1
                target_decimals = decimals0
            else:
                self.logger.warning("No WETH found in pool, cannot analyze big buys")
                return big_buys
            
            # Verify target token matches
            if target_token != token_address.lower():
                self.logger.warning(f"Target token {target_token} doesn't match {token_address}")
                return big_buys
            
            # Decode swap events to find big buys
            from src.pricing.event_decoder import EventDecoder
            decoder = EventDecoder()
            
            for event in swap_events:
                try:
                    # Decode the swap event
                    decoded_event = decoder.decode_swap_event(event['data'], event['topics'])
                    if not decoded_event:
                        continue
                    
                    # Get amounts in human readable format
                    amount0_in = decoded_event['amount0In'] / (10 ** decimals0)
                    amount1_in = decoded_event['amount1In'] / (10 ** decimals1)
                    amount0_out = decoded_event['amount0Out'] / (10 ** decimals0)
                    amount1_out = decoded_event['amount1Out'] / (10 ** decimals1)
                    
                    # Determine if this is a buy (someone buying tokens with ETH)
                    eth_amount = 0
                    if token0 == weth_address:
                        # ETH is token0
                        if amount0_in > 0 and amount1_out > 0:
                            # Someone is buying tokens with ETH
                            eth_amount = amount0_in
                    else:
                        # ETH is token1
                        if amount1_in > 0 and amount0_out > 0:
                            # Someone is buying tokens with ETH
                            eth_amount = amount1_in
                    
                    # Check if this is a big buy
                    if eth_amount >= threshold_eth:
                        big_buy = {
                            'tx_hash': event.get('transactionHash', ''),
                            'block_number': int(event['blockNumber'], 16),
                            'timestamp': int(event['timeStamp'], 16),
                            'eth_amount': eth_amount,
                            'token_amount_in': amount0_in if token0 == weth_address else amount1_in,
                            'token_amount_out': amount1_out if token0 == weth_address else amount0_out,
                            'from_address': decoded_event.get('sender', ''),
                            'to_address': decoded_event.get('to', '')
                        }
                        big_buys.append(big_buy)
                        
                except Exception as e:
                    self.logger.warning(f"Error decoding swap event: {e}")
                    continue
            
            self.logger.info(f"Found {len(big_buys)} big buys >= {threshold_eth} ETH")
            return big_buys
            
        except Exception as e:
            self.logger.error(f"Error finding big buys: {e}")
            return big_buys
    
    def find_big_buys_from_prices(self, prices: List[Dict], threshold_eth: float = 0.1) -> List[Dict]:
        """
        Find big buys by analyzing price movements in existing price data.
        This is a more efficient approach that uses already extracted data.
        
        Args:
            prices: List of price data points (from PriceExtractor)
            threshold_eth: Minimum ETH amount to consider a big buy
            
        Returns:
            List of potential big buy events
        """
        big_buys = []
        
        if not prices:
            return big_buys
        
        self.logger.info(f"Analyzing {len(prices)} price points for big buys >= {threshold_eth} ETH")
        
        # Sort prices by timestamp to get chronological order
        sorted_prices = sorted(prices, key=lambda x: x['timestamp'])
        
        # Look for significant price movements that could indicate big buys
        # We'll use price volatility and volume as indicators
        
        for i in range(1, len(sorted_prices)):
            current_price = sorted_prices[i]
            previous_price = sorted_prices[i-1]
            
            # Calculate price change percentage
            if previous_price['token_price_usd'] and current_price['token_price_usd']:
                price_change = ((current_price['token_price_usd'] - previous_price['token_price_usd']) / 
                               previous_price['token_price_usd']) * 100
                
                # Look for significant price increases (>10% in one block)
                if price_change > 10:
                    # This could be a big buy - estimate ETH amount based on price impact
                    # This is a rough estimation - in reality we'd need the actual swap data
                    estimated_eth = price_change / 100  # Rough estimate: 1% price change per 0.01 ETH
                    
                    if estimated_eth >= threshold_eth:
                        big_buy = {
                            'tx_hash': f"estimated_{current_price['block_number']}",  # Placeholder
                            'block_number': current_price['block_number'],
                            'timestamp': current_price['timestamp'],
                            'eth_amount': estimated_eth,
                            'price_before': previous_price['token_price_usd'],
                            'price_after': current_price['token_price_usd'],
                            'price_change_percent': price_change,
                            'estimated': True  # Flag to indicate this is an estimation
                        }
                        big_buys.append(big_buy)
        
        self.logger.info(f"Found {len(big_buys)} potential big buys from price analysis")
        return big_buys
    
    def analyze_price_impact_5days(self, prices: List[Dict], big_buy_block: int) -> Dict:
        """
        Analyze price impact 5 days after a big buy.
        
        Args:
            prices: List of price data points
            big_buy_block: Block number of the big buy
            
        Returns:
            Dictionary with price analysis
        """
        if not prices:
            return {}
        
        # Find the big buy in the price data
        big_buy_price = None
        big_buy_timestamp = None
        
        for price in prices:
            if price['block_number'] == big_buy_block:
                big_buy_price = price
                big_buy_timestamp = price['timestamp']
                break
        
        if not big_buy_price or big_buy_timestamp is None:
            return {}
        
        # Calculate 5 days in seconds (approximate)
        five_days_seconds = 5 * 24 * 3600
        
        # Find prices within 5 days after the big buy
        prices_after_big_buy = []
        for price in prices:
            if (price['timestamp'] > big_buy_timestamp and 
                price['timestamp'] <= big_buy_timestamp + five_days_seconds):
                prices_after_big_buy.append(price)
        
        if not prices_after_big_buy:
            return {
                'big_buy_block': big_buy_block,
                'big_buy_timestamp': big_buy_timestamp,
                'price_at_big_buy': big_buy_price['token_price_usd'],
                'max_price_5d': None,
                'min_price_5d': None,
                'price_change_5d': None
            }
        
        # Find max and min prices in 5 days
        max_price = max(prices_after_big_buy, key=lambda x: x['token_price_usd'])
        min_price = min(prices_after_big_buy, key=lambda x: x['token_price_usd'])
        
        # Calculate price changes
        price_change_max = ((max_price['token_price_usd'] - big_buy_price['token_price_usd']) / 
                           big_buy_price['token_price_usd']) * 100
        price_change_min = ((min_price['token_price_usd'] - big_buy_price['token_price_usd']) / 
                           big_buy_price['token_price_usd']) * 100
        
        return {
            'big_buy_block': big_buy_block,
            'big_buy_timestamp': big_buy_timestamp,
            'price_at_big_buy': big_buy_price['token_price_usd'],
            'max_price_5d': max_price['token_price_usd'],
            'min_price_5d': min_price['token_price_usd'],
            'max_price_block': max_price['block_number'],
            'min_price_block': min_price['block_number'],
            'price_change_max_5d': price_change_max,
            'price_change_min_5d': price_change_min
        }
    
    def get_big_buy_analysis(self, token_address: str, pool_address: str, 
                           prices: List[Dict], threshold_eth: float = 0.1) -> Dict:
        """
        Complete big buy analysis for a token.
        
        Args:
            token_address: Token address
            pool_address: Pool address
            prices: List of price data points
            threshold_eth: Minimum ETH amount for big buy
            
        Returns:
            Dictionary with complete big buy analysis
        """
        if not prices:
            return {}
        
        # Get block range from prices
        start_block = min(p['block_number'] for p in prices)
        end_block = max(p['block_number'] for p in prices)
        
        # Find big buys
        big_buys = self.find_big_buys_in_history(token_address, pool_address, 
                                                start_block, end_block, threshold_eth)
        
        # Analyze each big buy
        big_buy_analysis = []
        for big_buy in big_buys:
            analysis = self.analyze_price_impact_5days(prices, big_buy['block_number'])
            if analysis:
                analysis['tx_hash'] = big_buy.get('tx_hash', '')
                analysis['eth_amount'] = big_buy.get('eth_amount', 0)
                big_buy_analysis.append(analysis)
        
        return {
            'total_big_buys': len(big_buy_analysis),
            'threshold_eth': threshold_eth,
            'big_buys': big_buy_analysis
        } 
    
    def get_big_buy_analysis_from_prices(self, prices: List[Dict], threshold_eth: float = 0.1) -> Dict:
        """
        Complete big buy analysis using existing price data.
        
        Args:
            prices: List of price data points
            threshold_eth: Minimum ETH amount for big buy
            
        Returns:
            Dictionary with complete big buy analysis
        """
        if not prices:
            return {}
        
        # Find big buys from price data
        big_buys = self.find_big_buys_from_prices(prices, threshold_eth)
        
        # Analyze each big buy
        big_buy_analysis = []
        for big_buy in big_buys:
            analysis = self.analyze_price_impact_5days(prices, big_buy['block_number'])
            if analysis:
                analysis['tx_hash'] = big_buy.get('tx_hash', '')
                analysis['eth_amount'] = big_buy.get('eth_amount', 0)
                analysis['price_change_percent'] = big_buy.get('price_change_percent', 0)
                analysis['estimated'] = big_buy.get('estimated', False)
                big_buy_analysis.append(analysis)
        
        return {
            'total_big_buys': len(big_buy_analysis),
            'threshold_eth': threshold_eth,
            'big_buys': big_buy_analysis
        } 