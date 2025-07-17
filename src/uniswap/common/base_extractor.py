"""
Base Extractor Interface for Uniswap

This module defines the abstract base class that both Uniswap V2 and V3 extractors
must implement. This ensures consistent interface across different Uniswap versions.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import logging


class BaseUniswapExtractor(ABC):
    """
    Abstract base class for Uniswap price extractors.
    
    This class defines the interface that all Uniswap extractors (V2, V3) must implement.
    It ensures consistent behavior and method signatures across different versions.
    """
    
    def __init__(self, etherscan_api_key: str, eth_price_file: str = "historical_price_eth/eth_historical_prices_complete.csv"):
        """
        Initialize the base extractor.
        
        Args:
            etherscan_api_key: API key for Etherscan
            eth_price_file: Path to ETH historical prices file
        """
        self.etherscan_api_key = etherscan_api_key
        self.eth_price_file = eth_price_file
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def get_pool_info(self, pool_address: str) -> Dict:
        """
        Get pool information (tokens, decimals, etc.).
        
        Args:
            pool_address: Address of the Uniswap pool
            
        Returns:
            Dictionary with pool information
        """
        pass
    
    @abstractmethod
    def decode_swap_event(self, event_data: str, event_topics: List[str]) -> Optional[Dict]:
        """
        Decode a swap event from the blockchain.
        
        Args:
            event_data: Raw event data
            event_topics: Event topics
            
        Returns:
            Decoded swap event data or None if decoding fails
        """
        pass
    
    @abstractmethod
    def calculate_token_price(self, decoded_event: Dict, pool_info: Dict, token_address: str) -> Optional[float]:
        """
        Calculate token price from decoded swap event.
        
        Args:
            decoded_event: Decoded swap event data
            pool_info: Pool information
            token_address: Address of the token to calculate price for
            
        Returns:
            Token price in ETH or None if calculation fails
        """
        pass
    
    @abstractmethod
    def extract_prices(self, token_address: str, pool_address: str, start_block: int, end_block: int) -> List[Dict]:
        """
        Extract price data from swap events.
        
        Args:
            token_address: Token address to analyze
            pool_address: Pool address
            start_block: Starting block number
            end_block: Ending block number
            
        Returns:
            List of price data points
        """
        pass
    
    @abstractmethod
    def get_swap_events(self, pool_address: str, start_block: int, end_block: int) -> List[Dict]:
        """
        Get swap events from the blockchain.
        
        Args:
            pool_address: Pool address
            start_block: Starting block number
            end_block: Ending block number
            
        Returns:
            List of raw swap events
        """
        pass
    
    @property
    @abstractmethod
    def w3(self):
        """
        Web3 instance for blockchain interactions.
        
        Returns:
            Web3 instance
        """
        pass
    
    def get_latest_block(self) -> int:
        """
        Get the latest block number.
        
        Returns:
            Latest block number
        """
        return self.w3.eth.block_number
    
    def analyze_token_complete(self, token_address: str, pool_address: str, 
                             start_block: int, end_block: int, 
                             threshold_eth: float = 0.1) -> Dict:
        """
        Complete token analysis including prices and big buy analysis.
        
        This is a concrete method that uses the abstract methods above.
        
        Args:
            token_address: Token address to analyze
            pool_address: Pool address
            start_block: Starting block number
            end_block: Ending block number
            threshold_eth: Minimum ETH amount for big buy analysis
            
        Returns:
            Dictionary with complete analysis
        """
        # Extract prices using the abstract method
        prices = self.extract_prices(token_address, pool_address, start_block, end_block)
        
        if not prices:
            return {
                'prices': [],
                'price_stats': {},
                'big_buy_analysis': {},
                'error': 'No prices found'
            }
        
        # Calculate price statistics
        price_stats = self._calculate_price_stats(prices)
        
        # Analyze big buys (this would need to be implemented or imported)
        big_buy_analysis = self._analyze_big_buys(prices, threshold_eth)
        
        return {
            'prices': prices,
            'price_stats': price_stats,
            'big_buy_analysis': big_buy_analysis,
            'error': None
        }
    
    def _calculate_price_stats(self, prices: List[Dict]) -> Dict:
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
    
    def _analyze_big_buys(self, prices: List[Dict], threshold_eth: float = 0.1) -> Dict:
        """
        Analyze big buys from price data.
        
        Args:
            prices: List of price data points
            threshold_eth: Minimum ETH amount for big buy analysis
            
        Returns:
            Dictionary with big buy analysis
        """
        # This is a placeholder - would need to implement or import from existing big buy analyzer
        # For now, return empty analysis
        return {
            'total_big_buys': 0,
            'threshold_eth': threshold_eth,
            'big_buys': []
        } 