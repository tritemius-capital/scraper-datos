"""
Base Extractor Interface for Uniswap

This module defines the abstract base class that both Uniswap V2 and V3 extractors
must implement. This ensures consistent interface across different Uniswap versions.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import logging
from web3 import Web3
import os


class BaseUniswapExtractor(ABC):
    """
    Abstract base class for Uniswap price extractors.
    
    This class defines the interface that all Uniswap extractors (V2, V3) must implement.
    It ensures consistent behavior and method signatures across different versions.
    """
    
    def __init__(self, api_key: str, eth_price_file: str = "historical_price_eth/eth_historical_prices_complete.csv", use_node: bool = False):
        """
        Initialize the base extractor.
        
        Args:
            api_key: API key for Etherscan or Archive Node
            eth_price_file: Path to ETH historical prices file
            use_node: If True, use Archive Node instead of Etherscan
        """
        self.api_key = api_key
        self.eth_price_file = eth_price_file
        self.use_node = use_node
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize Web3 based on configuration
        if use_node:
            node_rpc_url = os.getenv('NODE_RPC_URL')
            if not node_rpc_url:
                raise ValueError("NODE_RPC_URL environment variable not set")
            self._w3 = Web3(Web3.HTTPProvider(node_rpc_url))
            self.logger.info(f"Using Archive Node at {node_rpc_url}")
        else:
            self._w3 = Web3(Web3.HTTPProvider("https://eth.llamarpc.com"))
            self.logger.info("Using public RPC endpoint")
        
        # Initialize ETH price reader
        self.web3_client = None  # Will be set by factory if using node
        self._init_eth_price_reader()
    
    def _init_eth_price_reader(self):
        """Initialize ETH price reader based on configuration"""
        from src.pricing.eth_price_reader import ETHPriceReader
        
        if self.use_node and hasattr(self, 'web3_client') and self.web3_client:
            # Use node for ETH prices
            self.eth_price_reader = ETHPriceReader(
                csv_file_path=self.eth_price_file,
                use_node=True,
                web3_client=self.web3_client
            )
        else:
            # Use CSV file for ETH prices
            self.eth_price_reader = ETHPriceReader(
                csv_file_path=self.eth_price_file,
                use_node=False
            )
    
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
    def w3(self):
        """
        Web3 instance for blockchain interactions.
        
        Returns:
            Web3 instance
        """
        return self._w3
    
    def get_latest_block(self) -> int:
        """
        Get the latest block number.
        
        Returns:
            Latest block number
        """
        if self.w3 is None:
            raise ValueError("Web3 instance not initialized")
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
        # Store current analysis context for big buy analysis
        self.current_pool_address = pool_address
        self.current_start_block = start_block
        self.current_end_block = end_block
        
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
        
        # Analyze big buys with context
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
        Analyze big buys from price data and swap events.
        
        Args:
            prices: List of price data points
            threshold_eth: Minimum ETH amount for big buy analysis
            
        Returns:
            Dictionary with big buy analysis
        """
        try:
            from src.pricing.big_buy_analyzer import BigBuyAnalyzer
            
            # Create big buy analyzer
            analyzer = BigBuyAnalyzer()
            
            # Get pool information for swap event analysis
            pool_info = self.get_pool_info(self.current_pool_address) if hasattr(self, 'current_pool_address') else {}
            
            # Get swap events for the same time period
            if hasattr(self, 'current_pool_address') and hasattr(self, 'current_start_block') and hasattr(self, 'current_end_block'):
                self.logger.info(f"Getting swap events for big buy analysis: blocks {self.current_start_block} to {self.current_end_block}")
                swap_events = self.get_swap_events(
                    self.current_pool_address, 
                    self.current_start_block, 
                    self.current_end_block
                )
                self.logger.info(f"Got {len(swap_events)} swap events for big buy analysis")
            else:
                self.logger.warning("Missing pool address or block range for big buy analysis")
                swap_events = []
            
            # Get direct transactions (if available)
            transactions = []  # This would need to be implemented or passed in
            
            # Analyze big buys using the new analyzer with price enrichment
            big_buy_analysis = analyzer.combine_big_buy_analysis(
                swap_events=swap_events,
                transactions=transactions,
                pool_info=pool_info,
                threshold_eth=threshold_eth,
                prices=prices  # Pass prices for enrichment
            )
            
            return big_buy_analysis
            
        except Exception as e:
            self.logger.error(f"Error in big buy analysis: {e}")
            return {
                'big_buys': [],
                'threshold_eth': threshold_eth,
                'total_big_buys': 0,
                'total_eth_volume': 0.0,
                'error': str(e)
            } 