"""
ETH Price Reader Module

Reads ETH historical prices from CSV file or gets live prices from archive node.
"""

import pandas as pd
import logging
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class ETHPriceReader:
    """Reads ETH prices from CSV file or archive node"""
    
    def __init__(self, csv_file_path: str = "historical_price_eth/eth_historical_prices_complete.csv", 
                 use_node: bool = False, web3_client=None):
        """
        Initialize ETH price reader.
        
        Args:
            csv_file_path: Path to ETH historical prices CSV file
            use_node: If True, use archive node for live prices
            web3_client: Web3Client instance (required if use_node=True)
        """
        self.csv_file_path = csv_file_path
        self.use_node = use_node
        self.web3_client = web3_client
        self.eth_prices_df = None
        
        if use_node:
            if not web3_client:
                raise ValueError("web3_client is required when use_node=True")
            logger.info("Using archive node for ETH prices")
        else:
            logger.info(f"Using CSV file for ETH prices: {csv_file_path}")
            self._load_csv_prices()
    
    def _load_csv_prices(self):
        """Load ETH prices from CSV file"""
        try:
            self.eth_prices_df = pd.read_csv(self.csv_file_path)
            logger.info(f"Loaded {len(self.eth_prices_df)} ETH price records from CSV")
        except Exception as e:
            logger.error(f"Error loading ETH prices CSV: {e}")
            self.eth_prices_df = None
    
    def get_eth_price(self, timestamp: Optional[int] = None, block_number: Optional[int] = None) -> float:
        """
        Get ETH price in USD.
        
        Args:
            timestamp: Unix timestamp (used for CSV lookup)
            block_number: Block number (used for node lookup)
            
        Returns:
            ETH price in USD
        """
        if self.use_node:
            return self._get_eth_price_from_node(block_number)
        else:
            return self._get_eth_price_from_csv(timestamp)
    
    def _get_eth_price_from_node(self, block_number: Optional[int] = None) -> float:
        """Get ETH price from archive node using Chainlink oracle"""
        try:
            # Note: Chainlink oracle gives current price, not historical
            return self.web3_client.get_eth_price_usd()
        except Exception as e:
            logger.error(f"Error getting ETH price from node: {e}")
            # Fallback to default price
            return 3000.0
    
    def _get_eth_price_from_csv(self, timestamp: Optional[int] = None) -> float:
        """Get ETH price from CSV file"""
        if self.eth_prices_df is None:
            logger.warning("ETH prices CSV not loaded, using default price")
            return 3000.0
        
        if timestamp is None:
            logger.warning("No timestamp provided for CSV lookup, using latest price")
            return float(self.eth_prices_df.iloc[-1]['price_usd'])
        
        try:
            # Convert timestamp to datetime for comparison
            target_date = datetime.fromtimestamp(timestamp)
            
            # Convert CSV timestamp column to datetime if it's not already
            if 'timestamp' in self.eth_prices_df.columns:
                # Assume timestamp is in Unix format
                self.eth_prices_df['datetime'] = pd.to_datetime(self.eth_prices_df['timestamp'], unit='s')
            elif 'date' in self.eth_prices_df.columns:
                # Assume date is in string format
                self.eth_prices_df['datetime'] = pd.to_datetime(self.eth_prices_df['date'])
            else:
                logger.error("No timestamp or date column found in ETH prices CSV")
                return 3000.0
            
            # Find closest price by date
            self.eth_prices_df['time_diff'] = abs(self.eth_prices_df['datetime'] - target_date)
            closest_row = self.eth_prices_df.loc[self.eth_prices_df['time_diff'].idxmin()]
            
            # Get price from the appropriate column
            if 'price_usd' in closest_row:
                price = float(closest_row['price_usd'])
            elif 'price' in closest_row:
                price = float(closest_row['price'])
            else:
                logger.error("No price column found in ETH prices CSV")
                return 3000.0
            
            logger.debug(f"Found ETH price ${price:.2f} for timestamp {timestamp}")
            return price
            
        except Exception as e:
            logger.error(f"Error getting ETH price from CSV: {e}")
            return 3000.0 