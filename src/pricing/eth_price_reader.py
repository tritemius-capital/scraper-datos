"""
ETH Price Reader Module

Reads historical ETH prices from local CSV file.
"""

import csv
import logging
from typing import Optional


class ETHPriceReader:
    """Reads ETH prices from local CSV file."""
    
    def __init__(self, eth_price_file: str = "historical_price_eth/eth_historical_prices_complete.csv"):
        self.eth_price_file = eth_price_file
        self.logger = logging.getLogger(__name__)
    
    def get_eth_price_at_timestamp(self, timestamp: int) -> Optional[float]:
        """
        Get ETH price in USD at a specific timestamp from local file.
        
        Args:
            timestamp: Unix timestamp
            
        Returns:
            ETH price in USD or None if not found
        """
        try:
            with open(self.eth_price_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'time' in row and 'close' in row:
                        file_timestamp = int(row['time'])
                        if file_timestamp == timestamp:
                            return float(row['close'])
            
            # If exact match not found, find closest timestamp
            closest_price = None
            min_diff = float('inf')
            
            with open(self.eth_price_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'time' in row and 'close' in row:
                        file_timestamp = int(row['time'])
                        diff = abs(file_timestamp - timestamp)
                        if diff < min_diff:
                            min_diff = diff
                            closest_price = float(row['close'])
            
            return closest_price
            
        except Exception as e:
            self.logger.warning(f"Could not get ETH price for timestamp {timestamp}: {e}")
            return None 