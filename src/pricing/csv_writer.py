"""
CSV Writer Module

Handles writing price data to CSV files.
"""

import csv
import logging
from typing import List, Dict


class CSVWriter:
    """Writes price data to CSV files."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def _format_price(self, price):
        """Format price to avoid scientific notation."""
        if price is None:
            return ""
        # Convert to string with full decimal representation
        return f"{price:.18f}".rstrip('0').rstrip('.') if price != 0 else "0"
    
    def save_prices_to_csv(self, prices: List[Dict], output_file: str):
        """
        Save price data to CSV file.
        
        Args:
            prices: List of price data points
            output_file: Output CSV file path
        """
        if not prices:
            self.logger.warning("No prices to save")
            return
        
        try:
            with open(output_file, 'w', newline='') as f:
                fieldnames = ['timestamp', 'block_number', 'token_price_eth', 'token_price_usd', 'eth_price_usd']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for price_data in prices:
                    # Format prices to avoid scientific notation
                    formatted_data = {
                        'timestamp': price_data['timestamp'],
                        'block_number': price_data['block_number'],
                        'token_price_eth': self._format_price(price_data['token_price_eth']),
                        'token_price_usd': self._format_price(price_data['token_price_usd']),
                        'eth_price_usd': self._format_price(price_data['eth_price_usd'])
                    }
                    writer.writerow(formatted_data)
            
            self.logger.info(f"Saved {len(prices)} price points to {output_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving prices to CSV: {e}") 