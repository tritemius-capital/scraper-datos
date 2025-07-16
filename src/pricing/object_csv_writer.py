"""
Object CSV Writer Module

Handles writing price data to CSV files with horizontal format.
"""

import csv
import logging
from typing import List, Dict, Optional


class ObjectCSVWriter:
    """Writes price data to CSV files with horizontal format."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def _format_price(self, price):
        """Format price to avoid scientific notation and make it easily readable."""
        if price is None:
            return ""
        
        if price == 0:
            return "0"
        
        # Convert to string with full decimal representation, no scientific notation
        # Use Decimal for precise formatting without scientific notation
        from decimal import Decimal, getcontext
        
        # Set precision to avoid scientific notation
        getcontext().prec = 28
        
        # Convert to Decimal and format as string
        decimal_price = Decimal(str(price))
        formatted = f"{decimal_price:.18f}"
        
        # Remove trailing zeros and decimal point if not needed
        formatted = formatted.rstrip('0').rstrip('.')
        
        return formatted
    
    def _create_block_json(self, price_data: Dict, block_index: int) -> str:
        """Create a JSON block object from price data."""
        timestamp = price_data['timestamp']
        block_number = price_data['block_number']
        token_price_eth = self._format_price(price_data['token_price_eth'])
        token_price_usd = self._format_price(price_data['token_price_usd'])
        eth_price_usd = self._format_price(price_data['eth_price_usd'])
        
        # Create JSON block: {"timestamp":"...","block_number":"...","token_price_eth":"...","token_price_usd":"...","eth_price_usd":"..."}
        block_json = f'{{"timestamp":"{timestamp}","block_number":"{block_number}","token_price_eth":"{token_price_eth}","token_price_usd":"{token_price_usd}","eth_price_usd":"{eth_price_usd}"}}'
        
        return f'bloque{block_index}:{block_json}'
    
    def _create_stats_summary(self, stats: Dict) -> str:
        """Create a summary string from price statistics."""
        if not stats:
            return ""
        
        # Format prices to avoid scientific notation
        lowest_price = self._format_price(stats['lowest_price_usd'])
        current_price = self._format_price(stats['current_price_usd'])
        highest_price = self._format_price(stats['highest_price_usd'])
        
        # Create summary: lowest_price current_price highest_price price_change_from_low price_change_from_high total_swaps
        summary = f'{{"lowest_price_usd":"{lowest_price}","current_price_usd":"{current_price}","highest_price_usd":"{highest_price}","price_change_from_low":"{stats["price_change_from_low"]:.2f}%","price_change_from_high":"{stats["price_change_from_high"]:.2f}%","total_swaps":"{stats["total_swaps"]}"}}'
        
        return summary
    
    def save_prices_to_object_csv(self, prices: List[Dict], output_file: str, token_address: str, pool_address: str = "", stats: Optional[Dict] = None):
        """
        Save price data to CSV file with JSON blocks and statistics in one column.
        
        Args:
            prices: List of price data points
            output_file: Output CSV file path
            token_address: Token address for the first column
            pool_address: Pool address for the second column
            stats: Price statistics dictionary (optional)
        """
        if not prices:
            self.logger.warning("No prices to save")
            return
        
        try:
            with open(output_file, 'w', newline='') as f:
                fieldnames = ['token_address', 'pool_address', 'price_summary', 'all_blocks']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                # Create all block objects with JSON format
                block_objects = []
                for i, price_data in enumerate(prices, 1):
                    block_obj = self._create_block_json(price_data, i)
                    block_objects.append(block_obj)
                
                # Join all blocks with space separator
                all_blocks = " ".join(block_objects)
                
                # Create price summary
                price_summary = self._create_stats_summary(stats) if stats else ""
                
                # Write single row with all data
                row_data = {
                    'token_address': token_address,
                    'pool_address': pool_address,
                    'price_summary': price_summary,
                    'all_blocks': all_blocks
                }
                writer.writerow(row_data)
            
            self.logger.info(f"Saved {len(prices)} price objects to {output_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving price objects to CSV: {e}") 