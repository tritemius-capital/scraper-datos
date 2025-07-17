"""
Object CSV Writer Module

Handles writing price data to CSV files with horizontal format.
"""

import csv
import logging
from typing import List, Dict, Optional
import json


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
    
    def _create_big_buy_json(self, big_buy_data: Dict, buy_index: int) -> str:
        """Create a JSON block object from big buy data."""
        block_number = big_buy_data.get('blockNumber', 'N/A')
        timestamp = big_buy_data.get('timestamp', 'N/A')
        eth_amount = self._format_price(big_buy_data.get('ethAmount', 0))
        transaction_hash = big_buy_data.get('transactionHash', 'N/A')
        source = big_buy_data.get('source', 'N/A')
        
        # Include additional fields if they exist
        amount0_in = big_buy_data.get('amount0In', 0)
        amount1_in = big_buy_data.get('amount1In', 0)
        amount0_out = big_buy_data.get('amount0Out', 0)
        amount1_out = big_buy_data.get('amount1Out', 0)
        
        # Create JSON block with all available fields
        big_buy_json = f'{{"blockNumber":"{block_number}","timestamp":"{timestamp}","ethAmount":"{eth_amount}","transactionHash":"{transaction_hash}","source":"{source}","amount0In":"{amount0_in}","amount1In":"{amount1_in}","amount0Out":"{amount0_out}","amount1Out":"{amount1_out}"}}'
        
        return f'big_buy{buy_index}:{big_buy_json}'
    
    def _create_big_buy_blocks(self, big_buy_analysis: Dict) -> str:
        """Create individual blocks for each big buy."""
        if not big_buy_analysis or 'big_buys' not in big_buy_analysis:
            return ""
        
        big_buys = big_buy_analysis['big_buys']
        if not big_buys:
            return ""
        
        # Create individual blocks for each big buy
        big_buy_blocks = []
        for i, big_buy in enumerate(big_buys, 1):
            big_buy_block = self._create_big_buy_json(big_buy, i)
            big_buy_blocks.append(big_buy_block)
        
        # Join all big buy blocks with space separator
        return " ".join(big_buy_blocks)
    
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
    
    def save_prices_to_object_csv(self, prices: List[Dict], output_file: str, token_address: str, pool_address: str = "", uniswap_version: str = "", stats: Optional[Dict] = None, big_buy_analysis: Optional[Dict] = None, append: bool = False):
        """
        Save price data to CSV file with JSON blocks and statistics in one column.
        
        Args:
            prices: List of price data points
            output_file: Output CSV file path
            token_address: Token address for the first column
            pool_address: Pool address for the second column
            uniswap_version: Uniswap version (v2/v3) for the third column
            stats: Price statistics dictionary (optional)
            big_buy_analysis: Big buy analysis dictionary (optional)
            append: If True, append to existing file. If False, overwrite file.
        """
        if not prices:
            self.logger.warning("No prices to save")
            return
        
        try:
            # Check if file exists and we're appending
            file_exists = False
            if append:
                try:
                    with open(output_file, 'r') as f:
                        file_exists = True
                except FileNotFoundError:
                    file_exists = False
            
            # Open file in appropriate mode
            mode = 'a' if append and file_exists else 'w'
            with open(output_file, mode, newline='') as f:
                fieldnames = ['token_address', 'pool_address', 'uniswap_version', 'price_summary', 'big_buy_analysis', 'all_blocks']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # Write header only if creating new file or not appending
                if not append or not file_exists:
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
                
                # Create big buy analysis summary
                big_buy_summary = self._create_big_buy_blocks(big_buy_analysis) if big_buy_analysis else ""
                
                # Write single row with all data
                row_data = {
                    'token_address': token_address,
                    'pool_address': pool_address,
                    'uniswap_version': uniswap_version,
                    'price_summary': price_summary,
                    'big_buy_analysis': big_buy_summary,
                    'all_blocks': all_blocks
                }
                writer.writerow(row_data)
            
            action = "Appended" if append and file_exists else "Saved"
            self.logger.info(f"{action} {len(prices)} price objects to {output_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving price objects to CSV: {e}")
    
    def append_prices_to_object_csv(self, prices: List[Dict], output_file: str, token_address: str, pool_address: str = "", uniswap_version: str = "", stats: Optional[Dict] = None, big_buy_analysis: Optional[Dict] = None):
        """
        Append price data to existing CSV file.
        
        Args:
            prices: List of price data points
            output_file: Output CSV file path
            token_address: Token address for the first column
            pool_address: Pool address for the second column
            uniswap_version: Uniswap version (v2/v3) for the third column
            stats: Price statistics dictionary (optional)
            big_buy_analysis: Big buy analysis dictionary (optional)
        """
        return self.save_prices_to_object_csv(prices, output_file, token_address, pool_address, uniswap_version, stats, big_buy_analysis, append=True) 