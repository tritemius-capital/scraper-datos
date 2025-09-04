"""
Consolidated CSV Writer Module

Handles writing consolidated analysis data to CSV files.
"""

import csv
import logging
import os
from typing import List, Dict
from datetime import datetime


class ConsolidatedCSVWriter:
    """Writes consolidated analysis data to CSV files."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def create_consolidated_csv(self, results_data: List[Dict], output_file: str = "data/consolidated_analysis.csv"):
        """
        Create a consolidated CSV file with all token analysis results.
        
        Args:
            results_data: List of dictionaries with token analysis results
            output_file: Path to the output CSV file
        """
        if not results_data:
            self.logger.warning("No data to consolidate")
            return False
        
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Define CSV headers
            headers = [
                'token_name',
                'token_address', 
                'pool_address',
                'uniswap_version',
                'analysis_timestamp',
                'blocks_analyzed',
                'start_block',
                'end_block',
                
                # Price metrics
                'total_swaps',
                'current_price_usd',
                'lowest_price_usd',
                'highest_price_usd',
                'price_change_from_low_pct',
                'price_change_from_high_pct',
                
                # Big buy metrics
                'big_buys_count',
                'total_big_buy_eth',
                'total_big_buy_usd',
                'avg_big_buy_eth',
                'largest_big_buy_eth',
                
                # Generated files
                'detailed_csv_file',
                'summary_csv_file',
                'compact_csv_file'
            ]
            
            # Write consolidated CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                
                for result in results_data:
                    # Create row with only the fields we want in the consolidated view
                    row = {header: result.get(header, '') for header in headers}
                    writer.writerow(row)
            
            self.logger.info(f"Created consolidated CSV with {len(results_data)} tokens: {output_file}")
            
            # Create a detailed breakdown CSV as well
            detailed_output = output_file.replace('.csv', '_detailed_breakdown.csv')
            self._create_detailed_breakdown(results_data, detailed_output)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating consolidated CSV: {e}")
            return False
    
    def _create_detailed_breakdown(self, results_data: List[Dict], output_file: str):
        """
        Create a detailed breakdown CSV with transaction-level data for all tokens.
        
        Args:
            results_data: List of token analysis results
            output_file: Path to the detailed breakdown CSV
        """
        try:
            all_transactions = []
            
            for result in results_data:
                token_name = result.get('token_name', 'Unknown')
                token_address = result.get('token_address', '')
                pool_address = result.get('pool_address', '')
                uniswap_version = result.get('uniswap_version', '')
                
                # Get detailed prices (transaction data)
                detailed_prices = result.get('detailed_prices', [])
                
                for price_data in detailed_prices:
                    transaction = {
                        # Token info
                        'token_name': token_name,
                        'token_address': token_address,
                        'pool_address': pool_address,
                        'uniswap_version': uniswap_version,
                        
                        # Transaction data
                        'timestamp': price_data.get('timestamp', ''),
                        'datetime': datetime.fromtimestamp(price_data.get('timestamp', 0)).strftime('%Y-%m-%d %H:%M:%S') if price_data.get('timestamp') else '',
                        'block_number': price_data.get('block_number', ''),
                        'transaction_hash': price_data.get('transaction_hash', ''),
                        
                        # Price data
                        'token_price_eth': self._format_decimal(price_data.get('token_price_eth')),
                        'token_price_usd': self._format_decimal(price_data.get('token_price_usd')),
                        'eth_price_usd': self._format_decimal(price_data.get('eth_price_usd')),
                        
                        # Volume data
                        'eth_volume': self._format_decimal(price_data.get('eth_volume', '')),
                        'usd_volume': self._format_decimal(price_data.get('usd_volume', '')),
                        
                        # Transaction amounts
                        'amount0_in': self._format_decimal(price_data.get('amount0In', price_data.get('amount0In', ''))),
                        'amount1_in': self._format_decimal(price_data.get('amount1In', price_data.get('amount1In', ''))),
                        'amount0_out': self._format_decimal(price_data.get('amount0Out', price_data.get('amount0Out', ''))),
                        'amount1_out': self._format_decimal(price_data.get('amount1Out', price_data.get('amount1Out', ''))),
                    }
                    
                    all_transactions.append(transaction)
            
            if all_transactions:
                # Sort by timestamp (newest first)
                all_transactions.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
                
                # Write to CSV
                headers = list(all_transactions[0].keys())
                with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(all_transactions)
                
                self.logger.info(f"Created detailed breakdown CSV with {len(all_transactions)} transactions: {output_file}")
            
        except Exception as e:
            self.logger.error(f"Error creating detailed breakdown CSV: {e}")
    
    def _format_decimal(self, value):
        """Format decimal values to avoid scientific notation."""
        if value is None or value == '':
            return ''
        
        try:
            if isinstance(value, str):
                value = float(value)
            
            if value == 0:
                return '0'
            
            # Format with appropriate precision
            if value < 0.000001:
                return f"{value:.12f}".rstrip('0').rstrip('.')
            elif value < 0.01:
                return f"{value:.8f}".rstrip('0').rstrip('.')
            else:
                return f"{value:.6f}".rstrip('0').rstrip('.')
                
        except (ValueError, TypeError):
            return str(value) 