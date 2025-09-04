"""
Detailed CSV Writer Module

Handles writing detailed transaction data to CSV files for comprehensive analysis.
"""

import csv
import logging
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime


class DetailedCSVWriter:
    """Writes detailed transaction data to CSV files for analysis."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def save_detailed_transactions(self, 
                                 prices: List[Dict], 
                                 big_buy_analysis: Dict,
                                 token_address: str,
                                 pool_address: str,
                                 uniswap_version: str,
                                 stats: Dict,
                                 output_file: str):
        """
        Save detailed transaction data to CSV for comprehensive analysis.
        
        Args:
            prices: List of price data points
            big_buy_analysis: Big buy analysis data
            token_address: Token contract address
            pool_address: Pool contract address  
            uniswap_version: Uniswap version (v2/v3)
            stats: Price statistics
            output_file: Output CSV file path
        """
        if not prices:
            self.logger.warning("No transaction data to save")
            return
        
        try:
            # Prepare detailed transaction data
            detailed_transactions = []
            
            # Get big buys for reference
            big_buys = big_buy_analysis.get('big_buys', []) if big_buy_analysis else []
            big_buy_hashes = {buy.get('transactionHash', ''): buy for buy in big_buys}
            
            for price_data in prices:
                # Basic transaction info
                tx_data = {
                    'timestamp': price_data.get('timestamp', ''),
                    'datetime': datetime.fromtimestamp(price_data.get('timestamp', 0)).strftime('%Y-%m-%d %H:%M:%S') if price_data.get('timestamp') else '',
                    'block_number': price_data.get('block_number', ''),
                    'transaction_hash': price_data.get('transaction_hash', ''),
                    
                    # Token and pool info
                    'token_address': token_address,
                    'pool_address': pool_address,
                    'uniswap_version': uniswap_version,
                    
                    # Price data
                    'token_price_eth': self._format_decimal(price_data.get('token_price_eth')),
                    'token_price_usd': self._format_decimal(price_data.get('token_price_usd')),
                    'eth_price_usd': self._format_decimal(price_data.get('eth_price_usd')),
                    
                    # Transaction amounts (V2 format)
                    'amount0_in': self._format_decimal(price_data.get('amount0In', price_data.get('amount0In', 0))),
                    'amount1_in': self._format_decimal(price_data.get('amount1In', price_data.get('amount1In', 0))),
                    'amount0_out': self._format_decimal(price_data.get('amount0Out', price_data.get('amount0Out', 0))),
                    'amount1_out': self._format_decimal(price_data.get('amount1Out', price_data.get('amount1Out', 0))),
                    
                    # V3 specific data
                    'amount0': self._format_decimal(price_data.get('amount0', '')),
                    'amount1': self._format_decimal(price_data.get('amount1', '')),
                    'sqrtPriceX96': self._format_decimal(price_data.get('sqrtPriceX96', '')),
                    'liquidity': self._format_decimal(price_data.get('liquidity', '')),
                    'tick': price_data.get('tick', ''),
                    
                    # Sender/recipient info
                    'sender': price_data.get('sender', ''),
                    'recipient': price_data.get('recipient', price_data.get('to', '')),
                    
                    # Calculated values
                    'eth_volume': self._format_decimal(price_data.get('eth_volume', '')),
                    'usd_volume': self._format_decimal(price_data.get('usd_volume', '')),
                    'transaction_type': self._determine_transaction_type(price_data),
                    
                    # Gas data
                    'gas_used': price_data.get('gas_used', ''),
                    'gas_price': price_data.get('gas_price', ''),
                    
                    # Big buy flag
                    'is_big_buy': price_data.get('transaction_hash', '') in big_buy_hashes,
                    'big_buy_eth_amount': '',
                    'big_buy_usd_value': '',
                }
                
                # Add big buy specific data if this is a big buy
                if tx_data['is_big_buy']:
                    big_buy_data = big_buy_hashes[price_data.get('transaction_hash', '')]
                    tx_data['big_buy_eth_amount'] = self._format_decimal(big_buy_data.get('ethAmount'))
                    tx_data['big_buy_usd_value'] = self._format_decimal(big_buy_data.get('usd_value'))
                
                detailed_transactions.append(tx_data)
            
            # Create DataFrame and save to CSV
            df = pd.DataFrame(detailed_transactions)
            
            # Sort by timestamp (newest first)
            df = df.sort_values('timestamp', ascending=False)
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            
            # Create summary file
            summary_file = output_file.replace('.csv', '_summary.csv')
            self._create_summary_file(stats, big_buy_analysis, token_address, pool_address, uniswap_version, summary_file)
            
            self.logger.info(f"Saved {len(detailed_transactions)} detailed transactions to {output_file}")
            self.logger.info(f"Saved summary to {summary_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving detailed transactions: {e}")
    
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
    
    def _calculate_eth_volume(self, price_data: Dict) -> str:
        """Calculate ETH volume from transaction amounts."""
        try:
            amount0_in = float(price_data.get('amount0In', 0)) if price_data.get('amount0In') else 0
            amount1_in = float(price_data.get('amount1In', 0)) if price_data.get('amount1In') else 0
            amount0_out = float(price_data.get('amount0Out', 0)) if price_data.get('amount0Out') else 0
            amount1_out = float(price_data.get('amount1Out', 0)) if price_data.get('amount1Out') else 0
            
            # Assume one of the tokens is ETH/WETH - take the larger amount
            eth_volume = max(amount0_in, amount1_in, amount0_out, amount1_out)
            
            # Convert from wei to ETH if necessary
            if eth_volume > 1e15:  # Likely in wei
                eth_volume = eth_volume / 1e18
            
            return self._format_decimal(eth_volume)
            
        except (ValueError, TypeError):
            return ''
    
    def _calculate_usd_volume(self, price_data: Dict) -> str:
        """Calculate USD volume from ETH volume and ETH price."""
        try:
            eth_volume = float(self._calculate_eth_volume(price_data))
            eth_price_usd = float(price_data.get('eth_price_usd', 0))
            
            usd_volume = eth_volume * eth_price_usd
            return self._format_decimal(usd_volume)
            
        except (ValueError, TypeError):
            return ''
    
    def _determine_transaction_type(self, price_data: Dict) -> str:
        """Determine if transaction is buy or sell based on amounts."""
        try:
            amount0_in = float(price_data.get('amount0In', 0)) if price_data.get('amount0In') else 0
            amount1_in = float(price_data.get('amount1In', 0)) if price_data.get('amount1In') else 0
            amount0_out = float(price_data.get('amount0Out', 0)) if price_data.get('amount0Out') else 0
            amount1_out = float(price_data.get('amount1Out', 0)) if price_data.get('amount1Out') else 0
            
            # Simple heuristic: if more tokens are going out than in, it's likely a buy
            if amount0_out > amount0_in or amount1_out > amount1_in:
                return 'BUY'
            elif amount0_in > amount0_out or amount1_in > amount1_out:
                return 'SELL'
            else:
                return 'SWAP'
                
        except (ValueError, TypeError):
            return 'UNKNOWN'
    
    def _create_summary_file(self, stats: Dict, big_buy_analysis: Dict, token_address: str, pool_address: str, uniswap_version: str, summary_file: str):
        """Create a summary file with key metrics."""
        try:
            summary_data = {
                'token_address': token_address,
                'pool_address': pool_address,
                'uniswap_version': uniswap_version,
                'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                
                # Price metrics
                'current_price_usd': self._format_decimal(stats.get('current_price_usd')),
                'lowest_price_usd': self._format_decimal(stats.get('lowest_price_usd')),
                'highest_price_usd': self._format_decimal(stats.get('highest_price_usd')),
                'price_change_from_low_pct': f"{stats.get('price_change_from_low', 0):.2f}%",
                'price_change_from_high_pct': f"{stats.get('price_change_from_high', 0):.2f}%",
                
                # Volume metrics
                'total_swaps': stats.get('total_swaps', 0),
                'big_buys_count': len(big_buy_analysis.get('big_buys', [])) if big_buy_analysis else 0,
                'big_buy_threshold_eth': big_buy_analysis.get('threshold_eth', 0) if big_buy_analysis else 0,
                
                # Big buy metrics
                'total_big_buy_eth': self._calculate_total_big_buy_eth(big_buy_analysis),
                'total_big_buy_usd': self._calculate_total_big_buy_usd(big_buy_analysis),
                'avg_big_buy_eth': self._calculate_avg_big_buy_eth(big_buy_analysis),
                'largest_big_buy_eth': self._calculate_largest_big_buy_eth(big_buy_analysis),
            }
            
            # Save summary
            df_summary = pd.DataFrame([summary_data])
            df_summary.to_csv(summary_file, index=False)
            
        except Exception as e:
            self.logger.error(f"Error creating summary file: {e}")
    
    def _calculate_total_big_buy_eth(self, big_buy_analysis: Dict) -> str:
        """Calculate total ETH volume from big buys."""
        try:
            if not big_buy_analysis or 'big_buys' not in big_buy_analysis:
                return '0'
            
            total = sum(float(buy.get('ethAmount', 0)) for buy in big_buy_analysis['big_buys'])
            return self._format_decimal(total)
            
        except (ValueError, TypeError):
            return '0'
    
    def _calculate_total_big_buy_usd(self, big_buy_analysis: Dict) -> str:
        """Calculate total USD value from big buys."""
        try:
            if not big_buy_analysis or 'big_buys' not in big_buy_analysis:
                return '0'
            
            total = sum(float(buy.get('usd_value', 0)) for buy in big_buy_analysis['big_buys'] if buy.get('usd_value'))
            return self._format_decimal(total)
            
        except (ValueError, TypeError):
            return '0'
    
    def _calculate_avg_big_buy_eth(self, big_buy_analysis: Dict) -> str:
        """Calculate average ETH amount of big buys."""
        try:
            if not big_buy_analysis or 'big_buys' not in big_buy_analysis:
                return '0'
            
            big_buys = big_buy_analysis['big_buys']
            if not big_buys:
                return '0'
            
            total = sum(float(buy.get('ethAmount', 0)) for buy in big_buys)
            avg = total / len(big_buys)
            return self._format_decimal(avg)
            
        except (ValueError, TypeError):
            return '0'
    
    def _calculate_largest_big_buy_eth(self, big_buy_analysis: Dict) -> str:
        """Find the largest big buy in ETH."""
        try:
            if not big_buy_analysis or 'big_buys' not in big_buy_analysis:
                return '0'
            
            big_buys = big_buy_analysis['big_buys']
            if not big_buys:
                return '0'
            
            largest = max(float(buy.get('ethAmount', 0)) for buy in big_buys)
            return self._format_decimal(largest)
            
        except (ValueError, TypeError):
            return '0' 