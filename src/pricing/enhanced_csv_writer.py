"""
Enhanced CSV Writer for Token Analysis

Generates clean, scalable CSV output with aggregated metrics only.
Individual swap data goes to JSONL files.
"""

import csv
import json
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class EnhancedCSVWriter:
    """Enhanced CSV writer for clean aggregated token analysis data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def save_enhanced_analysis_csv(self, 
                                 results: List[Dict],
                                 output_file: str) -> bool:
        """
        Save enhanced analysis results to CSV with clean aggregated data
        
        Args:
            results: List of analysis results
            output_file: Output CSV file path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Define enhanced fieldnames (no all_blocks)
            fieldnames = [
                'token_address',
                'pool_address', 
                'uniswap_version',
                'token_symbol',
                'token_name',
                'token_decimals',
                'token_supply',
                'current_price_usd',
                'current_price_eth',
                'lowest_price_usd',
                'highest_price_usd',
                'price_change_pct',
                'market_cap_usd',
                'pool_tvl_usd',
                'pool_tvl_eth',
                'volume_24h_usd',
                'volume_24h_eth',
                'total_swaps',
                'unique_traders',
                'big_buys_count',
                'big_buys_total_eth',
                'big_buys_total_usd',
                'largest_buy_eth',
                'largest_buy_usd',
                'first_swap_timestamp',
                'last_swap_timestamp',
                'analysis_blocks',
                'data_source'
            ]
            
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for result in results:
                    try:
                        # Extract basic info
                        token_address = result.get('token_address', '')
                        pool_address = result.get('pool_address', '')
                        version = result.get('uniswap_version', '')
                        
                        # Extract price stats
                        price_stats = result.get('price_stats', {})
                        prices = result.get('prices', [])
                        
                        # Extract pool info
                        pool_info = result.get('pool_info', {})
                        
                        # Extract big buy analysis
                        big_buy_analysis = result.get('big_buy_analysis', {})
                        big_buys = big_buy_analysis.get('big_buys', []) if isinstance(big_buy_analysis, dict) else []
                        
                        # Extract advanced analytics
                        advanced_analytics = result.get('advanced_analytics', {})
                        trading_activity = advanced_analytics.get('trading_activity', {})
                        volume_patterns = advanced_analytics.get('volume_patterns', {})
                        
                        # Calculate enhanced metrics
                        enhanced_row = self._calculate_enhanced_metrics(
                            token_address, pool_address, version,
                            price_stats, prices, pool_info,
                            big_buys, trading_activity, volume_patterns,
                            result
                        )
                        
                        writer.writerow(enhanced_row)
                        
                    except Exception as e:
                        self.logger.warning(f"Error processing result for {result.get('token_address', 'unknown')}: {e}")
                        continue
            
            self.logger.info(f"Enhanced analysis saved to {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving enhanced analysis CSV: {e}")
            return False
    
    def _calculate_enhanced_metrics(self, 
                                  token_address: str,
                                  pool_address: str, 
                                  version: str,
                                  price_stats: Dict,
                                  prices: List[Dict],
                                  pool_info: Dict,
                                  big_buys: List[Dict],
                                  trading_activity: Dict,
                                  volume_patterns: Dict,
                                  result: Dict) -> Dict:
        """Calculate enhanced metrics for a token"""
        
        # Basic info
        row = {
            'token_address': token_address,
            'pool_address': pool_address,
            'uniswap_version': version,
            'token_symbol': pool_info.get('token0_symbol', 'UNKNOWN') if pool_info.get('token0', '').lower() == token_address.lower() else pool_info.get('token1_symbol', 'UNKNOWN'),
            'token_name': pool_info.get('token0_name', 'Unknown') if pool_info.get('token0', '').lower() == token_address.lower() else pool_info.get('token1_name', 'Unknown'),
            'token_decimals': pool_info.get('token0_decimals', 18) if pool_info.get('token0', '').lower() == token_address.lower() else pool_info.get('token1_decimals', 18),
        }
        
        # Token supply and market cap
        token_supply = pool_info.get('token0_total_supply', 0) if pool_info.get('token0', '').lower() == token_address.lower() else pool_info.get('token1_total_supply', 0)
        current_price_usd = float(price_stats.get('current_price_usd', 0))
        current_price_eth = float(price_stats.get('current_price_eth', 0))
        
        row.update({
            'token_supply': self._format_large_number(token_supply),
            'current_price_usd': f"{current_price_usd:.12f}",
            'current_price_eth': f"{current_price_eth:.18f}",
            'market_cap_usd': self._format_large_number(float(token_supply) * current_price_usd) if token_supply else "0",
        })
        
        # Price metrics
        lowest_price = float(price_stats.get('lowest_price_usd', 0))
        highest_price = float(price_stats.get('highest_price_usd', 0))
        
        price_change = 0
        if lowest_price > 0:
            price_change = ((current_price_usd - lowest_price) / lowest_price) * 100
        
        row.update({
            'lowest_price_usd': f"{lowest_price:.12f}",
            'highest_price_usd': f"{highest_price:.12f}",
            'price_change_pct': f"{price_change:.2f}%",
        })
        
        # Pool TVL
        pool_tvl_usd = pool_info.get('tvl_usd', 0)
        pool_tvl_eth = pool_info.get('tvl_eth', 0)
        
        row.update({
            'pool_tvl_usd': self._format_large_number(pool_tvl_usd),
            'pool_tvl_eth': f"{float(pool_tvl_eth):.6f}",
        })
        
        # Volume metrics
        volume_24h_eth = volume_patterns.get('total_volume_eth', 0)
        volume_24h_usd = volume_patterns.get('total_volume_usd', 0)
        
        row.update({
            'volume_24h_usd': self._format_large_number(volume_24h_usd),
            'volume_24h_eth': f"{float(volume_24h_eth):.6f}",
        })
        
        # Trading activity
        total_swaps = len(prices)
        unique_traders = trading_activity.get('unique_traders', 0)
        
        timestamps = [p.get('timestamp', 0) for p in prices if p.get('timestamp')]
        first_swap = min(timestamps) if timestamps else 0
        last_swap = max(timestamps) if timestamps else 0
        
        row.update({
            'total_swaps': str(total_swaps),
            'unique_traders': str(unique_traders),
            'first_swap_timestamp': str(first_swap),
            'last_swap_timestamp': str(last_swap),
        })
        
        # Big buy analysis
        big_buys_count = len(big_buys)
        big_buys_total_eth = sum(float(buy.get('ethAmount', 0)) for buy in big_buys)
        big_buys_total_usd = sum(float(buy.get('usd_value', 0)) for buy in big_buys if buy.get('usd_value'))
        
        largest_buy_eth = max((float(buy.get('ethAmount', 0)) for buy in big_buys), default=0)
        largest_buy_usd = max((float(buy.get('usd_value', 0)) for buy in big_buys if buy.get('usd_value')), default=0)
        
        row.update({
            'big_buys_count': str(big_buys_count),
            'big_buys_total_eth': f"{big_buys_total_eth:.6f}",
            'big_buys_total_usd': self._format_large_number(big_buys_total_usd),
            'largest_buy_eth': f"{largest_buy_eth:.6f}",
            'largest_buy_usd': self._format_large_number(largest_buy_usd),
        })
        
        # Analysis metadata
        row.update({
            'analysis_blocks': str(result.get('blocks_analyzed', 0)),
            'data_source': result.get('data_source', 'unknown'),
        })
        
        return row
    
    def _format_large_number(self, value: float) -> str:
        """Format large numbers in a readable way"""
        try:
            value = float(value)
            if value == 0:
                return "0"
            elif value >= 1e12:
                return f"{value/1e12:.2f}T"
            elif value >= 1e9:
                return f"{value/1e9:.2f}B"
            elif value >= 1e6:
                return f"{value/1e6:.2f}M"
            elif value >= 1e3:
                return f"{value/1e3:.2f}K"
            elif value >= 1:
                return f"{value:.2f}"
            elif value >= 1e-6:
                return f"{value:.8f}"
            else:
                return f"{value:.12f}"
        except:
            return "0" 