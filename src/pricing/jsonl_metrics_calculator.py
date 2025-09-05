"""
JSONL Metrics Calculator

Calculates aggregated metrics from JSONL swap files for the main CSV report.
"""

import json
import gzip
from typing import Dict, Set, Optional
from pathlib import Path

class JSONLMetricsCalculator:
    """Calculates metrics from JSONL swap files"""
    
    def __init__(self):
        pass
    
    def calculate_metrics_from_jsonl(self, jsonl_file_path: str) -> Dict:
        """
        Calculate aggregated metrics from a JSONL file
        
        Returns:
            Dict with volume_24h_usd, unique_traders, big_buys_count, etc.
        """
        try:
            if not Path(jsonl_file_path).exists():
                return self._empty_metrics()
            
            total_usdt = 0
            big_buys_count = 0
            traders = set()
            largest_usdt = 0
            largest_eth = 0.0
            total_swaps = 0
            
            # Read compressed JSONL
            with gzip.open(jsonl_file_path, 'rt') as f:
                for line in f:
                    try:
                        swap = json.loads(line.strip())
                        total_swaps += 1
                        
                        # USDT volume
                        usdt_micro = int(swap.get('usdt', 0))
                        total_usdt += usdt_micro
                        
                        # Big buys
                        if swap.get('bb', 0) == 1:
                            big_buys_count += 1
                            if usdt_micro > largest_usdt:
                                largest_usdt = usdt_micro
                        
                        # Unique traders (sender and recipient)
                        sender = swap.get('sd', '').lower()
                        recipient = swap.get('rc', '').lower()
                        if sender and sender != '0x':
                            traders.add(sender)
                        if recipient and recipient != '0x':
                            traders.add(recipient)
                        
                        # Calculate ETH amount for largest tracking
                        token1 = swap.get('t1', '').lower()
                        weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                        
                        if token1 == weth:
                            a1 = int(swap.get('a1', 0))
                            eth_amount = abs(a1) / 1e18
                            if eth_amount > largest_eth:
                                largest_eth = eth_amount
                        
                    except (json.JSONDecodeError, ValueError) as e:
                        continue  # Skip malformed lines
            
            return {
                'volume_24h_usd': total_usdt / 1_000_000,  # Convert to USD
                'unique_traders': len(traders),
                'big_buys_count': big_buys_count,
                'largest_buy_usd': largest_usdt / 1_000_000,
                'largest_buy_eth': largest_eth,
                'total_swaps': total_swaps
            }
            
        except Exception as e:
            print(f"Error calculating metrics from {jsonl_file_path}: {e}")
            return self._empty_metrics()
    
    def _empty_metrics(self) -> Dict:
        """Return empty metrics structure"""
        return {
            'volume_24h_usd': 0.0,
            'unique_traders': 0,
            'big_buys_count': 0,
            'largest_buy_usd': 0.0,
            'largest_buy_eth': 0.0,
            'total_swaps': 0
        } 