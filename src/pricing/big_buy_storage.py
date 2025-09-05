"""
Big Buy Storage Module

Detects and stores complete big buy information for swaps.
Works with both ETH and USDT thresholds.
"""

import json
import gzip
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class BigBuyStorage:
    """Detects and stores complete big buy information"""
    
    def __init__(self, usdt_oracle=None):
        self.logger = logging.getLogger(__name__)
        self.usdt_oracle = usdt_oracle
        
        # Thresholds
        self.eth_threshold = 0.1  # 0.1 ETH
        self.usdt_threshold = 100_000_000  # 100 USDT (micro-USDT)
        
        # WETH address
        self.WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    
    def detect_big_buys_from_swaps(self, swaps: List[Dict], pool_info: Dict) -> List[Dict]:
        """
        Detect big buys from a list of swaps
        
        Args:
            swaps: List of swap events
            pool_info: Pool metadata
            
        Returns:
            List of big buy records with complete information
        """
        big_buys = []
        
        try:
            token0 = pool_info.get('token0', '').lower()
            token1 = pool_info.get('token1', '').lower()
            
            for swap in swaps:
                big_buy = self._analyze_swap_for_big_buy(swap, pool_info)
                if big_buy:
                    big_buys.append(big_buy)
            
            self.logger.info(f"Detected {len(big_buys)} big buys from {len(swaps)} swaps")
            return big_buys
            
        except Exception as e:
            self.logger.error(f"Error detecting big buys: {e}")
            return []
    
    def _analyze_swap_for_big_buy(self, swap: Dict, pool_info: Dict) -> Optional[Dict]:
        """
        Analyze a single swap to determine if it's a big buy
        
        Args:
            swap: Individual swap data
            pool_info: Pool metadata
            
        Returns:
            Big buy record if detected, None otherwise
        """
        try:
            # Get basic swap info
            timestamp = swap.get('timestamp', 0)
            block_number = swap.get('blockNumber', swap.get('block_number', 0))
            tx_hash = swap.get('transactionHash', swap.get('transaction_hash', ''))
            
            # Handle different formats
            if hasattr(tx_hash, 'hex'):
                tx_hash = tx_hash.hex()
            if not tx_hash.startswith('0x'):
                tx_hash = '0x' + tx_hash
            
            # Get amounts
            amount0 = int(swap.get('amount0', swap.get('a0', 0)))
            amount1 = int(swap.get('amount1', swap.get('a1', 0)))
            
            # Get token info
            token0 = pool_info.get('token0', '').lower()
            token1 = pool_info.get('token1', '').lower()
            decimals0 = int(pool_info.get('token0_decimals', 18))
            decimals1 = int(pool_info.get('token1_decimals', 18))
            
            # Calculate ETH amount if WETH is involved
            eth_amount = 0
            token_amount = 0
            token_bought = ""
            is_buy = False
            
            if token0 == self.WETH.lower():
                # WETH is token0
                if amount0 < 0:  # WETH going in (trader buying token1)
                    eth_amount = abs(amount0) / (10 ** decimals0)
                    token_amount = abs(amount1) / (10 ** decimals1)
                    token_bought = pool_info.get('token1_symbol', 'UNKNOWN')
                    is_buy = True
            elif token1 == self.WETH.lower():
                # WETH is token1
                if amount1 < 0:  # WETH going in (trader buying token0)
                    eth_amount = abs(amount1) / (10 ** decimals1)
                    token_amount = abs(amount0) / (10 ** decimals0)
                    token_bought = pool_info.get('token0_symbol', 'UNKNOWN')
                    is_buy = True
            
            # Check if it's a big buy
            is_big_buy_eth = eth_amount >= self.eth_threshold
            
            # Calculate USDT value
            usdt_value_raw = 0
            is_big_buy_usdt = False
            
            if self.usdt_oracle:
                usdt_value_raw = self.usdt_oracle.get_usdt_value_raw(
                    {"a0": str(amount0), "a1": str(amount1), "b": block_number},
                    pool_info
                ) or 0
                is_big_buy_usdt = self.usdt_oracle.is_big_buy_usdt(usdt_value_raw)
            
            # If it's a big buy by either metric and it's a buy transaction
            if is_buy and (is_big_buy_eth or is_big_buy_usdt):
                return {
                    'timestamp': int(timestamp),
                    'block_number': int(block_number),
                    'transaction_hash': tx_hash,
                    'pool_address': pool_info.get('pool_address', ''),
                    'version': pool_info.get('version', 'UNKNOWN'),
                    'token_bought': token_bought,
                    'token_amount': f"{token_amount:.18f}",
                    'eth_amount': f"{eth_amount:.18f}",
                    'usdt_value_raw': str(usdt_value_raw),
                    'usdt_value_formatted': f"{usdt_value_raw / 1_000_000:.6f}",
                    'is_big_buy_eth': is_big_buy_eth,
                    'is_big_buy_usdt': is_big_buy_usdt,
                    'sender': swap.get('sender', ''),
                    'recipient': swap.get('recipient', swap.get('to', '')),
                    'gas_used': swap.get('gasUsed', 0),
                    'gas_price': swap.get('gasPrice', 0)
                }
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error analyzing swap for big buy: {e}")
            return None
    
    def save_big_buys_to_jsonl(self, big_buys: List[Dict], output_file: str, 
                              compress: bool = True) -> bool:
        """
        Save big buys to JSONL format
        
        Args:
            big_buys: List of big buy records
            output_file: Output file path
            compress: Whether to compress with gzip
            
        Returns:
            True if successful
        """
        try:
            if not big_buys:
                self.logger.info("No big buys to save")
                return True
            
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Add .gz extension if compressing
            if compress and not output_file.endswith('.gz'):
                output_file += '.gz'
            
            # Write big buys
            if compress:
                file_handle = gzip.open(output_file, 'wt', encoding='utf-8')
            else:
                file_handle = open(output_file, 'w', encoding='utf-8')
            
            with file_handle as f:
                for big_buy in big_buys:
                    f.write(json.dumps(big_buy, separators=(',', ':')) + '\n')
            
            self.logger.info(f"Saved {len(big_buys)} big buys to {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving big buys to JSONL: {e}")
            return False
    
    def get_big_buy_summary(self, big_buys: List[Dict]) -> Dict[str, Any]:
        """
        Generate summary statistics for big buys
        
        Args:
            big_buys: List of big buy records
            
        Returns:
            Summary dictionary
        """
        if not big_buys:
            return {
                'total_big_buys': 0,
                'total_eth_volume': 0,
                'total_usdt_volume': 0,
                'largest_eth': 0,
                'largest_usdt': 0,
                'unique_buyers': 0
            }
        
        try:
            total_eth = sum(float(bb.get('eth_amount', 0)) for bb in big_buys)
            total_usdt = sum(int(bb.get('usdt_value_raw', 0)) for bb in big_buys)
            largest_eth = max(float(bb.get('eth_amount', 0)) for bb in big_buys)
            largest_usdt = max(int(bb.get('usdt_value_raw', 0)) for bb in big_buys)
            
            unique_buyers = len(set(bb.get('sender', '') for bb in big_buys if bb.get('sender')))
            
            return {
                'total_big_buys': len(big_buys),
                'total_eth_volume': total_eth,
                'total_usdt_volume': total_usdt / 1_000_000,  # Convert to USDT
                'largest_eth': largest_eth,
                'largest_usdt': largest_usdt / 1_000_000,  # Convert to USDT
                'unique_buyers': unique_buyers
            }
            
        except Exception as e:
            self.logger.error(f"Error generating big buy summary: {e}")
            return {'error': str(e)} 