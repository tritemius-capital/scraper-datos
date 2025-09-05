"""
Swap JSONL Writer Module

Writes individual swap events to JSONL format for high-volume processing.
Ultra-minimal format optimized for millions of swaps.
"""

import json
import gzip
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from .usdt_oracle import USDTOracle

logger = logging.getLogger(__name__)

class SwapJSONLWriter:
    """Writes individual swap events to JSONL format (optionally compressed)"""
    
    def __init__(self, web3_client=None):
        self.logger = logging.getLogger(__name__)
        self.usdt_oracle = USDTOracle(web3_client)
    
    def write_swaps_to_jsonl(self, swaps: List[Dict], output_file: str, 
                           pool_address: str, version: str, compress: bool = True, 
                           pool_info: Optional[Dict] = None) -> bool:
        """
        Write swap events to JSONL format
        
        Args:
            swaps: List of swap events
            output_file: Output file path
            pool_address: Pool address
            version: Uniswap version (v2/v3)
            compress: Whether to compress with gzip
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Add .gz extension if compressing
            if compress and not output_file.endswith('.gz'):
                output_file += '.gz'
            
            # Open file (compressed or not)
            if compress:
                file_handle = gzip.open(output_file, 'wt', encoding='utf-8')
            else:
                file_handle = open(output_file, 'w', encoding='utf-8')
            
            swaps_written = 0
            
            with file_handle as f:
                for swap in swaps:
                    try:
                        # Convert swap to minimal JSONL format
                        minimal_swap = self._convert_to_minimal_format(
                            swap, pool_address, version, pool_info
                        )
                        
                        if minimal_swap:
                            # Write as single line JSON
                            f.write(json.dumps(minimal_swap, separators=(',', ':')) + '\n')
                            swaps_written += 1
                            
                    except Exception as e:
                        self.logger.warning(f"Error processing swap: {e}")
                        continue
            
            self.logger.info(f"Written {swaps_written} swaps to {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing swaps to JSONL: {e}")
            return False
    
    def _convert_to_minimal_format(self, swap: Dict, pool_address: str, version: str, pool_info: Optional[Dict] = None) -> Optional[Dict]:
        """
        Convert swap event to minimal JSONL format
        
        Format:
        {
            "t": timestamp,
            "b": block_number,
            "h": "tx_hash",
            "p": "pool_address", 
            "v": version_number,
            "t0": "token0_address",
            "t1": "token1_address", 
            "a0": "amount0_raw",
            "a1": "amount1_raw",
            "s": "sender",
            "r": "recipient"
        }
        
        Args:
            swap: Original swap event data
            pool_address: Pool address
            version: Uniswap version
            
        Returns:
            Minimal swap dict or None if conversion fails
        """
        try:
            # Extract basic fields
            timestamp = swap.get('timestamp', 0)
            block_number = swap.get('blockNumber', swap.get('block_number', 0))
            tx_hash = swap.get('transactionHash', swap.get('transaction_hash', ''))
            
            # Handle different formats of tx_hash and ensure 0x prefix
            if hasattr(tx_hash, 'hex'):
                tx_hash = tx_hash.hex()
            elif not isinstance(tx_hash, str):
                tx_hash = str(tx_hash)
            
            # Ensure 0x prefix for hash
            if tx_hash and not tx_hash.startswith('0x'):
                tx_hash = '0x' + tx_hash
            
            # Get token addresses from pool_info or swap data
            if pool_info:
                token0 = pool_info.get('token0', '')
                token1 = pool_info.get('token1', '')
            else:
                token0 = swap.get('token0', '')
                token1 = swap.get('token1', '')
            
            # Get amounts - these should be in raw format (wei)
            amount0 = swap.get('amount0', swap.get('amount0In', 0))
            amount1 = swap.get('amount1', swap.get('amount1In', 0))
            
            # For V2, calculate net amounts if we have In/Out separately
            if version.lower() == 'v2':
                amount0_in = swap.get('amount0In', 0)
                amount1_in = swap.get('amount1In', 0)
                amount0_out = swap.get('amount0Out', 0)
                amount1_out = swap.get('amount1Out', 0)
                
                # Net amount = Out - In (positive = pool gives to trader)
                if amount0_out or amount0_in:
                    amount0 = int(amount0_out) - int(amount0_in)
                if amount1_out or amount1_in:
                    amount1 = int(amount1_out) - int(amount1_in)
            
            # Get sender/recipient
            sender = swap.get('sender', '')
            recipient = swap.get('recipient', swap.get('to', ''))
            
            # Handle HexBytes objects and ensure 0x prefix
            if hasattr(sender, 'hex'):
                sender = sender.hex()
            if hasattr(recipient, 'hex'):
                recipient = recipient.hex()
            
            # Ensure 0x prefix for addresses
            if sender and not sender.startswith('0x'):
                sender = '0x' + sender
            if recipient and not recipient.startswith('0x'):
                recipient = '0x' + recipient
            
            # Convert version to number
            version_num = 3 if version.lower() == 'v3' else 2
            
            # Calculate USDT value
            usdt_value_raw = None
            big_buy_flag = 0
            
            if pool_info:
                usdt_value_raw = self.usdt_oracle.get_usdt_value_raw(
                    {"a0": str(amount0), "a1": str(amount1), "b": block_number}, 
                    pool_info
                )
                big_buy_flag = 1 if self.usdt_oracle.is_big_buy_usdt(usdt_value_raw) else 0
            
            # Create comprehensive swap object with all information
            minimal_swap = {
                "t": int(timestamp),
                "b": int(block_number),
                "h": str(tx_hash),  # Keep 0x prefix
                "p": str(pool_address).lower(),
                "v": version_num,
                "a0": str(amount0),  # Raw amount (see JSONL_SCHEMA.md for sign convention)
                "a1": str(amount1),  # Raw amount (see JSONL_SCHEMA.md for sign convention)
                "sd": str(sender).lower(),  # sender address
                "rc": str(recipient).lower(),  # recipient address
                "usdt": str(usdt_value_raw) if usdt_value_raw is not None else "0",  # USDT crudo (6 decimales)
                "bb": big_buy_flag,  # big buy flag
            }
            
            # Add additional swap information if available
            if swap.get('gasUsed'):
                minimal_swap['gas_used'] = str(swap['gasUsed'])
            if swap.get('gasPrice'):
                minimal_swap['gas_price'] = str(swap['gasPrice'])
            
            # Add price information if available
            if swap.get('price'):
                minimal_swap['price'] = str(swap['price'])
            if swap.get('price_usd'):
                minimal_swap['price_usd'] = str(swap['price_usd'])
            
            # Add liquidity change information (V3)
            if version_num == 3 and swap.get('liquidity'):
                minimal_swap['liquidity'] = str(swap['liquidity'])
            
            # Add tick information (V3)
            if version_num == 3 and swap.get('tick'):
                minimal_swap['tick'] = str(swap['tick'])
            
            # Add sqrtPriceX96 (V3)
            if version_num == 3 and swap.get('sqrtPriceX96'):
                minimal_swap['sqrt_price'] = str(swap['sqrtPriceX96'])
            
            return minimal_swap
            
        except Exception as e:
            self.logger.warning(f"Error converting swap to minimal format: {e}")
            return None
    
    def append_swaps_to_jsonl(self, swaps: List[Dict], output_file: str, 
                            pool_address: str, version: str, compress: bool = True,
                            pool_info: Optional[Dict] = None) -> bool:
        """
        Append swap events to existing JSONL file
        
        Args:
            swaps: List of swap events
            output_file: Output file path
            pool_address: Pool address
            version: Uniswap version
            compress: Whether file is compressed
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Add .gz extension if compressing
            if compress and not output_file.endswith('.gz'):
                output_file += '.gz'
            
            # Open file for appending
            if compress:
                file_handle = gzip.open(output_file, 'at', encoding='utf-8')
            else:
                file_handle = open(output_file, 'a', encoding='utf-8')
            
            swaps_written = 0
            
            with file_handle as f:
                for swap in swaps:
                    try:
                        minimal_swap = self._convert_to_minimal_format(
                            swap, pool_address, version, pool_info
                        )
                        
                        if minimal_swap:
                            f.write(json.dumps(minimal_swap, separators=(',', ':')) + '\n')
                            swaps_written += 1
                            
                    except Exception as e:
                        self.logger.warning(f"Error processing swap for append: {e}")
                        continue
            
            self.logger.info(f"Appended {swaps_written} swaps to {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error appending swaps to JSONL: {e}")
            return False
    
    def read_swaps_from_jsonl(self, input_file: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Read swaps from JSONL file
        
        Args:
            input_file: Input file path
            limit: Maximum number of swaps to read
            
        Returns:
            List of swap dictionaries
        """
        swaps = []
        
        try:
            # Determine if file is compressed
            is_compressed = input_file.endswith('.gz')
            
            # Open file
            if is_compressed:
                file_handle = gzip.open(input_file, 'rt', encoding='utf-8')
            else:
                file_handle = open(input_file, 'r', encoding='utf-8')
            
            with file_handle as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        line = line.strip()
                        if line:
                            swap = json.loads(line)
                            swaps.append(swap)
                            
                            if limit and len(swaps) >= limit:
                                break
                                
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Invalid JSON on line {line_num}: {e}")
                        continue
                    except Exception as e:
                        self.logger.warning(f"Error reading line {line_num}: {e}")
                        continue
            
            self.logger.info(f"Read {len(swaps)} swaps from {input_file}")
            return swaps
            
        except Exception as e:
            self.logger.error(f"Error reading swaps from JSONL: {e}")
            return []
    
    def get_file_stats(self, input_file: str) -> Dict[str, Any]:
        """
        Get statistics about a JSONL swap file
        
        Args:
            input_file: Input file path
            
        Returns:
            Dictionary with file statistics
        """
        try:
            is_compressed = input_file.endswith('.gz')
            
            if is_compressed:
                file_handle = gzip.open(input_file, 'rt', encoding='utf-8')
            else:
                file_handle = open(input_file, 'r', encoding='utf-8')
            
            total_swaps = 0
            versions = set()
            pools = set()
            tokens = set()
            blocks = []
            
            with file_handle as f:
                for line in f:
                    try:
                        line = line.strip()
                        if line:
                            swap = json.loads(line)
                            total_swaps += 1
                            
                            versions.add(swap.get('v', 0))
                            pools.add(swap.get('p', ''))
                            tokens.add(swap.get('t0', ''))
                            tokens.add(swap.get('t1', ''))
                            
                            block_num = swap.get('b', 0)
                            if block_num:
                                blocks.append(block_num)
                                
                    except:
                        continue
            
            # Calculate block range
            block_range = {}
            if blocks:
                block_range = {
                    'min': min(blocks),
                    'max': max(blocks),
                    'span': max(blocks) - min(blocks)
                }
            
            stats = {
                'total_swaps': total_swaps,
                'versions': sorted(list(versions)),
                'unique_pools': len(pools),
                'unique_tokens': len(tokens) - 1 if '' in tokens else len(tokens),  # Exclude empty strings
                'block_range': block_range,
                'compressed': is_compressed
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting file stats: {e}")
            return {'error': str(e)} 