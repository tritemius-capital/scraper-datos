"""
Big Buy Analyzer Module

Analyzes transactions to identify "big buys" - transactions involving significant amounts of ETH.
Considers both direct ETH transactions and ETH/WETH amounts from Uniswap swap events.
"""

import logging
from typing import List, Dict, Optional
from web3 import Web3


class BigBuyAnalyzer:
    """Analyzes transactions to identify big buys based on ETH amounts."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # WETH contract address (Wrapped ETH)
        self.WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        
        # Uniswap V2 Router address
        self.UNISWAP_V2_ROUTER = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
        
        # Uniswap V3 Router address
        self.UNISWAP_V3_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
    
    def analyze_big_buys_from_swap_events(self, swap_events: List[Dict], pool_info: Dict, 
                                         threshold_eth: float = 0.1) -> List[Dict]:
        """
        Analyze swap events to identify big buys based on ETH/WETH amounts.
        
        Args:
            swap_events: List of raw swap events from Etherscan
            pool_info: Pool information (token0, token1, decimals)
            threshold_eth: Minimum ETH amount to consider a "big buy"
            
        Returns:
            List of big buy transactions
        """
        big_buys = []
        
        try:
            token0 = pool_info.get('token0', '').lower()
            token1 = pool_info.get('token1', '').lower()
            decimals0 = pool_info.get('decimals0', 18)
            decimals1 = pool_info.get('decimals1', 18)
            
            # Check if WETH is one of the tokens in the pool
            weth_in_pool = (self.WETH_ADDRESS.lower() in [token0, token1])
            
            if not weth_in_pool:
                self.logger.info("WETH not found in pool, skipping swap event analysis")
                return big_buys
            
            for event in swap_events:
                try:
                    # Get block number and timestamp
                    block_number = int(event.get('blockNumber', '0'), 16)
                    timestamp = int(event.get('timeStamp', '0'), 16)
                    
                    # Decode the swap event data
                    decoded_event = self._decode_swap_event(event)
                    if not decoded_event:
                        continue
                    
                    # Calculate ETH amount from the swap
                    eth_amount = self._calculate_eth_amount_from_swap(
                        decoded_event, pool_info, token0, token1, decimals0, decimals1
                    )
                    
                    if eth_amount and eth_amount >= threshold_eth:
                        big_buy = {
                            'blockNumber': block_number,
                            'timestamp': timestamp,
                            'ethAmount': eth_amount,
                            'transactionHash': event.get('transactionHash', ''),
                            'source': 'swap_event',
                            'amount0In': decoded_event.get('amount0In', 0),
                            'amount1In': decoded_event.get('amount1In', 0),
                            'amount0Out': decoded_event.get('amount0Out', 0),
                            'amount1Out': decoded_event.get('amount1Out', 0)
                        }
                        big_buys.append(big_buy)
                        self.logger.info(f"Big buy detected in block {block_number}: {eth_amount:.6f} ETH")
                
                except Exception as e:
                    self.logger.warning(f"Error analyzing swap event: {e}")
                    continue
            
            self.logger.info(f"Found {len(big_buys)} big buys from swap events")
            return big_buys
            
        except Exception as e:
            self.logger.error(f"Error analyzing big buys from swap events: {e}")
            return big_buys
    
    def _decode_swap_event(self, event: Dict) -> Optional[Dict]:
        """Decode a swap event from raw event data (supports both V2 and V3)."""
        try:
            # Check if this is V2 or V3 based on the number of topics
            # V2: Swap(address, uint256, uint256, uint256, uint256, address) - 3 topics
            # V3: Swap(address, address, int256, int256, uint160, uint128, int24) - 3 topics
            
            topics = event.get('topics', [])
            data = event.get('data', '')
            
            if not data or data == '0x':
                return None
            
            # Remove '0x' prefix and decode
            data = data[2:]  # Remove '0x'
            
            # Try V2 format first (4 uint256 parameters)
            if len(data) >= 256:  # 4 parameters * 64 hex chars
                try:
                    amount0In = int(data[0:64], 16)
                    amount1In = int(data[64:128], 16)
                    amount0Out = int(data[128:192], 16)
                    amount1Out = int(data[192:256], 16)
                    
                    return {
                        'version': 'v2',
                        'amount0In': amount0In,
                        'amount1In': amount1In,
                        'amount0Out': amount0Out,
                        'amount1Out': amount1Out
                    }
                except:
                    pass
            
            # Try V3 format (2 int256 parameters + others)
            if len(data) >= 128:  # At least 2 parameters * 64 hex chars
                try:
                    # V3: amount0, amount1 are signed integers
                    amount0_raw = int(data[0:64], 16)
                    amount1_raw = int(data[64:128], 16)
                    
                    # Convert to signed integers (two's complement)
                    if amount0_raw > 2**255:
                        amount0 = amount0_raw - 2**256
                    else:
                        amount0 = amount0_raw
                    
                    if amount1_raw > 2**255:
                        amount1 = amount1_raw - 2**256
                    else:
                        amount1 = amount1_raw
                    
                    return {
                        'version': 'v3',
                        'amount0': amount0,
                        'amount1': amount1
                    }
                except:
                    pass
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error decoding swap event: {e}")
            return None
    
    def _calculate_eth_amount_from_swap(self, decoded_event: Dict, pool_info: Dict, 
                                      token0: str, token1: str, decimals0: int, decimals1: int) -> Optional[float]:
        """
        Calculate ETH amount from a swap event (supports both V2 and V3).
        
        Args:
            decoded_event: Decoded swap event data
            pool_info: Pool information
            token0: Token0 address
            token1: Token1 address
            decimals0: Token0 decimals
            decimals1: Token1 decimals
            
        Returns:
            ETH amount in ETH units, or None if not a buy with ETH
        """
        try:
            weth_address = self.WETH_ADDRESS.lower()
            version = decoded_event.get('version', 'v2')
            
            if version == 'v2':
                # V2 format: amount0In, amount1In, amount0Out, amount1Out
                a0in = decoded_event['amount0In'] / (10 ** decimals0)
                a1in = decoded_event['amount1In'] / (10 ** decimals1)
                a0out = decoded_event['amount0Out'] / (10 ** decimals0)
                a1out = decoded_event['amount1Out'] / (10 ** decimals1)
                
                # Determine which token is WETH
                if token0 == weth_address:
                    # WETH is token0
                    weth_in = a0in
                    weth_out = a0out
                elif token1 == weth_address:
                    # WETH is token1
                    weth_in = a1in
                    weth_out = a1out
                else:
                    # WETH not in this pool
                    return None
                
                # Check if this is a buy (WETH going in, tokens coming out)
                if weth_in > 0 and weth_out == 0:
                    # This is a buy with ETH/WETH
                    return weth_in
                elif weth_out > 0 and weth_in == 0:
                    # This is a sell (WETH coming out)
                    return None
                else:
                    # Complex swap or no clear direction
                    return None
                    
            elif version == 'v3':
                # V3 format: amount0, amount1 (signed integers)
                # Positive = token going in, Negative = token going out
                a0 = decoded_event['amount0'] / (10 ** decimals0)
                a1 = decoded_event['amount1'] / (10 ** decimals1)
                
                # Determine which token is WETH
                if token0 == weth_address:
                    # WETH is token0
                    if a0 > 0:
                        # WETH going in (positive) = buy
                        return a0
                    else:
                        # WETH going out (negative) = sell
                        return None
                elif token1 == weth_address:
                    # WETH is token1
                    if a1 > 0:
                        # WETH going in (positive) = buy
                        return a1
                    else:
                        # WETH going out (negative) = sell
                        return None
                else:
                    # WETH not in this pool
                    return None
            else:
                # Unknown version
                return None
                
        except Exception as e:
            self.logger.warning(f"Error calculating ETH amount from swap: {e}")
            return None
    
    def analyze_big_buys_from_transactions(self, transactions: List[Dict], 
                                         threshold_eth: float = 0.1) -> List[Dict]:
        """
        Analyze regular transactions to identify big buys based on ETH value.
        
        Args:
            transactions: List of transaction data
            threshold_eth: Minimum ETH amount to consider a "big buy"
            
        Returns:
            List of big buy transactions
        """
        big_buys = []
        
        try:
            for tx in transactions:
                try:
                    eth_amount = tx.get('valueETH', 0)
                    
                    if eth_amount >= threshold_eth:
                        big_buy = {
                            'blockNumber': tx.get('blockNumber'),
                            'timestamp': tx.get('timestamp'),
                            'ethAmount': eth_amount,
                            'transactionHash': tx.get('hash', ''),
                            'source': 'direct_transaction',
                            'from': tx.get('from', ''),
                            'to': tx.get('to', '')
                        }
                        big_buys.append(big_buy)
                        self.logger.info(f"Big buy detected in transaction {tx.get('hash', '')}: {eth_amount:.6f} ETH")
                
                except Exception as e:
                    self.logger.warning(f"Error analyzing transaction: {e}")
                    continue
            
            self.logger.info(f"Found {len(big_buys)} big buys from direct transactions")
            return big_buys
            
        except Exception as e:
            self.logger.error(f"Error analyzing big buys from transactions: {e}")
            return big_buys
    
    def combine_big_buy_analysis(self, swap_events: List[Dict], transactions: List[Dict], 
                                pool_info: Dict, threshold_eth: float = 0.1) -> Dict:
        """
        Combine analysis from both swap events and direct transactions.
        
        Args:
            swap_events: List of swap events
            transactions: List of direct transactions
            pool_info: Pool information
            threshold_eth: Minimum ETH amount for big buy
            
        Returns:
            Combined big buy analysis
        """
        try:
            # Analyze big buys from swap events
            swap_big_buys = self.analyze_big_buys_from_swap_events(
                swap_events, pool_info, threshold_eth
            )
            
            # Analyze big buys from direct transactions
            tx_big_buys = self.analyze_big_buys_from_transactions(
                transactions, threshold_eth
            )
            
            # Combine and sort by block number
            all_big_buys = swap_big_buys + tx_big_buys
            all_big_buys.sort(key=lambda x: x.get('blockNumber', 0))
            
            # Calculate statistics
            total_eth = sum(buy.get('ethAmount', 0) for buy in all_big_buys)
            avg_eth = total_eth / len(all_big_buys) if all_big_buys else 0
            
            analysis = {
                'big_buys': all_big_buys,
                'total_big_buys': len(all_big_buys),
                'total_eth_amount': total_eth,
                'average_eth_amount': avg_eth,
                'swap_event_buys': len(swap_big_buys),
                'direct_transaction_buys': len(tx_big_buys),
                'threshold_eth': threshold_eth
            }
            
            self.logger.info(f"Big buy analysis complete: {len(all_big_buys)} total big buys, {total_eth:.6f} total ETH")
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error in combined big buy analysis: {e}")
            return {
                'big_buys': [],
                'total_big_buys': 0,
                'total_eth_amount': 0,
                'average_eth_amount': 0,
                'swap_event_buys': 0,
                'direct_transaction_buys': 0,
                'threshold_eth': threshold_eth,
                'error': str(e)
            } 