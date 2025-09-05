"""
Advanced Analytics Module

Provides advanced analytics for token and pool data including:
- Holder analysis
- Activity metrics
- Pool creation and first swap detection
- Volume analysis
- Unique trader counting
"""

import logging
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict, Counter
from datetime import datetime

logger = logging.getLogger(__name__)

class AdvancedAnalytics:
    """Advanced analytics for token and pool data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def analyze_trading_activity(self, swap_events: List[Dict]) -> Dict:
        """
        Analyze trading activity from swap events
        
        Args:
            swap_events: List of swap events
            
        Returns:
            Dictionary with activity metrics
        """
        if not swap_events:
            return {
                'total_swaps': 0,
                'unique_traders': 0,
                'unique_senders': 0,
                'unique_recipients': 0,
                'first_swap': None,
                'last_swap': None,
                'most_active_trader': None,
                'trading_sessions': 0
            }
        
        try:
            # Count unique addresses
            senders = set()
            recipients = set()
            all_traders = set()
            
            # Track activity by address
            trader_activity = defaultdict(int)
            
            # Track blocks for session analysis
            blocks = []
            
            # Process each swap
            for event in swap_events:
                sender = event.get('sender', '').lower()
                recipient = event.get('recipient', '').lower()
                block_number = event.get('blockNumber', 0)
                
                if sender:
                    senders.add(sender)
                    all_traders.add(sender)
                    trader_activity[sender] += 1
                    
                if recipient and recipient != sender:
                    recipients.add(recipient)
                    all_traders.add(recipient)
                    trader_activity[recipient] += 1
                
                if isinstance(block_number, int) and block_number > 0:
                    blocks.append(block_number)
            
            # Sort events by block number to find first/last
            sorted_events = sorted(swap_events, key=lambda x: x.get('blockNumber', 0))
            first_swap = sorted_events[0] if sorted_events else None
            last_swap = sorted_events[-1] if sorted_events else None
            
            # Find most active trader
            most_active_trader = max(trader_activity.items(), key=lambda x: x[1]) if trader_activity else None
            
            # Estimate trading sessions (group by block ranges)
            trading_sessions = self._estimate_trading_sessions(blocks)
            
            return {
                'total_swaps': len(swap_events),
                'unique_traders': len(all_traders),
                'unique_senders': len(senders),
                'unique_recipients': len(recipients),
                'first_swap': {
                    'block_number': first_swap.get('blockNumber') if first_swap else None,
                    'transaction_hash': first_swap.get('transactionHash') if first_swap else None,
                    'sender': first_swap.get('sender') if first_swap else None
                } if first_swap else None,
                'last_swap': {
                    'block_number': last_swap.get('blockNumber') if last_swap else None,
                    'transaction_hash': last_swap.get('transactionHash') if last_swap else None,
                    'sender': last_swap.get('sender') if last_swap else None
                } if last_swap else None,
                'most_active_trader': {
                    'address': most_active_trader[0],
                    'swap_count': most_active_trader[1]
                } if most_active_trader else None,
                'trading_sessions': trading_sessions,
                'trader_activity_distribution': dict(Counter(trader_activity.values()))
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing trading activity: {e}")
            return {
                'total_swaps': len(swap_events),
                'unique_traders': 0,
                'error': str(e)
            }
    
    def _estimate_trading_sessions(self, blocks: List[int]) -> int:
        """
        Estimate number of trading sessions based on block gaps
        
        Args:
            blocks: List of block numbers
            
        Returns:
            Estimated number of trading sessions
        """
        if not blocks:
            return 0
            
        if len(blocks) == 1:
            return 1
        
        # Sort blocks
        sorted_blocks = sorted(blocks)
        
        # Define session gap (approximately 1 hour = ~300 blocks)
        session_gap = 300
        sessions = 1
        
        for i in range(1, len(sorted_blocks)):
            if sorted_blocks[i] - sorted_blocks[i-1] > session_gap:
                sessions += 1
        
        return sessions
    
    def analyze_volume_patterns(self, swap_events: List[Dict], token_address: str) -> Dict:
        """
        Analyze volume patterns for a specific token
        
        Args:
            swap_events: List of swap events
            token_address: Token address to analyze
            
        Returns:
            Dictionary with volume analysis
        """
        if not swap_events:
            return {
                'total_volume_token': 0,
                'total_volume_eth': 0,
                'buy_volume': 0,
                'sell_volume': 0,
                'volume_by_hour': {},
                'large_trades': []
            }
        
        try:
            token_address = token_address.lower()
            total_volume_token = 0
            total_volume_eth = 0
            buy_volume = 0
            sell_volume = 0
            volume_by_hour = defaultdict(float)
            large_trades = []
            
            # Define large trade threshold (in ETH)
            large_trade_threshold = 1.0
            
            for event in swap_events:
                # Determine if this is a buy or sell for our token
                amount0 = event.get('amount0', 0)
                amount1 = event.get('amount1', 0)
                
                # Get ETH volume if available
                eth_volume = event.get('eth_volume', 0)
                if eth_volume:
                    total_volume_eth += abs(eth_volume)
                    
                    # Check if it's a large trade
                    if abs(eth_volume) >= large_trade_threshold:
                        large_trades.append({
                            'block_number': event.get('blockNumber'),
                            'transaction_hash': event.get('transactionHash'),
                            'eth_volume': abs(eth_volume),
                            'sender': event.get('sender'),
                            'type': 'buy' if amount0 > 0 else 'sell'
                        })
                
                # Estimate hour from block number (rough approximation)
                block_number = event.get('blockNumber', 0)
                if block_number:
                    # Approximate hour (12 seconds per block)
                    estimated_hour = (block_number * 12) // 3600
                    volume_by_hour[estimated_hour] += abs(eth_volume) if eth_volume else 0
                
                # Determine buy vs sell
                if amount0 > 0:  # Token out, ETH in = sell
                    sell_volume += abs(amount0)
                elif amount0 < 0:  # Token in, ETH out = buy
                    buy_volume += abs(amount0)
            
            # Sort large trades by volume
            large_trades.sort(key=lambda x: x['eth_volume'], reverse=True)
            
            return {
                'total_volume_token': total_volume_token,
                'total_volume_eth': total_volume_eth,
                'buy_volume': buy_volume,
                'sell_volume': sell_volume,
                'buy_sell_ratio': buy_volume / sell_volume if sell_volume > 0 else float('inf'),
                'volume_by_hour': dict(volume_by_hour),
                'large_trades': large_trades[:10],  # Top 10 large trades
                'large_trades_count': len(large_trades)
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing volume patterns: {e}")
            return {'error': str(e)}
    
    def detect_pool_creation_info(self, pool_address: str, web3_client) -> Dict:
        """
        Detect pool creation information
        
        Args:
            pool_address: Pool address
            web3_client: Web3Client instance
            
        Returns:
            Dictionary with pool creation info
        """
        try:
            # This is a simplified version - in practice, you'd need to:
            # 1. Find the pool creation transaction
            # 2. Get the creation block and timestamp
            # 3. Analyze initial liquidity
            
            # For now, we'll return basic info
            current_block = web3_client.get_current_block()
            
            return {
                'pool_address': pool_address.lower(),
                'creation_block': 'unknown',  # Would need to search for PoolCreated event
                'creation_timestamp': 'unknown',
                'age_blocks': 'unknown',
                'current_block': current_block,
                'note': 'Pool creation detection requires historical event scanning'
            }
            
        except Exception as e:
            self.logger.error(f"Error detecting pool creation info: {e}")
            return {'error': str(e)}
    
    def calculate_price_impact_analysis(self, swap_events: List[Dict]) -> Dict:
        """
        Analyze price impact patterns from swap events
        
        Args:
            swap_events: List of swap events with price data
            
        Returns:
            Dictionary with price impact analysis
        """
        if not swap_events:
            return {
                'high_impact_trades': [],
                'average_price_impact': 0,
                'price_volatility': 0
            }
        
        try:
            prices = []
            high_impact_trades = []
            
            # Extract prices and identify high impact trades
            for i, event in enumerate(swap_events):
                price = event.get('token_price_eth', 0)
                eth_volume = event.get('eth_volume', 0)
                
                if price > 0:
                    prices.append(price)
                    
                    # Check for high impact (large volume trades)
                    if abs(eth_volume) > 5.0:  # > 5 ETH trades
                        # Calculate price change if we have previous price
                        price_change = 0
                        if i > 0 and len(prices) > 1:
                            prev_price = prices[-2]
                            price_change = ((price - prev_price) / prev_price) * 100
                        
                        high_impact_trades.append({
                            'block_number': event.get('blockNumber'),
                            'eth_volume': abs(eth_volume),
                            'price_before': prices[-2] if len(prices) > 1 else 0,
                            'price_after': price,
                            'price_change_percent': price_change,
                            'transaction_hash': event.get('transactionHash')
                        })
            
            # Calculate volatility
            price_volatility = 0
            if len(prices) > 1:
                price_changes = []
                for i in range(1, len(prices)):
                    change = ((prices[i] - prices[i-1]) / prices[i-1]) * 100
                    price_changes.append(abs(change))
                
                price_volatility = sum(price_changes) / len(price_changes) if price_changes else 0
            
            return {
                'high_impact_trades': high_impact_trades,
                'average_price_impact': sum(trade['price_change_percent'] for trade in high_impact_trades) / len(high_impact_trades) if high_impact_trades else 0,
                'price_volatility': price_volatility,
                'total_price_points': len(prices),
                'price_range': {
                    'min': min(prices) if prices else 0,
                    'max': max(prices) if prices else 0,
                    'avg': sum(prices) / len(prices) if prices else 0
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating price impact analysis: {e}")
            return {'error': str(e)} 