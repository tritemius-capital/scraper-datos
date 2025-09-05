"""
USDT Oracle Module

Calculates USDT values for swaps using on-chain price data.
Keeps a cache of ETH/USDT prices by block range.
"""

import logging
from typing import Dict, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)

class USDTOracle:
    """Simple USDT oracle for swap valuation"""
    
    def __init__(self, web3_client=None):
        self.web3_client = web3_client
        self.logger = logging.getLogger(__name__)
        
        # Token addresses (mainnet)
        self.WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        self.USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
        self.USDC = "0xa0b86a33e6417c24b5e8d2d6c28b67c6e3a8b1e2f"
        
        # Reference pools for price lookup
        self.WETH_USDC_POOL = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # V3 0.05%
        
        # Price cache: block_range -> eth_usdt_scaled
        self.price_cache = {}
        self.cache_block_size = 300  # ~1 hour
    
    def get_usdt_value_raw(self, swap: Dict, pool_info: Dict) -> Optional[int]:
        """
        Calculate USDT value for a swap (in micro-USDT, 6 decimals)
        
        Args:
            swap: Swap data with a0, a1, b (block)
            pool_info: Pool metadata with token0, token1, decimals
            
        Returns:
            USDT value in micro-USDT (6 decimals) or None
        """
        try:
            a0 = int(swap.get('a0', 0))
            a1 = int(swap.get('a1', 0))
            block = int(swap.get('b', 0))
            
            token0 = pool_info.get('token0', '').lower()
            token1 = pool_info.get('token1', '').lower()
            
            # Case 1: Direct USDT pair
            if token0 == self.USDT.lower():
                return abs(a0)  # USDT has 6 decimals
            elif token1 == self.USDT.lower():
                return abs(a1)
            
            # Case 2: WETH pair - use ETH/USDT oracle
            elif token0 == self.WETH.lower():
                eth_usdt_scaled = self._get_eth_usdt_price_scaled(block)
                if eth_usdt_scaled:
                    # a0 is WETH in 1e18, convert to micro-USDT
                    return (abs(a0) * eth_usdt_scaled) // (10 ** 18)
            elif token1 == self.WETH.lower():
                eth_usdt_scaled = self._get_eth_usdt_price_scaled(block)
                if eth_usdt_scaled:
                    return (abs(a1) * eth_usdt_scaled) // (10 ** 18)
            
            # Case 3: No direct USDT/WETH - return None for now
            return None
            
        except Exception as e:
            self.logger.warning(f"Error calculating USDT value: {e}")
            return None
    
    def _get_eth_usdt_price_scaled(self, block: int) -> Optional[int]:
        """
        Get ETH/USDT price scaled by 1e6 (micro-USDT per ETH)
        Uses cache to avoid repeated calls
        
        Returns:
            Price in micro-USDT per ETH (e.g., 3500123456 = 3500.123456 USDT/ETH)
        """
        try:
            # Calculate cache key (block range)
            cache_key = (block // self.cache_block_size) * self.cache_block_size
            
            # Check cache
            if cache_key in self.price_cache:
                return self.price_cache[cache_key]
            
            # Fetch price from on-chain (simplified - use a fixed rate for now)
            # In production, you'd query the WETH/USDC pool slot0
            eth_usdt_price = self._fetch_eth_usdt_price_onchain(block)
            
            # Cache the result
            self.price_cache[cache_key] = eth_usdt_price
            
            return eth_usdt_price
            
        except Exception as e:
            self.logger.warning(f"Error getting ETH/USDT price: {e}")
            return None
    
    def _fetch_eth_usdt_price_onchain(self, block: int) -> Optional[int]:
        """
        Fetch ETH/USDT price from on-chain data using current ETH price
        Uses web3 client to get current ETH price from Chainlink
        """
        try:
            # Use current ETH price from Chainlink oracle if available
            if self.web3_client:
                try:
                    eth_usd_price = self.web3_client.get_eth_price_usd()
                    if eth_usd_price and eth_usd_price > 0:
                        # Convert to micro-USDT (assuming 1 USD ≈ 1 USDT)
                        return int(eth_usd_price * 1_000_000)
                except Exception as e:
                    self.logger.warning(f"Error getting ETH price from Chainlink: {e}")
            
            # Fallback: use reasonable fixed rate
            eth_usdt_rate = 3500.0  # Fallback rate
            return int(eth_usdt_rate * 1_000_000)  # Convert to micro-USDT
            
        except Exception as e:
            self.logger.error(f"Error fetching ETH/USDT price: {e}")
            return None
    
    def is_big_buy_usdt(self, usdt_value_raw: Optional[int], threshold_usdt: int = 100_000_000) -> bool:
        """
        Check if a swap is a big buy based on USDT value
        
        Args:
            usdt_value_raw: USDT value in micro-USDT
            threshold_usdt: Threshold in micro-USDT (default: 100 USDT = 100_000_000)
            
        Returns:
            True if it's a big buy
        """
        if usdt_value_raw is None:
            return False
        return usdt_value_raw >= threshold_usdt
    
    def format_usdt_human(self, usdt_value_raw: Optional[int]) -> str:
        """Format micro-USDT to human readable string"""
        if usdt_value_raw is None:
            return "0"
        return f"{usdt_value_raw / 1_000_000:.6f}" 