"""
Price Calculator Module

Calculates token prices from Uniswap V2 swap events.
"""

import logging
from typing import Dict, Optional


class PriceCalculator:
    """Calculates token prices from swap event data."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def calculate_token_price(self, swap_data: Dict, token_address: str, pool_address: str) -> Optional[float]:
        """
        Calculate token price from swap data.
        
        Args:
            swap_data: Decoded swap event data
            token_address: The token we're pricing
            pool_address: Pool address
            
        Returns:
            Token price in ETH or None if calculation fails
        """
        try:
            amount0_in = swap_data['amount0In']
            amount1_in = swap_data['amount1In']
            amount0_out = swap_data['amount0Out']
            amount1_out = swap_data['amount1Out']
            
            # Calculate price based on the swap direction
            if amount0_in > 0 and amount1_out > 0:
                # Token -> ETH swap
                price = amount1_out / amount0_in
            elif amount1_in > 0 and amount0_out > 0:
                # ETH -> Token swap
                price = amount1_in / amount0_out
            else:
                return None
            
            return price
            
        except Exception as e:
            self.logger.warning(f"Failed to calculate price: {e}")
            return None
    
    def convert_eth_to_usd(self, eth_price: float, eth_price_usd: float) -> float:
        """
        Convert ETH price to USD price.
        
        Args:
            eth_price: Token price in ETH
            eth_price_usd: ETH price in USD
            
        Returns:
            Token price in USD
        """
        return eth_price * eth_price_usd 