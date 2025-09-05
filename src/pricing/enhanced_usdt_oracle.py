#!/usr/bin/env python3
"""
Enhanced USDT Oracle with pool metadata caching and V3 price calculations
"""

import logging
from typing import Dict, Optional, Tuple
from web3 import Web3

class EnhancedUSDTOracle:
    """Enhanced USDT oracle with caching and V3 price calculations"""
    
    def __init__(self, web3_client=None):
        self.web3_client = web3_client
        self.logger = logging.getLogger(__name__)
        
        # Token addresses (mainnet)
        self.WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        self.USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
        self.USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"  # Circle USDC (correct address)
        
        # Reference pools for price lookup (verified addresses)
        self.WETH_USDC_POOL = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # V3 0.05% WETH/USDC
        self.USDC_USDT_POOL = "0x3416cf6c708da44db2624d63ea0aaef7113527c6"  # V3 0.01% USDC/USDT
        
        # Caches
        self.pool_metadata_cache = {}  # pool_address -> {t0, t1, d0, d1, version, fee}
        self.eth_usdt_price_cache = {}  # block_range -> eth_usdt_scaled
        self.cache_block_size = 300  # ~1 hour
        
        # Big buy thresholds
        self.BIG_BUY_ETH_THRESHOLD = int(0.1 * 10**18)  # 0.1 ETH in wei
        self.BIG_BUY_USDT_THRESHOLD = 1_000_000_000  # 1000 USDT in micro-USDT
    
    def get_pool_metadata(self, pool_address: str, version: str) -> Dict:
        """Get or fetch pool metadata (cached)"""
        pool_key = pool_address.lower()
        
        if pool_key in self.pool_metadata_cache:
            return self.pool_metadata_cache[pool_key]
        
        try:
            if not self.web3_client or not hasattr(self.web3_client, 'w3'):
                raise Exception("Web3 client not available")
            
            w3 = self.web3_client.w3
            
            # Pool contract
            if version.upper() == 'V3':
                pool_abi = [
                    {"inputs": [], "name": "token0", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
                    {"inputs": [], "name": "token1", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
                    {"inputs": [], "name": "fee", "outputs": [{"type": "uint24"}], "stateMutability": "view", "type": "function"}
                ]
            else:  # V2
                pool_abi = [
                    {"inputs": [], "name": "token0", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
                    {"inputs": [], "name": "token1", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"}
                ]
            
            pool_contract = w3.eth.contract(
                address=Web3.to_checksum_address(pool_address),
                abi=pool_abi
            )
            
            token0 = pool_contract.functions.token0().call().lower()
            token1 = pool_contract.functions.token1().call().lower()
            
            # ERC20 ABI for decimals
            erc20_abi = [
                {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"}
            ]
            
            token0_contract = w3.eth.contract(address=Web3.to_checksum_address(token0), abi=erc20_abi)
            token1_contract = w3.eth.contract(address=Web3.to_checksum_address(token1), abi=erc20_abi)
            
            d0 = token0_contract.functions.decimals().call()
            d1 = token1_contract.functions.decimals().call()
            
            fee_tier = 0
            if version.upper() == 'V3':
                fee_tier = pool_contract.functions.fee().call()
            
            metadata = {
                't0': token0,
                't1': token1,
                'd0': d0,
                'd1': d1,
                'version': version.upper(),
                'fee': fee_tier
            }
            
            # Cache it
            self.pool_metadata_cache[pool_key] = metadata
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error fetching pool metadata for {pool_address}: {e}")
            return {}
    
    def get_eth_usdt_price_scaled(self, block_number: int) -> Optional[int]:
        """Get ETH/USDT price in micro-USDT per ETH (cached by block range)"""
        block_range = (block_number // self.cache_block_size) * self.cache_block_size
        
        if block_range in self.eth_usdt_price_cache:
            return self.eth_usdt_price_cache[block_range]
        
        try:
            # Primary: Get historical ETH price from WETH/USDC V3 pool at the specific block
            eth_usdc_price = self._get_v3_price_from_slot0(self.WETH_USDC_POOL, block_number)
            if eth_usdc_price and eth_usdc_price > 0:
                # USDC ≈ USDT for most purposes (stable coin peg)
                scaled_price = int(eth_usdc_price * 1_000_000)  # Convert to micro-USDT
                self.eth_usdt_price_cache[block_range] = scaled_price
                self.logger.debug(f"Historical ETH price for block {block_number}: ${eth_usdc_price:.2f}")
                return scaled_price
            
            # Fallback 1: Try current ETH price from Chainlink (less accurate for historical data)
            if self.web3_client and hasattr(self.web3_client, 'get_eth_price_usd'):
                try:
                    eth_usd_price = self.web3_client.get_eth_price_usd()
                    if eth_usd_price and eth_usd_price > 0:
                        scaled_price = int(eth_usd_price * 1_000_000)  # Convert to micro-USDT
                        self.eth_usdt_price_cache[block_range] = scaled_price
                        self.logger.warning(f"Using current ETH price ${eth_usd_price:.2f} for historical block {block_number}")
                        return scaled_price
                except Exception as e:
                    self.logger.warning(f"Error getting ETH price from Chainlink: {e}")
            
            # Final fallback (should rarely be used now)
            fallback_price = int(3500.0 * 1_000_000)  # $3500 in micro-USDT
            self.eth_usdt_price_cache[block_range] = fallback_price
            self.logger.error(f"Using fallback ETH price $3500 for block {block_number} - this may be inaccurate!")
            return fallback_price
            
        except Exception as e:
            self.logger.error(f"Error getting ETH/USDT price for block {block_number}: {e}")
            return None
    
    def _get_v3_price_from_slot0(self, pool_address: str, block_number: int) -> Optional[float]:
        """Get historical ETH price from V3 WETH/USDC pool at specific block"""
        try:
            if not self.web3_client or not hasattr(self.web3_client, 'w3'):
                return None
            
            w3 = self.web3_client.w3
            
            slot0_abi = [
                {
                    "inputs": [],
                    "name": "slot0",
                    "outputs": [
                        {"type": "uint160", "name": "sqrtPriceX96"},
                        {"type": "int24", "name": "tick"},
                        {"type": "uint16", "name": "observationIndex"},
                        {"type": "uint16", "name": "observationCardinality"},
                        {"type": "uint16", "name": "observationCardinalityNext"},
                        {"type": "uint8", "name": "feeProtocol"},
                        {"type": "bool", "name": "unlocked"}
                    ],
                    "stateMutability": "view",
                    "type": "function"
                }
            ]
            
            pool_contract = w3.eth.contract(
                address=Web3.to_checksum_address(pool_address),
                abi=slot0_abi
            )
            
            # Get historical slot0 data at the specific block
            slot0_data = pool_contract.functions.slot0().call(block_identifier=block_number)
            sqrt_price_x96 = slot0_data[0]
            
            if sqrt_price_x96 == 0:
                return None
            
            # Convert sqrtPriceX96 to actual price
            # Formula: price = (sqrtPriceX96 / 2^96)^2 * 10^(decimal1 - decimal0)
            # For WETH/USDC pool: token0=WETH(18), token1=USDC(6)
            
            # Step 1: (sqrtPriceX96 / 2^96)^2
            price_ratio = (sqrt_price_x96 / (2 ** 96)) ** 2
            
            # Step 2: Adjust for decimals (USDC has 6, WETH has 18)
            # This gives us USDC per WETH (what we want)
            weth_price_usdc = price_ratio * (10 ** (6 - 18))
            
            # The result is negative due to decimal adjustment, so we need the absolute value
            # and then invert because we want WETH price in USDC
            weth_price_usdc = abs(weth_price_usdc)
            
            if weth_price_usdc > 0:
                # Actually, let's recalculate this properly
                # sqrtPriceX96 = sqrt(price) * 2^96
                # price = (sqrtPriceX96 / 2^96)^2
                # For WETH/USDC, this gives us USDC/WETH rate
                
                price_raw = (sqrt_price_x96 ** 2) / (2 ** 192)  # This is the raw price ratio
                
                # For USDC/WETH pool: token0=USDC(6), token1=WETH(18)
                # price_raw gives us token1/token0 = WETH/USDC (small number)
                # We want USDC/WETH (large number like ~4000)
                
                # Adjust for decimals: USDC(6) vs WETH(18) = 10^12 difference
                weth_per_usdc = price_raw / (10 ** 12)  # WETH per USDC (very small)
                
                if weth_per_usdc > 0:
                    # Invert to get USDC per WETH (the ETH price we want)
                    usdc_per_weth = 1 / weth_per_usdc
                    return usdc_per_weth
                else:
                    return None
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error getting historical ETH price from block {block_number}: {e}")
            return None
    
    def get_usdt_value_for_swap(self, swap: Dict, pool_address: str, version: str) -> Tuple[Optional[int], bool]:
        """
        Calculate USDT value and big buy flag for a swap
        
        Returns:
            (usdt_value_micro, is_big_buy)
        """
        try:
            # Get pool metadata
            metadata = self.get_pool_metadata(pool_address, version)
            if not metadata:
                return None, False
            
            a0 = int(swap.get('a0', 0))
            a1 = int(swap.get('a1', 0))
            block_number = int(swap.get('b', 0))
            
            token0 = metadata['t0']
            token1 = metadata['t1']
            
            usdt_value = None
            
            # Case 1: Direct USDT pair
            if token0 == self.USDT.lower():
                usdt_value = abs(a0)  # USDT has 6 decimals, already in micro-USDT
            elif token1 == self.USDT.lower():
                usdt_value = abs(a1)
            
            # Case 2: WETH pair - convert using ETH price
            elif token0 == self.WETH.lower():
                eth_usdt_scaled = self.get_eth_usdt_price_scaled(block_number)
                if eth_usdt_scaled:
                    # a0 is WETH in wei (18 decimals), convert to micro-USDT
                    usdt_value = (abs(a0) * eth_usdt_scaled) // (10 ** 18)
            elif token1 == self.WETH.lower():
                eth_usdt_scaled = self.get_eth_usdt_price_scaled(block_number)
                if eth_usdt_scaled:
                    # a1 is WETH in wei (18 decimals), convert to micro-USDT
                    usdt_value = (abs(a1) * eth_usdt_scaled) // (10 ** 18)
            
            # Determine if it's a big buy
            is_big_buy = False
            if usdt_value and usdt_value >= self.BIG_BUY_USDT_THRESHOLD:
                is_big_buy = True
            
            # Also check ETH threshold for WETH pairs
            if token0 == self.WETH.lower() and abs(a0) >= self.BIG_BUY_ETH_THRESHOLD:
                is_big_buy = True
            elif token1 == self.WETH.lower() and abs(a1) >= self.BIG_BUY_ETH_THRESHOLD:
                is_big_buy = True
            
            return usdt_value, is_big_buy
            
        except Exception as e:
            self.logger.error(f"Error calculating USDT value for swap: {e}")
            return None, False
    
    def format_usdt_human(self, micro_usdt: Optional[int]) -> str:
        """Format micro-USDT to human readable string"""
        if micro_usdt is None:
            return "0.000000"
        return f"{micro_usdt / 1_000_000:.6f}" 