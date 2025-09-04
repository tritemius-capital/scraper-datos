"""
Uniswap Extractor Factory

This module provides a factory class to automatically select the correct
Uniswap extractor (V2 or V3) based on pool characteristics or user preference.
"""

import logging
from typing import Optional
from web3 import Web3
import os

from src.uniswap.v2.extractor import UniswapV2Extractor
from src.uniswap.v3.extractor import UniswapV3Extractor
from src.uniswap.common.base_extractor import BaseUniswapExtractor


class UniswapExtractorFactory:
    """
    Factory class for creating Uniswap extractors.
    
    Automatically selects the appropriate extractor (V2 or V3) based on
    pool characteristics or user preference.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Initialize Web3 with default or node RPC URL
        node_rpc_url = os.getenv('NODE_RPC_URL')
        self.w3 = Web3(Web3.HTTPProvider(node_rpc_url if node_rpc_url else "https://eth.llamarpc.com"))
    
    def create_extractor(self, version: str, api_key: str, 
                        eth_price_file: str = "historical_price_eth/eth_historical_prices_complete.csv",
                        use_node: bool = False) -> BaseUniswapExtractor:
        """
        Create a Uniswap extractor for the specified version.
        
        Args:
            version: Uniswap version ('v2' or 'v3')
            api_key: API key for Etherscan or Archive Node
            eth_price_file: Path to ETH historical prices file
            use_node: If True, use Archive Node instead of Etherscan
            
        Returns:
            Appropriate Uniswap extractor instance
            
        Raises:
            ValueError: If version is not supported
        """
        version = version.lower()
        
        if version == 'v2':
            self.logger.info("Creating Uniswap V2 extractor")
            return UniswapV2Extractor(api_key, eth_price_file, use_node=use_node)
        elif version == 'v3':
            self.logger.info("Creating Uniswap V3 extractor")
            return UniswapV3Extractor(api_key, eth_price_file, use_node=use_node)
        else:
            raise ValueError(f"Unsupported Uniswap version: {version}. Supported versions: 'v2', 'v3'")
    
    def detect_version_from_pool(self, pool_address: str) -> Optional[str]:
        """
        Attempt to detect Uniswap version from pool address characteristics.
        
        Args:
            pool_address: Address of the Uniswap pool
            
        Returns:
            Detected version ('v2' or 'v3') or None if detection fails
        """
        try:
            # Try to call V3-specific functions first
            v3_abi = [
                {"constant":True,"inputs":[],"name":"fee","outputs":[{"name":"","type":"uint24"}],"type":"function"},
                {"constant":True,"inputs":[],"name":"tickSpacing","outputs":[{"name":"","type":"int24"}],"type":"function"}
            ]
            v3_contract = self.w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=v3_abi)
            
            # Try to call V3-specific functions
            try:
                fee = v3_contract.functions.fee().call()
                tick_spacing = v3_contract.functions.tickSpacing().call()
                self.logger.info(f"Detected Uniswap V3 pool: {pool_address} (fee: {fee}, tickSpacing: {tick_spacing})")
                return 'v3'
            except Exception:
                pass
            
            # If V3 functions fail, try V2-specific functions
            v2_abi = [
                {"constant":True,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},
                {"constant":True,"inputs":[],"name":"getReserves","outputs":[{"name":"_reserve0","type":"uint112"},{"name":"_reserve1","type":"uint112"},{"name":"_blockTimestampLast","type":"uint32"}],"type":"function"}
            ]
            v2_contract = self.w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=v2_abi)
            
            # Try to call V2-specific functions
            try:
                token0 = v2_contract.functions.token0().call()
                reserves = v2_contract.functions.getReserves().call()
                self.logger.info(f"Detected Uniswap V2 pool: {pool_address}")
                return 'v2'
            except Exception:
                pass
            
            self.logger.warning(f"Could not detect Uniswap version for {pool_address}")
            return None
            
        except Exception as e:
            self.logger.warning(f"Error detecting Uniswap version for {pool_address}: {e}")
            return None
    
    def create_auto_extractor(self, pool_address: str, api_key: str,
                             eth_price_file: str = "historical_price_eth/eth_historical_prices_complete.csv",
                             fallback_version: str = 'v2',
                             use_node: bool = False) -> BaseUniswapExtractor:
        """
        Create an extractor with automatic version detection.
        
        Args:
            pool_address: Address of the Uniswap pool
            api_key: API key for Etherscan or Archive Node
            eth_price_file: Path to ETH historical prices file
            fallback_version: Version to use if detection fails (default: 'v2')
            use_node: If True, use Archive Node instead of Etherscan
            
        Returns:
            Appropriate Uniswap extractor instance
        """
        detected_version = self.detect_version_from_pool(pool_address)
        
        if detected_version:
            version = detected_version
            self.logger.info(f"Auto-detected Uniswap {version.upper()} for pool {pool_address}")
        else:
            version = fallback_version
            self.logger.info(f"Using fallback Uniswap {fallback_version.upper()} for pool {pool_address}")
        
        return self.create_extractor(version, api_key, eth_price_file, use_node=use_node) 