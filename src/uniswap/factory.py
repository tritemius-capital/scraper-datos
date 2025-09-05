"""
Uniswap Extractor Factory

This module provides a factory for creating Uniswap extractors (V2/V3)
and auto-detecting the version of a pool.
"""

import logging
from web3 import Web3
from typing import Optional

from .v2.extractor import UniswapV2Extractor
from .v3.extractor import UniswapV3Extractor

logger = logging.getLogger(__name__)

class UniswapExtractorFactory:
    """Factory for creating Uniswap extractors with version detection"""
    
    def create_extractor(self, version: str, api_key: str):
        """
        Create a Uniswap extractor using Etherscan API.
        
        Args:
            version: 'v2', 'v3', or 'auto'
            api_key: Etherscan API key
            
        Returns:
            Appropriate extractor instance
        """
        if version == "v2":
            return UniswapV2Extractor(api_key)
        elif version == "v3":
            return UniswapV3Extractor(api_key)
        elif version == "auto":
            # Return V2 by default for auto-detection
            # The actual detection happens when analyzing a specific pool
            return UniswapV2Extractor(api_key)
        else:
            raise ValueError(f"Unknown version: {version}. Use 'v2', 'v3', or 'auto'")
    
    def create_extractor_with_node(self, version: str, web3_client):
        """
        Create a Uniswap extractor using Archive Node.
        
        Args:
            version: 'v2', 'v3', or 'auto'
            web3_client: Web3Client instance
            
        Returns:
            Appropriate extractor instance
        """
        # For node-based extractors, we'll pass a dummy API key and set use_node=True
        dummy_api_key = "node"
        
        if version == "v2":
            extractor = UniswapV2Extractor(dummy_api_key, use_node=True)
            extractor.w3 = web3_client.w3  # Set the Web3 instance
            extractor.web3_client = web3_client  # Store the client for ETH prices
            extractor._init_eth_price_reader()  # Re-initialize with node support
            return extractor
        elif version == "v3":
            extractor = UniswapV3Extractor(dummy_api_key, use_node=True)
            extractor.w3 = web3_client.w3  # Set the Web3 instance
            extractor.web3_client = web3_client  # Store the client for ETH prices
            extractor._init_eth_price_reader()  # Re-initialize with node support
            return extractor
        elif version == "auto":
            # Return V2 by default for auto-detection
            extractor = UniswapV2Extractor(dummy_api_key, use_node=True)
            extractor.w3 = web3_client.w3  # Set the Web3 instance
            extractor.web3_client = web3_client  # Store the client for ETH prices
            extractor._init_eth_price_reader()  # Re-initialize with node support
            return extractor
        else:
            raise ValueError(f"Unknown version: {version}. Use 'v2', 'v3', or 'auto'")
    
    def detect_version_from_pool(self, pool_address: str, w3: Web3) -> str:
        """
        Detect Uniswap version from a pool address.
        
        Args:
            pool_address: Pool contract address
            w3: Web3 instance
            
        Returns:
            'v2' or 'v3'
        """
        try:
            pool_address = Web3.to_checksum_address(pool_address)
            
            # Try V3 specific functions first
            try:
                # V3 pools have fee() function
                fee_abi = [{
                    "inputs": [],
                    "name": "fee",
                    "outputs": [{"type": "uint24"}],
                    "stateMutability": "view",
                    "type": "function"
                }]
                
                contract = w3.eth.contract(address=pool_address, abi=fee_abi)
                fee = contract.functions.fee().call()
                logger.info(f"Pool {pool_address} has fee function, detected as V3")
                return "v3"
                
            except Exception:
                pass
            
            try:
                # V3 pools have tickSpacing() function
                tick_spacing_abi = [{
                    "inputs": [],
                    "name": "tickSpacing",
                    "outputs": [{"type": "int24"}],
                    "stateMutability": "view",
                    "type": "function"
                }]
                
                contract = w3.eth.contract(address=pool_address, abi=tick_spacing_abi)
                tick_spacing = contract.functions.tickSpacing().call()
                logger.info(f"Pool {pool_address} has tickSpacing function, detected as V3")
                return "v3"
                
            except Exception:
                pass
            
            # Try V2 specific functions
            try:
                # V2 pools have getReserves() function
                reserves_abi = [{
                    "inputs": [],
                    "name": "getReserves",
                    "outputs": [
                        {"type": "uint112"},
                        {"type": "uint112"},
                        {"type": "uint32"}
                    ],
                    "stateMutability": "view",
                    "type": "function"
                }]
                
                contract = w3.eth.contract(address=pool_address, abi=reserves_abi)
                reserves = contract.functions.getReserves().call()
                logger.info(f"Pool {pool_address} has getReserves function, detected as V2")
                return "v2"
                
            except Exception:
                pass
            
            # If none of the above work, default to V2
            logger.warning(f"Could not detect version for pool {pool_address}, defaulting to V2")
            return "v2"
            
        except Exception as e:
            logger.error(f"Error detecting version for pool {pool_address}: {e}")
            return "v2" 