"""
Uniswap Price Extraction Package

This package provides unified interfaces for extracting price data from both
Uniswap V2 and V3 pools. It automatically handles the differences between
versions and provides a consistent API.

Usage:
    from src.uniswap import UniswapExtractorFactory
    
    factory = UniswapExtractorFactory()
    extractor = factory.create_auto_extractor(pool_address, etherscan_api_key)
    prices = extractor.extract_prices(token_address, pool_address, start_block, end_block)
"""

from .factory import UniswapExtractorFactory
from .v2.extractor import UniswapV2Extractor
from .v3.extractor import UniswapV3Extractor
from .common.base_extractor import BaseUniswapExtractor

__all__ = [
    'UniswapExtractorFactory',
    'UniswapV2Extractor', 
    'UniswapV3Extractor',
    'BaseUniswapExtractor'
] 