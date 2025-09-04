"""
Uniswap V3 Price Extractor

This module implements the Uniswap V3 specific price extraction logic.
It handles V3's complex swap events with sqrtPriceX96 and ticks.
"""

import logging
from typing import List, Dict, Optional
from tqdm import tqdm
from web3 import Web3
from web3.contract import Contract
import math
import os

from src.uniswap.common.base_extractor import BaseUniswapExtractor
from src.client.etherscan_client import EtherscanClient
from src.client.web3_client import Web3NodeClient
from src.pricing.eth_price_reader import ETHPriceReader


class UniswapV3Extractor(BaseUniswapExtractor):
    """
    Uniswap V3 specific price extractor.
    
    Handles V3's complex swap events with sqrtPriceX96 calculations and ticks.
    """
    
    # Uniswap V3 Pool ABI (minimal for token0/token1/decimals)
    POOL_ABI = [
        {"constant":True,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},
        {"constant":True,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"},
        {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"},
    ]
    
    # ERC20 ABI for token decimals
    ERC20_ABI = [
        {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}
    ]
    
    # Uniswap V3 Swap Event ABI
    SWAP_EVENT_ABI = [
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "name": "sender", "type": "address"},
                {"indexed": True, "name": "recipient", "type": "address"},
                {"indexed": False, "name": "amount0", "type": "int256"},
                {"indexed": False, "name": "amount1", "type": "int256"},
                {"indexed": False, "name": "sqrtPriceX96", "type": "uint160"},
                {"indexed": False, "name": "liquidity", "type": "uint128"},
                {"indexed": False, "name": "tick", "type": "int24"}
            ],
            "name": "Swap",
            "type": "event"
        }
    ]

    def __init__(self, api_key: str, eth_price_file: str = "historical_price_eth/eth_historical_prices_complete.csv", use_node: bool = False):
        """
        Initialize Uniswap V3 extractor.
        
        Args:
            api_key: API key for Etherscan or Archive Node
            eth_price_file: Path to ETH historical prices file
            use_node: If True, use Archive Node instead of Etherscan
        """
        super().__init__(api_key, eth_price_file, use_node)
        
        if use_node:
            node_rpc_url = os.getenv('NODE_RPC_URL')
            node_api_key = os.getenv('NODE_API_KEY')
            self.node_client = Web3NodeClient(node_rpc_url, api_key=node_api_key)
            self.logger.info("Using Archive Node for data extraction")
        else:
            self.etherscan_client = EtherscanClient(api_key)
            self.logger.info("Using Etherscan for data extraction")
            
        self.eth_price_reader = ETHPriceReader(eth_price_file)

    @property
    def w3(self):
        """Web3 instance for blockchain interactions."""
        if self.use_node:
            return self.node_client.w3
        else:
            # Para Etherscan, usar la instancia Web3 heredada del base_extractor
            return self._w3
    
    @w3.setter
    def w3(self, value):
        if self.use_node:
            self.node_client.w3 = value
        else:
            self._w3 = value

    def get_pool_info(self, pool_address: str) -> Dict:
        """
        Get Uniswap V3 pool information (token0, token1, decimals).
        
        Args:
            pool_address: Address of the Uniswap V3 pool
            
        Returns:
            Dictionary with pool information
        """
        try:
            contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(pool_address), 
                abi=self.POOL_ABI
            )
            
            token0 = contract.functions.token0().call()
            token1 = contract.functions.token1().call()
            
            # Get decimals for both tokens
            token0_contract = self.w3.eth.contract(address=token0, abi=self.ERC20_ABI)
            token1_contract = self.w3.eth.contract(address=token1, abi=self.ERC20_ABI)
            decimals0 = token0_contract.functions.decimals().call()
            decimals1 = token1_contract.functions.decimals().call()
            
            return {
                "token0": token0.lower(),
                "token1": token1.lower(), 
                "decimals0": decimals0,
                "decimals1": decimals1
            }
        except Exception as e:
            self.logger.error(f"Error getting V3 pool info for {pool_address}: {e}")
            raise

    def decode_swap_event(self, event_data: str = None, event_topics: List[str] = None, log: Dict = None, pool_info: Dict = None) -> Optional[Dict]:
        """
        Decode Uniswap V3 swap event.
        
        Args:
            event_data: Raw event data (legacy parameter)
            event_topics: Event topics (legacy parameter)
            log: Raw log entry from blockchain (new parameter)
            pool_info: Pool information (new parameter)
            
        Returns:
            Dictionary with decoded event data
        """
        try:
            # Handle both old and new parameter styles
            if log is not None:
                # New style - log dict with pool_info
                event_data = log['data']
                event_topics = log['topics']
            elif event_data is None or event_topics is None:
                return None
            
            # Add missing fields that Etherscan doesn't provide
            log_entry = {
                'data': event_data,
                'topics': event_topics,
                'logIndex': 0,  # Add missing field
                'transactionIndex': 0,  # Add missing field
                'blockHash': '0x0000000000000000000000000000000000000000000000000000000000000000',  # Add missing field
                'blockNumber': 0,  # Add missing field
                'address': '0x0000000000000000000000000000000000000000',  # Add missing field
                'transactionHash': '0x0000000000000000000000000000000000000000000000000000000000000000'  # Add missing field
            }
            
            # Create contract instance for decoding
            contract = self.w3.eth.contract(abi=self.SWAP_EVENT_ABI)
            
            # Process the log to decode the event
            decoded_log = contract.events.Swap().process_log(log_entry)
            
            # Extract the arguments
            args = decoded_log['args']
            
            return {
                'sender': args['sender'],
                'recipient': args['recipient'],
                'amount0': args['amount0'],
                'amount1': args['amount1'],
                'sqrtPriceX96': args['sqrtPriceX96'],
                'liquidity': args['liquidity'],
                'tick': args['tick']
            }
            
        except Exception as e:
            self.logger.error(f"Error decoding V3 swap event: {e}")
            return None

    def calculate_token_price(self, decoded_event: Dict, pool_info: Dict, token_address: str) -> Optional[float]:
        """
        Calculate token price from Uniswap V3 swap event using sqrtPriceX96.
        
        Args:
            decoded_event: Decoded swap event data
            pool_info: Pool information
            token_address: Address of the token to calculate price for
            
        Returns:
            Token price in ETH or None if calculation fails
        """
        try:
            token_address = token_address.lower()
            token0 = pool_info["token0"]
            token1 = pool_info["token1"]
            decimals0 = pool_info["decimals0"]
            decimals1 = pool_info["decimals1"]
            
            sqrtPriceX96 = decoded_event['sqrtPriceX96']
            amount0 = decoded_event['amount0']
            amount1 = decoded_event['amount1']
            
            # Debug logging
            self.logger.debug(f"Token address: {token_address}")
            self.logger.debug(f"Token0: {token0}, Token1: {token1}")
            self.logger.debug(f"Decimals0: {decimals0}, Decimals1: {decimals1}")
            self.logger.debug(f"sqrtPriceX96: {sqrtPriceX96}")
            self.logger.debug(f"amount0: {amount0}, amount1: {amount1}")
            
            # Method 1: Use sqrtPriceX96 calculation
            try:
                # Convert sqrtPriceX96 to price
                # Price = (sqrtPriceX96 / 2^96)^2
                price_ratio = (sqrtPriceX96 / (2**96)) ** 2
                
                # Adjust for token decimals
                decimal_adjustment = (10 ** decimals1) / (10 ** decimals0)
                price_ratio = price_ratio * decimal_adjustment
                
                self.logger.debug(f"Price ratio from sqrtPriceX96: {price_ratio}")
                
                # Calculate price based on which token we're analyzing
                if token_address == token0:
                    # Token is token0, price is in terms of token1
                    price_eth = price_ratio
                elif token_address == token1:
                    # Token is token1, price is in terms of token0
                    price_eth = 1 / price_ratio
                else:
                    # Token not found in this pool - only log once per pool
                    if not hasattr(self, '_token_not_found_logged'):
                        self.logger.warning(f"Token {token_address} not found in pool. Token0: {token0}, Token1: {token1}")
                        self._token_not_found_logged = True
                    return None
                
                # Validate price is reasonable
                if price_eth > 0 and price_eth <= 1e10:
                    self.logger.debug(f"Using sqrtPriceX96 method, price: {price_eth}")
                    return price_eth
                    
            except Exception as e:
                self.logger.debug(f"sqrtPriceX96 method failed: {e}")
            
            # Method 2: Use amount0/amount1 calculation (fallback)
            try:
                # Convert amounts to decimal values
                amount0_decimal = amount0 / (10 ** decimals0)
                amount1_decimal = amount1 / (10 ** decimals1)
                
                self.logger.debug(f"amount0_decimal: {amount0_decimal}, amount1_decimal: {amount1_decimal}")
                
                # Calculate price based on which token we're analyzing
                if token_address == token0:
                    # Token is token0, calculate price in terms of token1
                    if amount0_decimal > 0 and amount1_decimal > 0:
                        price_eth = amount1_decimal / amount0_decimal
                    elif amount0_decimal < 0 and amount1_decimal < 0:
                        # Negative amounts indicate output, so we invert
                        price_eth = abs(amount1_decimal) / abs(amount0_decimal)
                    else:
                        return None
                elif token_address == token1:
                    # Token is token1, calculate price in terms of token0
                    if amount1_decimal > 0 and amount0_decimal > 0:
                        price_eth = amount0_decimal / amount1_decimal
                    elif amount1_decimal < 0 and amount0_decimal < 0:
                        # Negative amounts indicate output, so we invert
                        price_eth = abs(amount0_decimal) / abs(amount1_decimal)
                    else:
                        return None
                else:
                    # Token not found in this pool - only log once per pool
                    if not hasattr(self, '_token_not_found_logged'):
                        self.logger.warning(f"Token {token_address} not found in pool. Token0: {token0}, Token1: {token1}")
                        self._token_not_found_logged = True
                    return None
                
                # Validate price is reasonable
                if price_eth > 0 and price_eth <= 1e10:
                    self.logger.debug(f"Using amount method, price: {price_eth}")
                    return price_eth
                    
            except Exception as e:
                self.logger.debug(f"Amount method failed: {e}")
            
            self.logger.warning("Both price calculation methods failed")
            return None
            
        except Exception as e:
            self.logger.warning(f"Error calculating V3 token price: {e}")
            return None

    def get_swap_events(self, pool_address: str, start_block: int, end_block: int) -> List[Dict]:
        """
        Get swap events for a specific pool and block range
        
        Args:
            pool_address: The pool address to get events for
            start_block: Starting block number
            end_block: Ending block number
            
        Returns:
            List of swap events with decoded data
        """
        if self.use_node:
            # Get pool info for decoding
            pool_info = self.get_pool_info(pool_address)
            
            # Get the V3 swap event signature
            swap_event_signature = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
            
            logs = self.node_client.get_logs(
                from_block=start_block,
                to_block=end_block,
                address=pool_address,
                topics=[swap_event_signature]
            )
            
            # Decode and enrich each event
            enriched_events = []
            for log in logs:
                try:
                    # Decode the event data
                    decoded_event = self.decode_swap_event(log=log, pool_info=pool_info)
                    if decoded_event:
                        # Add decoded data to the original log
                        enriched_log = log.copy()
                        enriched_log.update({
                            'amount0': decoded_event.get('amount0', 0),
                            'amount1': decoded_event.get('amount1', 0),
                            'sqrtPriceX96': decoded_event.get('sqrtPriceX96', 0),
                            'liquidity': decoded_event.get('liquidity', 0),
                            'tick': decoded_event.get('tick', 0),
                            'sender': decoded_event.get('sender', ''),
                            'recipient': decoded_event.get('recipient', ''),
                        })
                        enriched_events.append(enriched_log)
                except Exception as e:
                    self.logger.warning(f"Error enriching swap event: {e}")
                    # Include the original log even if we can't decode it
                    enriched_events.append(log)
            
            return enriched_events
        else:
            return self.etherscan_client.get_swap_events(pool_address, start_block, end_block, version='v3')

    def extract_prices(self, token_address: str, pool_address: str, start_block: int, end_block: int) -> List[Dict]:
        """
        Extract price data from Uniswap V3 swap events.
        
        Args:
            token_address: Token address to analyze
            pool_address: Pool address
            start_block: Starting block number
            end_block: Ending block number
            
        Returns:
            List of price data points
        """
        prices = []
        try:
            self.logger.info(f"Extracting V3 prices for {token_address} from blocks {start_block} to {end_block}")
            
            # Get pool information
            pool_info = self.get_pool_info(pool_address)
            token_address = token_address.lower()
            
            # Get swap events
            swap_events = self.get_swap_events(pool_address, start_block, end_block)
            self.logger.info(f"Found {len(swap_events)} V3 swap events")
            
            for event in tqdm(swap_events, desc="Processing V3 swap events", unit="event"):
                try:
                    # Decode the swap event
                    decoded_event = self.decode_swap_event(log=event, pool_info=pool_info)
                    if not decoded_event:
                        continue
                    
                    # Get block/timestamp
                    if self.use_node:
                        block_number = event['blockNumber']
                        block = self.w3.eth.get_block(block_number)
                        timestamp = block.timestamp
                    else:
                        block_number = int(event['blockNumber'], 16)
                        timestamp = int(event['timeStamp'], 16)
                    
                    # Get ETH price at timestamp
                    eth_price_usd = self.eth_price_reader.get_eth_price_at_timestamp(timestamp)
                    
                    # Calculate token price
                    price_eth = self.calculate_token_price(decoded_event, pool_info, token_address)
                    if price_eth is None:
                        continue
                    
                    # Calculate USD price
                    price_usd = price_eth * eth_price_usd if eth_price_usd else None
                    
                    # Calcular volumen en USD de esta transacción
                    amount0 = decoded_event.get('amount0', 0)
                    amount1 = decoded_event.get('amount1', 0)
                    
                    # Para V3, los amounts pueden ser negativos (salida)
                    eth_volume = max(
                        abs(amount0) / (10 ** pool_info.get('decimals0', 18)),
                        abs(amount1) / (10 ** pool_info.get('decimals1', 18))
                    )
                    usd_volume = eth_volume * eth_price_usd if eth_price_usd else None
                    
                    price_data = {
                        'timestamp': timestamp,
                        'block_number': block_number,
                        'transaction_hash': event.get('transactionHash', ''),
                        
                        # Datos de precio
                        'token_price_eth': price_eth,
                        'token_price_usd': price_usd,
                        'eth_price_usd': eth_price_usd,
                        
                        # Datos de transacción detallados V3
                        'amount0': amount0,
                        'amount1': amount1,
                        'sqrtPriceX96': decoded_event.get('sqrtPriceX96', 0),
                        'liquidity': decoded_event.get('liquidity', 0),
                        'tick': decoded_event.get('tick', 0),
                        'sender': decoded_event.get('sender', ''),
                        'recipient': decoded_event.get('recipient', ''),
                        
                        # Métricas calculadas
                        'eth_volume': eth_volume,
                        'usd_volume': usd_volume,
                        'gas_used': event.get('gasUsed', ''),
                        'gas_price': event.get('gasPrice', ''),
                    }
                    prices.append(price_data)
                    
                except Exception as e:
                    self.logger.warning(f"Error processing V3 event: {e}")
                    continue
            
            self.logger.info(f"Successfully extracted {len(prices)} V3 price points")
            return prices
            
        except Exception as e:
            self.logger.error(f"Error extracting V3 prices: {e}")
            return prices 