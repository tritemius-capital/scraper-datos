"""
Uniswap V2 Price Extractor

This module implements the Uniswap V2 specific price extraction logic.
It handles V2's simple swap events and constant product formula.
"""

import logging
from typing import List, Dict, Optional
from tqdm import tqdm
from web3 import Web3
from web3.contract import Contract
import os

from src.uniswap.common.base_extractor import BaseUniswapExtractor
from src.client.etherscan_client import EtherscanClient
from src.client.web3_client import Web3NodeClient
from src.pricing.eth_price_reader import ETHPriceReader


class UniswapV2Extractor(BaseUniswapExtractor):
    """
    Uniswap V2 specific price extractor.
    
    Handles V2's simple swap events with direct amount calculations.
    """
    
    # Uniswap V2 Pair ABI (minimal for token0/token1/decimals)
    PAIR_ABI = [
        {"constant":True,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},
        {"constant":True,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"},
        {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"},
    ]
    
    # ERC20 ABI for token decimals
    ERC20_ABI = [
        {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}
    ]
    
    # Uniswap V2 Swap Event ABI
    SWAP_EVENT_ABI = [
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "name": "sender", "type": "address"},
                {"indexed": False, "name": "amount0In", "type": "uint256"},
                {"indexed": False, "name": "amount1In", "type": "uint256"},
                {"indexed": False, "name": "amount0Out", "type": "uint256"},
                {"indexed": False, "name": "amount1Out", "type": "uint256"},
                {"indexed": True, "name": "to", "type": "address"}
            ],
            "name": "Swap",
            "type": "event"
        }
    ]

    def __init__(self, api_key: str, eth_price_file: str = "historical_price_eth/eth_historical_prices_complete.csv", use_node: bool = False):
        """
        Initialize Uniswap V2 extractor.
        
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
        Get Uniswap V2 pool information (token0, token1, decimals).
        
        Args:
            pool_address: Address of the Uniswap V2 pool
            
        Returns:
            Dictionary with pool information
        """
        try:
            contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(pool_address), 
                abi=self.PAIR_ABI
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
            self.logger.error(f"Error getting pool info for {pool_address}: {e}")
            raise

    def decode_swap_event(self, event_data: str, event_topics: List[str]) -> Optional[Dict]:
        """
        Decode Uniswap V2 swap event.
        
        Args:
            event_data: Raw event data
            event_topics: Event topics
            
        Returns:
            Decoded swap event data or None if decoding fails
        """
        try:
            # Create contract instance for decoding
            contract = self.w3.eth.contract(abi=self.SWAP_EVENT_ABI)
            
            # Add missing fields that web3 expects
            log_entry = {
                'data': event_data,
                'topics': event_topics,
                'logIndex': 0,  # Add dummy logIndex
                'transactionIndex': 0,  # Add dummy transactionIndex
                'transactionHash': '0x' + '0' * 64,  # Add dummy transaction hash
                'blockHash': '0x' + '0' * 64,  # Add dummy block hash
                'blockNumber': 0,  # Add dummy block number
                'address': '0x' + '0' * 40,  # Add dummy address
                'removed': False  # Add removed flag
            }
            
            # Decode the event
            decoded_log = contract.events.Swap().process_log(log_entry)
            
            return {
                'amount0In': decoded_log['args']['amount0In'],
                'amount1In': decoded_log['args']['amount1In'],
                'amount0Out': decoded_log['args']['amount0Out'],
                'amount1Out': decoded_log['args']['amount1Out'],
                'sender': decoded_log['args']['sender'],
                'to': decoded_log['args']['to']
            }
        except Exception as e:
            self.logger.warning(f"Error decoding V2 swap event: {e}")
            return None

    def calculate_token_price(self, decoded_event: Dict, pool_info: Dict, token_address: str) -> Optional[float]:
        """
        Calculate token price from Uniswap V2 swap event.
        
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
            
            # Get raw amounts
            a0in = decoded_event['amount0In'] / (10 ** decimals0)
            a1in = decoded_event['amount1In'] / (10 ** decimals1)
            a0out = decoded_event['amount0Out'] / (10 ** decimals0)
            a1out = decoded_event['amount1Out'] / (10 ** decimals1)
            
            # Calculate price based on which token we're analyzing
            if token_address == token0:
                # Token is token0, calculate price in terms of token1
                if a0in > 0 and a1out > 0:
                    price_eth = a1out / a0in
                elif a1in > 0 and a0out > 0:
                    price_eth = a1in / a0out
                else:
                    return None
            elif token_address == token1:
                # Token is token1, calculate price in terms of token0
                if a1in > 0 and a0out > 0:
                    price_eth = a0out / a1in
                elif a0in > 0 and a1out > 0:
                    price_eth = a0in / a1out
                else:
                    return None
            else:
                # Token not found in this pool
                return None
            
            return price_eth
            
        except Exception as e:
            self.logger.warning(f"Error calculating V2 token price: {e}")
            return None

    def get_swap_events(self, pool_address: str, start_block: int, end_block: int) -> List[Dict]:
        """
        Get Uniswap V2 swap events from Etherscan or Archive Node.
        
        Args:
            pool_address: Pool address
            start_block: Starting block number
            end_block: Ending block number
            
        Returns:
            List of raw swap events
        """
        try:
            if self.use_node:
                # Use Archive Node
                # Create contract instance
                contract = self.w3.eth.contract(
                    address=Web3.to_checksum_address(pool_address),
                    abi=self.SWAP_EVENT_ABI
                )
                
                # Get swap event signature
                swap_event_signature = self.w3.keccak(
                    text="Swap(address,uint256,uint256,uint256,uint256,address)"
                ).hex()
                
                # Get logs from node
                logs = self.node_client.get_logs(
                    from_block=start_block,
                    to_block=end_block,
                    address=pool_address,
                    topics=[swap_event_signature]
                )
                
                return logs
            else:
                # Use Etherscan
                return self.etherscan_client.get_swap_events(pool_address, start_block, end_block, version='v2')
                
        except Exception as e:
            self.logger.error(f"Error getting V2 swap events: {e}")
            return []

    def extract_prices(self, token_address: str, pool_address: str, start_block: int, end_block: int) -> List[Dict]:
        """
        Extract price data from Uniswap V2 swap events.
        
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
            self.logger.info(f"Extracting V2 prices for {token_address} from blocks {start_block} to {end_block}")
            
            # Get pool information
            pool_info = self.get_pool_info(pool_address)
            token_address = token_address.lower()
            
            # Get swap events
            swap_events = self.get_swap_events(pool_address, start_block, end_block)
            self.logger.info(f"Found {len(swap_events)} V2 swap events")
            
            for event in tqdm(swap_events, desc="Processing V2 swap events", unit="event"):
                try:
                    # Decode the swap event
                    decoded_event = self.decode_swap_event(event['data'], event['topics'])
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
                    
                    price_data = {
                        'timestamp': timestamp,
                        'block_number': block_number,
                        'token_price_eth': price_eth,
                        'token_price_usd': price_usd,
                        'eth_price_usd': eth_price_usd
                    }
                    prices.append(price_data)
                    
                except Exception as e:
                    self.logger.warning(f"Error processing V2 event: {e}")
                    continue
            
            self.logger.info(f"Successfully extracted {len(prices)} V2 price points")
            return prices
            
        except Exception as e:
            self.logger.error(f"Error extracting V2 prices: {e}")
            return prices 