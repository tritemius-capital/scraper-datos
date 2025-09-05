from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware
import logging
from typing import Optional, Dict, List, Any, Union
from hexbytes import HexBytes

from ..config import NODE_RPC_URL, NODE_API_KEY

logger = logging.getLogger(__name__)

class Web3Client:
    def __init__(self):
        """Initialize Web3 client with the local node"""
        if not NODE_RPC_URL or not NODE_API_KEY:
            raise ValueError("NODE_RPC_URL and NODE_API_KEY must be set in .env")
        
        # Add API key to headers if needed
        headers = {"Authorization": f"Bearer {NODE_API_KEY}"}
        
        # Initialize Web3 with the node's RPC endpoint
        self.w3 = Web3(HTTPProvider(NODE_RPC_URL, request_kwargs={"headers": headers}))
        
        # Add middleware for POA chains (if needed)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        
        # Test connection
        if not self.w3.is_connected():
            raise ConnectionError("Could not connect to Ethereum node")
        
        logger.info(f"Connected to Ethereum node. Chain ID: {self.w3.eth.chain_id}")
        
        # ETH/USD price oracle contracts (we'll use Chainlink)
        self.CHAINLINK_ETH_USD = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"  # ETH/USD price feed
        
        # Chainlink price feed ABI (simplified)
        self.CHAINLINK_ABI = [
            {
                "inputs": [],
                "name": "latestRoundData",
                "outputs": [
                    {"name": "roundId", "type": "uint80"},
                    {"name": "answer", "type": "int256"},
                    {"name": "startedAt", "type": "uint256"},
                    {"name": "updatedAt", "type": "uint256"},
                    {"name": "answeredInRound", "type": "uint80"}
                ],
                "stateMutability": "view",
                "type": "function"
            }
        ]
    
    def get_latest_block(self) -> int:
        """Get the latest block number"""
        return self.w3.eth.block_number
    
    def get_eth_price_usd(self, block_number: Optional[int] = None) -> float:
        """
        Get ETH price in USD from Chainlink oracle.
        
        Args:
            block_number: Block number for historical price (None for latest)
            
        Returns:
            ETH price in USD
        """
        try:
            # Create contract instance
            price_feed = self.w3.eth.contract(
                address=self.CHAINLINK_ETH_USD,
                abi=self.CHAINLINK_ABI
            )
            
            # Get latest round data
            if block_number:
                # For historical data, we'd need to find the right round
                # For now, just use latest price
                logger.warning(f"Historical ETH price not implemented, using latest price for block {block_number}")
            
            round_data = price_feed.functions.latestRoundData().call()
            
            # Chainlink ETH/USD has 8 decimals
            price = round_data[1] / 10**8
            
            logger.debug(f"ETH price from Chainlink: ${price:.2f}")
            return price
            
        except Exception as e:
            logger.error(f"Error getting ETH price from Chainlink: {e}")
            # Fallback to a reasonable default
            logger.warning("Using fallback ETH price of $3000")
            return 3000.0
    
    def get_logs(self, address: str, from_block: int, to_block: int, topics: List[str]) -> List[Dict[str, Any]]:
        """Get event logs from the node"""
        try:
            filter_params = {
                'address': Web3.to_checksum_address(address),
                'fromBlock': from_block,
                'toBlock': to_block,
                'topics': topics
            }
            
            logs = self.w3.eth.get_logs(filter_params)
            return [dict(log) for log in logs]
            
        except Exception as e:
            logger.error(f"Error getting logs: {e}")
            return []
    
    def get_block_timestamp(self, block_number: int) -> int:
        """Get block timestamp"""
        try:
            block = self.w3.eth.get_block(block_number)
            return block.timestamp
        except Exception as e:
            logger.error(f"Error getting block timestamp: {e}")
            return 0
    
    def get_contract(self, address: str, abi: List[Dict]) -> Any:
        """Get contract instance"""
        return self.w3.eth.contract(
            address=Web3.to_checksum_address(address),
            abi=abi
        )
    
    def decode_log(self, abi: List[Dict], log: Dict[str, Any]) -> Dict[str, Any]:
        """Decode a log entry using the contract ABI"""
        try:
            contract = self.w3.eth.contract(abi=abi)
            
            # Convert topics to HexBytes if they're strings
            if isinstance(log.get('topics', []), list):
                log['topics'] = [
                    HexBytes(topic) if isinstance(topic, str) else topic 
                    for topic in log['topics']
                ]
            
            # Decode the log
            decoded = contract.events.Swap().process_log(log)
            return decoded
            
        except Exception as e:
            logger.error(f"Error decoding log: {e}")
            return {} 