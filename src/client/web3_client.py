from web3 import Web3, HTTPProvider
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
        
        # Add API key to URL (Google Cloud Blockchain Node Engine format)
        rpc_url_with_key = f"{NODE_RPC_URL}?key={NODE_API_KEY}"
        
        # Initialize Web3 with the node's RPC endpoint
        self.w3 = Web3(HTTPProvider(rpc_url_with_key))
        
        # Try to add POA middleware if available (for some networks)
        try:
            from web3.middleware import geth_poa_middleware
            self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        except ImportError:
            # POA middleware not available in this web3 version, skip it
            logger.debug("POA middleware not available, skipping")
        
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
            },
            {
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "stateMutability": "view",
                "type": "function"
            }
        ]
        
        # ERC20 ABI for token operations
        self.ERC20_ABI = [
            {
                "constant": True,
                "inputs": [],
                "name": "totalSupply",
                "outputs": [{"name": "", "type": "uint256"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "balance", "type": "uint256"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "symbol",
                "outputs": [{"name": "", "type": "string"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "name",
                "outputs": [{"name": "", "type": "string"}],
                "type": "function"
            }
        ]
        
        # Uniswap V3 Pool ABI for slot0 and other functions
        self.UNISWAP_V3_POOL_ABI = [
            {
                "inputs": [],
                "name": "slot0",
                "outputs": [
                    {"name": "sqrtPriceX96", "type": "uint160"},
                    {"name": "tick", "type": "int24"},
                    {"name": "observationIndex", "type": "uint16"},
                    {"name": "observationCardinality", "type": "uint16"},
                    {"name": "observationCardinalityNext", "type": "uint16"},
                    {"name": "feeProtocol", "type": "uint8"},
                    {"name": "unlocked", "type": "bool"}
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "liquidity",
                "outputs": [{"name": "", "type": "uint128"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "token0",
                "outputs": [{"name": "", "type": "address"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "token1",
                "outputs": [{"name": "", "type": "address"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "fee",
                "outputs": [{"name": "", "type": "uint24"}],
                "stateMutability": "view",
                "type": "function"
            }
        ]

    def get_logs(self, from_block: int, to_block: int, address: str, topics: List[str]) -> List[Dict]:
        """
        Get logs from the blockchain
        
        Args:
            from_block: Starting block number
            to_block: Ending block number  
            address: Contract address to filter
            topics: List of topics to filter
            
        Returns:
            List of log entries
        """
        try:
            # Convert address to checksum format
            checksum_address = Web3.to_checksum_address(address)
            
            logs = self.w3.eth.get_logs({
                'fromBlock': from_block,
                'toBlock': to_block,
                'address': checksum_address,
                'topics': topics
            })
            
            # Convert HexBytes to regular format for easier processing
            processed_logs = []
            for log in logs:
                processed_log = dict(log)
                # Convert HexBytes to hex strings for JSON serialization
                for key, value in processed_log.items():
                    if isinstance(value, HexBytes):
                        processed_log[key] = value.hex()
                    elif isinstance(value, list):
                        processed_log[key] = [item.hex() if isinstance(item, HexBytes) else item for item in value]
                processed_logs.append(processed_log)
            
            return processed_logs
            
        except Exception as e:
            logger.error(f"Error getting logs: {e}")
            return []

    def get_eth_price_usd(self) -> Optional[float]:
        """
        Get current ETH price in USD from Chainlink oracle
        
        Returns:
            ETH price in USD or None if failed
        """
        try:
            contract = self.w3.eth.contract(
                address=self.CHAINLINK_ETH_USD,
                abi=self.CHAINLINK_ABI
            )
            
            # Get latest price data
            round_data = contract.functions.latestRoundData().call()
            price_raw = round_data[1]  # answer field
            
            # Get decimals to convert properly
            decimals = contract.functions.decimals().call()
            
            # Convert to float with proper decimals
            price_usd = price_raw / (10 ** decimals)
            
            logger.debug(f"Got ETH price from Chainlink: ${price_usd:.2f}")
            return price_usd
            
        except Exception as e:
            logger.error(f"Error getting ETH price from Chainlink: {e}")
            return None

    def get_token_info(self, token_address: str) -> Dict[str, Any]:
        """
        Get comprehensive token information
        
        Args:
            token_address: Token contract address
            
        Returns:
            Dictionary with token info (name, symbol, decimals, totalSupply)
        """
        try:
            # Convert to checksum address
            checksum_address = Web3.to_checksum_address(token_address)
            
            contract = self.w3.eth.contract(
                address=checksum_address,
                abi=self.ERC20_ABI
            )
            
            info = {}
            
            # Get basic token info
            try:
                info['name'] = contract.functions.name().call()
            except:
                info['name'] = 'Unknown'
                
            try:
                info['symbol'] = contract.functions.symbol().call()
            except:
                info['symbol'] = 'UNKNOWN'
                
            try:
                info['decimals'] = contract.functions.decimals().call()
            except:
                info['decimals'] = 18  # Default
                
            try:
                total_supply_raw = contract.functions.totalSupply().call()
                info['totalSupply'] = total_supply_raw
                info['totalSupplyFormatted'] = total_supply_raw / (10 ** info['decimals'])
            except:
                info['totalSupply'] = 0
                info['totalSupplyFormatted'] = 0
                
            info['address'] = token_address.lower()
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting token info for {token_address}: {e}")
            return {
                'name': 'Unknown',
                'symbol': 'UNKNOWN', 
                'decimals': 18,
                'totalSupply': 0,
                'totalSupplyFormatted': 0,
                'address': token_address.lower()
            }

    def get_pool_tvl(self, pool_address: str, token0_address: str, token1_address: str) -> Dict[str, Any]:
        """
        Get pool TVL (Total Value Locked) by reading token balances
        
        Args:
            pool_address: Pool contract address
            token0_address: Token0 address
            token1_address: Token1 address
            
        Returns:
            Dictionary with TVL info
        """
        try:
            # Convert addresses to checksum format
            pool_checksum = Web3.to_checksum_address(pool_address)
            token0_checksum = Web3.to_checksum_address(token0_address)
            token1_checksum = Web3.to_checksum_address(token1_address)
            
            # Get token contracts
            token0_contract = self.w3.eth.contract(address=token0_checksum, abi=self.ERC20_ABI)
            token1_contract = self.w3.eth.contract(address=token1_checksum, abi=self.ERC20_ABI)
            
            # Get balances
            token0_balance_raw = token0_contract.functions.balanceOf(pool_checksum).call()
            token1_balance_raw = token1_contract.functions.balanceOf(pool_checksum).call()
            
            # Get decimals
            token0_decimals = token0_contract.functions.decimals().call()
            token1_decimals = token1_contract.functions.decimals().call()
            
            # Format balances
            token0_balance = token0_balance_raw / (10 ** token0_decimals)
            token1_balance = token1_balance_raw / (10 ** token1_decimals)
            
            return {
                'token0_balance_raw': token0_balance_raw,
                'token1_balance_raw': token1_balance_raw,
                'token0_balance': token0_balance,
                'token1_balance': token1_balance,
                'token0_decimals': token0_decimals,
                'token1_decimals': token1_decimals,
                'pool_address': pool_address.lower()
            }
            
        except Exception as e:
            logger.error(f"Error getting pool TVL for {pool_address}: {e}")
            return {}

    def get_v3_pool_slot0(self, pool_address: str) -> Dict[str, Any]:
        """
        Get Uniswap V3 pool slot0 data (current price, tick, etc.)
        
        Args:
            pool_address: V3 pool address
            
        Returns:
            Dictionary with slot0 data
        """
        try:
            # Convert to checksum address
            checksum_address = Web3.to_checksum_address(pool_address)
            
            contract = self.w3.eth.contract(
                address=checksum_address,
                abi=self.UNISWAP_V3_POOL_ABI
            )
            
            # Get slot0 data
            slot0_data = contract.functions.slot0().call()
            
            # Get current liquidity
            liquidity = contract.functions.liquidity().call()
            
            # Get fee
            fee = contract.functions.fee().call()
            
            return {
                'sqrtPriceX96': slot0_data[0],
                'tick': slot0_data[1],
                'observationIndex': slot0_data[2],
                'observationCardinality': slot0_data[3],
                'observationCardinalityNext': slot0_data[4],
                'feeProtocol': slot0_data[5],
                'unlocked': slot0_data[6],
                'liquidity': liquidity,
                'fee': fee,
                'pool_address': pool_address.lower()
            }
            
        except Exception as e:
            logger.error(f"Error getting V3 pool slot0 for {pool_address}: {e}")
            return {}

    def get_block_timestamp(self, block_number: int) -> Optional[int]:
        """
        Get timestamp for a specific block
        
        Args:
            block_number: Block number
            
        Returns:
            Block timestamp or None if failed
        """
        try:
            block = self.w3.eth.get_block(block_number)
            return block.timestamp
        except Exception as e:
            logger.error(f"Error getting block timestamp for {block_number}: {e}")
            return None

    def get_current_block(self) -> int:
        """Get current block number"""
        return self.w3.eth.block_number

    def call_contract(self, contract_address: str, abi: List[Dict], function_name: str, args: List = None) -> Any:
        """
        Generic contract call
        
        Args:
            contract_address: Contract address
            abi: Contract ABI
            function_name: Function name to call
            args: Function arguments
            
        Returns:
            Function result
        """
        try:
            # Convert to checksum address
            checksum_address = Web3.to_checksum_address(contract_address)
            contract = self.w3.eth.contract(address=checksum_address, abi=abi)
            func = getattr(contract.functions, function_name)
            
            if args:
                return func(*args).call()
            else:
                return func().call()
                
        except Exception as e:
            logger.error(f"Error calling {function_name} on {contract_address}: {e}")
            return None 