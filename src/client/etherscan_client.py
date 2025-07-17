import requests
from datetime import datetime
from src.config import ETHERSCAN_API_KEY

class EtherscanClient:
    BASE_URL = 'https://api.etherscan.io/api'

    def __init__(self, api_key=None):
        self.api_key = api_key or ETHERSCAN_API_KEY

    def get_token_transactions(self, token_address, limit=50):
        """Get the latest transactions for a token"""
        params = {
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': token_address,
            'page': 1,
            'offset': limit,
            'sort': 'desc',
            'apikey': self.api_key
        }
        response = requests.get(self.BASE_URL, params=params)
        if response.status_code != 200:
            raise Exception(f"Etherscan API error: {response.status_code} - {response.text}")
        data = response.json()
        if data['status'] != '1':
            raise Exception(f"Etherscan API error: {data['message']}")
        return data['result']

    def get_logs(self, address, from_block, to_block, topic0=None, topic1=None, topic2=None, topic3=None):
        """
        Get event logs from a contract address.
        
        Args:
            address: Contract address
            from_block: Starting block number
            to_block: Ending block number
            topic0: Event signature (optional)
            topic1: First indexed parameter (optional)
            topic2: Second indexed parameter (optional)
            topic3: Third indexed parameter (optional)
        """
        params = {
            'module': 'logs',
            'action': 'getLogs',
            'address': address,
            'fromBlock': from_block,
            'toBlock': to_block,
            'apikey': self.api_key
        }
        
        # Add topics if provided
        if topic0:
            params['topic0'] = topic0
        if topic1:
            params['topic1'] = topic1
        if topic2:
            params['topic2'] = topic2
        if topic3:
            params['topic3'] = topic3
        
        response = requests.get(self.BASE_URL, params=params)
        if response.status_code != 200:
            raise Exception(f"Etherscan API error: {response.status_code} - {response.text}")
        
        data = response.json()
        if data['status'] != '1':
            raise Exception(f"Etherscan API error: {data['message']}")
        
        return data['result']

    def get_swap_events(self, pool_address, from_block, to_block, batch_size=1000, version='v2'):
        """
        Get Swap events from a Uniswap pool (V2 or V3).
        
        Args:
            pool_address: Pool contract address
            from_block: Starting block number
            to_block: Ending block number
            batch_size: Number of blocks per request (max 1000 for free tier)
            version: Uniswap version ('v2' or 'v3')
        """
        from tqdm import tqdm
        
        # Swap event signatures for different Uniswap versions
        swap_topics = {
            'v2': "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
            'v3': "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
        }
        
        if version not in swap_topics:
            raise ValueError(f"Unsupported Uniswap version: {version}. Supported: {list(swap_topics.keys())}")
        
        swap_topic = swap_topics[version]
        
        all_logs = []
        current_block = from_block
        total_blocks = to_block - from_block + 1
        
        # Create progress bar
        with tqdm(total=total_blocks, desc="🔍 Scanning blocks", unit="blocks") as pbar:
            while current_block <= to_block:
                end_block = min(current_block + batch_size - 1, to_block)
                blocks_in_batch = end_block - current_block + 1
                
                try:
                    logs = self.get_logs(
                        address=pool_address,
                        from_block=current_block,
                        to_block=end_block,
                        topic0=swap_topic
                    )
                    
                    if logs:
                        all_logs.extend(logs)
                        # Update progress bar with found events
                        pbar.set_postfix({
                            'events': len(all_logs),
                            'current': f"{current_block}-{end_block}"
                        })
                    else:
                        # Update progress bar for empty blocks
                        pbar.set_postfix({
                            'events': len(all_logs),
                            'current': f"{current_block}-{end_block} (empty)"
                        })
                    
                    current_block = end_block + 1
                    pbar.update(blocks_in_batch)
                    
                except Exception as e:
                    # Only show error if it's not "No records found" (which is normal)
                    if "No records found" not in str(e):
                        print(f"⚠️  Warning: Error fetching blocks {current_block}-{end_block}: {e}")
                    else:
                        # Update progress bar for empty blocks (no records found is normal)
                        pbar.set_postfix({
                            'events': len(all_logs),
                            'current': f"{current_block}-{end_block} (no events)"
                        })
                    
                    current_block = end_block + 1
                    pbar.update(blocks_in_batch)
                    continue
        
        return all_logs

    def get_swap_events_v2(self, pool_address, from_block, to_block, batch_size=1000):
        """Get Swap events from a Uniswap V2 pool (backward compatibility)."""
        return self.get_swap_events(pool_address, from_block, to_block, batch_size, 'v2')

    def get_swap_events_v3(self, pool_address, from_block, to_block, batch_size=1000):
        """Get Swap events from a Uniswap V3 pool."""
        return self.get_swap_events(pool_address, from_block, to_block, batch_size, 'v3')

    def get_latest_block(self):
        """Get the latest block number."""
        params = {
            'module': 'proxy',
            'action': 'eth_blockNumber',
            'apikey': self.api_key
        }
        
        response = requests.get(self.BASE_URL, params=params)
        if response.status_code != 200:
            raise Exception(f"Etherscan API error: {response.status_code} - {response.text}")
        
        data = response.json()
        if 'result' not in data:
            raise Exception(f"Etherscan API error: {data}")
        
        # Convert hex to decimal
        return int(data['result'], 16)

    def format_transactions(self, transactions):
        """Format transaction data into a standardized format"""
        formatted_txs = []
        for tx in transactions:
            formatted_tx = {
                'hash': tx.get('hash'),
                'from': tx.get('from'),
                'to': tx.get('to'),
                'valueETH': float(int(tx.get('value', '0')) / 10**18),
                'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', '0'))).isoformat(),
                'tokenSymbol': tx.get('tokenSymbol'),
                'tokenName': tx.get('tokenName'),
                'tokenDecimal': tx.get('tokenDecimal'),
                'gas': tx.get('gas'),
                'gasPrice': tx.get('gasPrice'),
                'gasUsed': tx.get('gasUsed')
            }
            formatted_txs.append(formatted_tx)
        return formatted_txs
        