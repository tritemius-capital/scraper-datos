import requests
from datetime import datetime
from config import ETHERSCAN_API_KEY

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