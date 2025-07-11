import requests
from src.config import DEXTOOLS_API_KEY

class DEXToolsClient:
    BASE_URL = 'https://api.dextools.io/v1'

    def __init__(self, api_key=None):
        self.api_key = api_key or DEXTOOLS_API_KEY
        self.headers = {
            'accept': 'application/json',
            'X-API-Key': self.api_key
        }

    def get_token_info(self, token_address):
        """Get token information including liquidity data"""
        url = f"{self.BASE_URL}/token/ether/{token_address}/info"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise Exception(f"DEXTools API error: {response.status_code} - {response.text}")
        data = response.json()
        if not data.get('success'):
            raise Exception(f"DEXTools API error: {data.get('message', 'Unknown error')}")
        return data.get('data', {})

    def get_main_pool(self, token_address):
        """Get the main pool for a token"""
        url = f"{self.BASE_URL}/token/ether/{token_address}/pools"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise Exception(f"DEXTools API error: {response.status_code} - {response.text}")
        data = response.json()
        if not data.get('success'):
            raise Exception(f"DEXTools API error: {data.get('message', 'Unknown error')}")
        pools = data.get('data', [])
        # Get the main pool (usually the one with highest liquidity)
        if pools:
            sorted_pools = sorted(pools, key=lambda x: float(x.get('liquidity', {}).get('usd', 0)), reverse=True)
            return sorted_pools[0]
        return None

    def get_liquidity_data(self, token_address):
        """Get liquidity data for a token's main pool"""
        pool = self.get_main_pool(token_address)
        if not pool:
            return {
                'liquidityUSD': 0,
                'poolAddress': None
            }
        return {
            'liquidityUSD': float(pool.get('liquidity', {}).get('usd', 0)),
            'poolAddress': pool.get('address')
        }