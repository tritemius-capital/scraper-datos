"""
Event Decoder Module

Handles decoding of Uniswap V2 swap events from raw log data.
"""

import logging
from typing import List, Dict, Optional
from web3 import Web3


class EventDecoder:
    """Decodes Uniswap V2 swap events from raw log data."""
    
    # Uniswap V2 Pair ABI (minimal for swap events)
    PAIR_ABI = [
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
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.w3 = Web3()
    
    def decode_swap_event(self, log_data: str, topics: List[str]) -> Optional[Dict]:
        """
        Decode a swap event from raw log data.
        
        Args:
            log_data: Raw log data
            topics: Event topics
            
        Returns:
            Decoded swap data or None if invalid
        """
        try:
            # Create contract instance for decoding
            contract = self.w3.eth.contract(
                address=self.w3.to_checksum_address("0x0000000000000000000000000000000000000000"), 
                abi=self.PAIR_ABI
            )
            
            # Create log object with all required fields for Web3.py
            log_object = {
                'address': '0x0000000000000000000000000000000000000000',
                'topics': topics,
                'data': log_data,
                'logIndex': 0,
                'blockNumber': 0,
                'transactionIndex': 0,
                'transactionHash': '0x0000000000000000000000000000000000000000000000000000000000000000',
                'blockHash': '0x0000000000000000000000000000000000000000000000000000000000000000',
                'removed': False
            }
            
            # Decode the event
            decoded_log = contract.events.Swap().process_log(log_object)
            
            return {
                'sender': decoded_log['args']['sender'],
                'amount0In': decoded_log['args']['amount0In'],
                'amount1In': decoded_log['args']['amount1In'],
                'amount0Out': decoded_log['args']['amount0Out'],
                'amount1Out': decoded_log['args']['amount1Out'],
                'to': decoded_log['args']['to']
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to decode swap event: {e}")
            return None 