from web3 import Web3
from typing import Optional, Dict, Any, List
import json
import logging
from eth_abi import decode_abi
from eth_utils import to_checksum_address

logger = logging.getLogger(__name__)

class Web3NodeClient:
    """
    Cliente para conectarse a un nodo propio de Ethereum
    """
    
    def __init__(self, node_url: str, timeout: int = 30):
        """
        Inicializa el cliente web3
        
        Args:
            node_url: URL del nodo (ej: http://tu-nodo:8545)
            timeout: Timeout en segundos para las peticiones
        """
        self.node_url = node_url
        self.w3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={'timeout': timeout}))
        
        # Verificar conexión
        if not self.w3.is_connected():
            raise ConnectionError(f"No se pudo conectar al nodo en {node_url}")
        
        logger.info(f"Conectado al nodo en {node_url}")
        logger.info(f"Último bloque: {self.w3.eth.block_number}")
    
    def get_latest_block(self) -> int:
        """Obtiene el número del último bloque"""
        return self.w3.eth.block_number
    
    def get_block_info(self, block_number: int) -> Dict[str, Any]:
        """Obtiene información detallada de un bloque"""
        block = self.w3.eth.get_block(block_number, full_transactions=True)
        return {
            'number': block.number,
            'hash': block.hash.hex(),
            'timestamp': block.timestamp,
            'transactions': len(block.transactions),
            'gas_used': block.gasUsed,
            'gas_limit': block.gasLimit
        }
    
    def get_transaction_receipt(self, tx_hash: str) -> Dict[str, Any]:
        """Obtiene el recibo de una transacción"""
        receipt = self.w3.eth.get_transaction_receipt(tx_hash)
        return {
            'transaction_hash': receipt.transactionHash.hex(),
            'block_number': receipt.blockNumber,
            'gas_used': receipt.gasUsed,
            'status': receipt.status,
            'logs': [log.hex() for log in receipt.logs]
        }
    
    def get_transaction(self, tx_hash: str) -> Dict[str, Any]:
        """Obtiene una transacción completa"""
        tx = self.w3.eth.get_transaction(tx_hash)
        return {
            'hash': tx.hash.hex(),
            'from': tx['from'],
            'to': tx['to'],
            'value': tx.value,
            'gas': tx.gas,
            'gas_price': tx.gasPrice,
            'input': tx.input.hex(),
            'block_number': tx.blockNumber
        }
    
    def get_logs(self, 
                 from_block: int, 
                 to_block: int, 
                 address: Optional[str] = None,
                 topics: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Obtiene logs de eventos entre bloques
        
        Args:
            from_block: Bloque inicial
            to_block: Bloque final
            address: Dirección del contrato (opcional)
            topics: Lista de topics para filtrar (opcional)
        """
        filter_params = {
            'fromBlock': from_block,
            'toBlock': to_block
        }
        
        if address:
            filter_params['address'] = to_checksum_address(address)
        
        if topics:
            filter_params['topics'] = topics
        
        logs = self.w3.eth.get_logs(filter_params)
        
        return [{
            'address': log.address,
            'topics': [topic.hex() for topic in log.topics],
            'data': log.data.hex(),
            'block_number': log.blockNumber,
            'transaction_hash': log.transactionHash.hex(),
            'log_index': log.logIndex
        } for log in logs]
    
    def call_contract(self, 
                     contract_address: str, 
                     function_signature: str, 
                     *args) -> Any:
        """
        Llama a una función de un contrato
        
        Args:
            contract_address: Dirección del contrato
            function_signature: Firma de la función (ej: "balanceOf(address)")
            *args: Argumentos de la función
        """
        # Crear función ABI básica
        abi = [{
            "type": "function",
            "name": function_signature.split('(')[0],
            "inputs": [],
            "outputs": [{"type": "uint256"}],
            "stateMutability": "view"
        }]
        
        contract = self.w3.eth.contract(
            address=to_checksum_address(contract_address),
            abi=abi
        )
        
        # Llamar función
        function_name = function_signature.split('(')[0]
        function = getattr(contract.functions, function_name)
        return function(*args).call()
    
    def get_eth_balance(self, address: str) -> int:
        """Obtiene el balance de ETH de una dirección"""
        return self.w3.eth.get_balance(to_checksum_address(address))
    
    def get_token_balance(self, token_address: str, wallet_address: str) -> int:
        """Obtiene el balance de un token ERC20"""
        return self.call_contract(
            token_address,
            "balanceOf(address)",
            wallet_address
        )
    
    def decode_log_data(self, data: str, abi_types: List[str]) -> List[Any]:
        """
        Decodifica datos de logs usando tipos ABI
        
        Args:
            data: Datos hex del log
            abi_types: Lista de tipos ABI (ej: ["uint256", "address"])
        """
        if data.startswith('0x'):
            data = data[2:]
        
        decoded = decode_abi(abi_types, bytes.fromhex(data))
        return decoded
    
    def get_contract_code(self, address: str) -> str:
        """Obtiene el bytecode de un contrato"""
        code = self.w3.eth.get_code(to_checksum_address(address))
        return code.hex()
    
    def is_contract(self, address: str) -> bool:
        """Verifica si una dirección es un contrato"""
        code = self.w3.eth.get_code(to_checksum_address(address))
        return code != b''
    
    def get_gas_price(self) -> int:
        """Obtiene el precio actual del gas"""
        return self.w3.eth.gas_price
    
    def estimate_gas(self, 
                    from_address: str, 
                    to_address: str, 
                    data: str = "0x",
                    value: int = 0) -> int:
        """
        Estima el gas necesario para una transacción
        
        Args:
            from_address: Dirección origen
            to_address: Dirección destino
            data: Datos de la transacción
            value: Valor en wei
        """
        tx_params = {
            'from': to_checksum_address(from_address),
            'to': to_checksum_address(to_address),
            'data': data,
            'value': value
        }
        
        return self.w3.eth.estimate_gas(tx_params) 