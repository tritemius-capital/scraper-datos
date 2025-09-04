from web3 import Web3
from typing import Optional, Dict, Any, List
import json
import logging
import os
import requests
try:
    from eth_abi import decode
except ImportError:
    try:
        from eth_abi import decode_abi as decode
    except ImportError:
        from eth_abi.main import decode_abi as decode
from eth_utils import to_checksum_address

logger = logging.getLogger(__name__)

class Web3NodeClient:
    """
    Cliente para conectarse a un nodo propio de Ethereum
    """
    
    def __init__(self, node_url: str, timeout: int = 120, api_key: Optional[str] = None):
        """
        Initialize Web3 client for Archive Node
        
        Args:
            node_url: Archive node RPC URL
            timeout: Request timeout in seconds (increased for V3)
            api_key: API key for authentication
        """
        self.node_url = node_url
        self.api_key = api_key
        
        # Verificar que la URL tenga el protocolo correcto
        if not node_url.startswith(('http://', 'https://')):
            logger.warning(f"URL del nodo no tiene protocolo, añadiendo https://: {node_url}")
            node_url = f"https://{node_url}"
            self.node_url = node_url
        
        # Para Google Cloud Archive Node, el API key va como query parameter
        if api_key:
            if '?' in node_url:
                node_url = f"{node_url}&key={api_key}"
            else:
                node_url = f"{node_url}?key={api_key}"
        
        logger.info(f"Intentando conectar al nodo: {node_url}")
        if api_key:
            logger.info(f"Usando API key: {api_key[:10]}...")
        
        # Configurar headers básicos
        request_kwargs = {'timeout': timeout}
        request_kwargs['headers'] = {'Content-Type': 'application/json'}
        
        # Primero, verificar conectividad básica HTTP
        try:
            logger.info("Verificando conectividad HTTP básica...")
            test_payload = {
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            }
            
            headers = {'Content-Type': 'application/json'}
            
            response = requests.post(
                node_url,
                json=test_payload,
                headers=headers,
                timeout=timeout
            )
            
            logger.info(f"Respuesta HTTP: Status {response.status_code}")
            if response.status_code != 200:
                logger.error(f"Error HTTP: {response.status_code} - {response.text}")
                raise ConnectionError(f"Error HTTP {response.status_code}: {response.text}")
            
            # Verificar respuesta JSON-RPC
            try:
                json_response = response.json()
                logger.info(f"Respuesta JSON-RPC: {json_response}")
                
                if 'error' in json_response:
                    logger.error(f"Error JSON-RPC: {json_response['error']}")
                    raise ConnectionError(f"Error JSON-RPC: {json_response['error']}")
                
                if 'result' not in json_response:
                    logger.error(f"Respuesta JSON-RPC inválida: {json_response}")
                    raise ConnectionError("Respuesta JSON-RPC inválida")
                    
            except json.JSONDecodeError as e:
                logger.error(f"Error decodificando JSON: {e}")
                logger.error(f"Contenido de respuesta: {response.text[:500]}")
                raise ConnectionError(f"Error decodificando respuesta JSON: {e}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conectividad HTTP: {e}")
            raise ConnectionError(f"Error de conectividad: {e}")
        
        # Si llegamos aquí, la conectividad básica funciona, ahora inicializar Web3
        try:
            self.w3 = Web3(Web3.HTTPProvider(node_url, request_kwargs=request_kwargs))
            
            # Verificar conexión Web3
            if not self.w3.is_connected():
                raise ConnectionError("Web3 no pudo conectarse")
            
            # Probar obtener el último bloque para verificar que funciona
            latest_block = self.w3.eth.block_number
            logger.info(f"✅ Conectado exitosamente al nodo")
            logger.info(f"Último bloque: {latest_block}")
            
        except Exception as e:
            logger.error(f"Error inicializando Web3: {e}")
            raise ConnectionError(f"Error inicializando Web3: {e}")
    
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
            'logs': [
                {
                    'address': log.address,
                    'topics': [topic.hex() for topic in log.topics],
                    'data': log.data.hex(),
                    'block_number': log.blockNumber,
                    'transaction_hash': log.transactionHash.hex(),
                    'log_index': log.logIndex
                }
                for log in receipt.logs
            ]
        }
    
    def get_logs(self, 
                 from_block: int, 
                 to_block: int, 
                 address: Optional[str] = None,
                 topics: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Obtiene logs de eventos de la blockchain
        
        Args:
            from_block: Bloque inicial
            to_block: Bloque final
            address: Dirección del contrato (opcional)
            topics: Topics del evento (opcional)
        """
        # Convertir números de bloque a formato hexadecimal para JSON-RPC
        from_block_hex = hex(from_block) if isinstance(from_block, int) else from_block
        to_block_hex = hex(to_block) if isinstance(to_block, int) else to_block
        
        filter_params = {
            'fromBlock': from_block_hex,
            'toBlock': to_block_hex
        }
        
        if address:
            filter_params['address'] = to_checksum_address(address)
        
        if topics:
            # Asegurar que todos los topics tengan el prefijo 0x
            formatted_topics = []
            for topic in topics:
                if isinstance(topic, str) and not topic.startswith('0x'):
                    formatted_topics.append(f'0x{topic}')
                else:
                    formatted_topics.append(topic)
            filter_params['topics'] = formatted_topics
        
        logger.info(f"Enviando get_logs con parámetros: {filter_params}")
        
        try:
            # Hacer llamada JSON-RPC directa para evitar problemas de conversión de Web3.py
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [filter_params],
                "id": 1
            }
            
            # Usar la URL con API key que ya tenemos configurada
            node_url = self.node_url
            if self.api_key:
                if '?' in node_url:
                    request_url = f"{node_url}&key={self.api_key}"
                else:
                    request_url = f"{node_url}?key={self.api_key}"
            else:
                request_url = node_url
            
            response = requests.post(
                request_url,  # Usar la URL con API key, no self.node_url
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=120  # Aumentado para V3 que tiene más datos
            )
            
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
            
            json_response = response.json()
            
            if 'error' in json_response:
                raise Exception(json_response['error'])
            
            if 'result' not in json_response:
                raise Exception(f"Respuesta inválida: {json_response}")
            
            logs_data = json_response['result']
            logger.info(f"Recibidos {len(logs_data)} logs vía JSON-RPC directo")
            
            # Convertir a formato esperado
            return [{
                'address': log['address'],
                'topics': log['topics'],
                'data': log['data'],
                'blockNumber': int(log['blockNumber'], 16),
                'transactionHash': log['transactionHash'],
                'log_index': int(log['logIndex'], 16)
            } for log in logs_data]
            
        except Exception as e:
            logger.error(f"Error en get_logs: {e}")
            logger.error(f"Parámetros enviados: {filter_params}")
            raise
    
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
        
        decoded = decode(abi_types, bytes.fromhex(data))
        return decoded
    
    def get_contract_code(self, address: str) -> str:
        """Obtiene el código de un contrato"""
        code = self.w3.eth.get_code(to_checksum_address(address))
        return code.hex() 