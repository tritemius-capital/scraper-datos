from src.uniswap import UniswapExtractorFactory
from src.pricing.object_csv_writer import ObjectCSVWriter
import os
import json
from datetime import datetime
import sys
import logging

# Configurar logging para diagnóstico
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_node_connectivity():
    """
    Prueba la conectividad del nodo Archive
    """
    import requests
    
    node_rpc_url = os.getenv('NODE_RPC_URL')
    node_api_key = os.getenv('NODE_API_KEY')
    
    if not all([node_rpc_url, node_api_key]):
        print("❌ NODE_RPC_URL y NODE_API_KEY deben estar configurados")
        return False
    
    print(f"🔍 Probando conectividad con: {node_rpc_url}")
    print(f"🔑 Usando API key: {node_api_key[:10]}...")
    
    try:
        # Verificar que la URL tenga protocolo
        if not node_rpc_url.startswith(('http://', 'https://')):
            node_rpc_url = f"https://{node_rpc_url}"
            print(f"⚠️  Añadiendo protocolo HTTPS: {node_rpc_url}")
        
        # Para Google Cloud Archive Node, el API key va como query parameter
        if '?' in node_rpc_url:
            test_url = f"{node_rpc_url}&key={node_api_key}"
        else:
            test_url = f"{node_rpc_url}?key={node_api_key}"
        
        # Hacer petición de prueba
        test_payload = {
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1
        }
        
        headers = {'Content-Type': 'application/json'}
        
        print("📡 Enviando petición de prueba...")
        response = requests.post(
            test_url,
            json=test_payload,
            headers=headers,
            timeout=30
        )
        
        print(f"📊 Respuesta HTTP: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ Error HTTP {response.status_code}: {response.text}")
            return False
        
        try:
            json_response = response.json()
            print(f"📋 Respuesta JSON: {json_response}")
            
            if 'error' in json_response:
                print(f"❌ Error JSON-RPC: {json_response['error']}")
                return False
            
            if 'result' in json_response:
                block_hex = json_response['result']
                block_number = int(block_hex, 16)
                print(f"✅ Conectividad exitosa! Último bloque: {block_number}")
                return True
            else:
                print(f"❌ Respuesta inesperada: {json_response}")
                return False
                
        except json.JSONDecodeError as e:
            print(f"❌ Error decodificando JSON: {e}")
            print(f"Contenido: {response.text[:200]}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error de conectividad: {e}")
        return False
    
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False

def extract_token_data(token_address, pool_address, num_blocks=1000, uniswap_version=None, use_node=False):
    """
    Extract price data and analyze big buys for a token using the unified Uniswap system
    """
    print(f"\n=== Extracting data for token {token_address} ===")
    print(f"Pool: {pool_address}")
    print(f"Blocks to analyze: {num_blocks}")
    
    try:
        # Get API credentials based on source
        if use_node:
            node_api_key = os.getenv('NODE_API_KEY')
            node_rpc_url = os.getenv('NODE_RPC_URL')
            if not all([node_api_key, node_rpc_url]):
                print("Error: NODE_API_KEY and NODE_RPC_URL environment variables not set")
                return False
            api_key = node_api_key
            print("Using Archive Node for data extraction")
        else:
            etherscan_api_key = os.getenv('ETHERSCAN_API_KEY')
            if not etherscan_api_key:
                print("Error: ETHERSCAN_API_KEY environment variable not set")
                return False
            api_key = etherscan_api_key
            print("Using Etherscan API for data extraction")
        
        # Create factory and extractor
        factory = UniswapExtractorFactory()
        
        if uniswap_version:
            # Use specified version
            print(f"Using Uniswap {uniswap_version.upper()}")
            extractor = factory.create_extractor(uniswap_version, api_key, use_node=use_node)
            detected_version = uniswap_version
        else:
            # Auto-detect version
            print("Auto-detecting Uniswap version...")
            extractor = factory.create_auto_extractor(pool_address, api_key, use_node=use_node)
            detected_version = factory.detect_version_from_pool(pool_address) or "v2"
            print(f"Detected Uniswap {detected_version.upper()}")
        
        # Get latest block number
        latest_block = extractor.get_latest_block()
        start_block = latest_block - num_blocks + 1
        end_block = latest_block
        
        print(f"Analyzing blocks {start_block} to {end_block}")
        
        # Extract price data and analyze big buys
        print("\nExtracting swap events and analyzing big buys...")
        result = extractor.analyze_token_complete(
            token_address=token_address,
            pool_address=pool_address,
            start_block=start_block,
            end_block=end_block,
            threshold_eth=0.1
        )
        
        if not result or result.get('error'):
            print(f"No data found or error: {result.get('error', 'Unknown error')}")
            return False
            
        # Save to CSV
        csv_writer = ObjectCSVWriter()
        csv_path = os.path.join("data", "token_analysis.csv")
        
        # Save using the existing method with append=True
        csv_writer.save_prices_to_object_csv(
            prices=result.get('prices', []),
            output_file=csv_path,
            token_address=token_address,
            pool_address=pool_address,
            uniswap_version=detected_version,
            stats=result.get('price_stats', {}),
            big_buy_analysis=result.get('big_buy_analysis', {}),
            append=True  # Append to existing file instead of overwriting
        )
        
        # Show summary
        print(f"\n=== Analysis Complete ===")
        print(f"Total swaps analyzed: {result.get('price_stats', {}).get('total_swaps', 0)}")
        
        big_buy_analysis = result.get('big_buy_analysis', {})
        big_buys_count = len(big_buy_analysis.get('big_buys', []))
        print(f"Big buys found: {big_buys_count}")
        
        if big_buys_count > 0:
            print("\nBig Buy Details:")
            big_buys = big_buy_analysis.get('big_buys', [])
            for i, buy in enumerate(big_buys, 1):
                print(f"  {i}. Block {buy.get('blockNumber', 'N/A')} - {buy.get('ethAmount', 'N/A')} ETH")
        
        print(f"\nData saved to: {csv_path}")
        return True
        
    except Exception as e:
        print(f"Error during extraction: {e}")
        return False

def main():
    print("=== Ethereum Token Price Extractor & Big Buy Analyzer ===")
    print("This tool will extract price data and analyze big buys for a token")
    print("Supports both Uniswap V2 and V3 pools")
    
    # Select data source
    print("\n📡 Select data source:")
    print("1. Archive Node (recommended)")
    print("2. Etherscan API (fallback)")
    
    while True:
        choice = input("\nEnter your choice (1/2): ").strip()
        if choice == "1":
            if not all([os.getenv('NODE_API_KEY'), os.getenv('NODE_RPC_URL')]):
                print("❌ Archive node configuration not found in .env file")
                print("Please configure NODE_API_KEY and NODE_RPC_URL first")
                return
            
            # Probar conectividad del nodo Archive
            print("\n🔧 Verificando conectividad del nodo Archive...")
            if not test_node_connectivity():
                print("❌ No se pudo conectar al nodo Archive")
                print("💡 Sugerencias:")
                print("   1. Verifica que las URLs tengan https:// al principio")
                print("   2. Verifica que el API key sea correcto")
                print("   3. Verifica que el nodo esté funcionando")
                print("   4. Prueba usar Etherscan como alternativa (opción 2)")
                return
            
            print("✅ Using Archive Node")
            break
        elif choice == "2":
            if not os.getenv('ETHERSCAN_API_KEY'):
                print("❌ Etherscan API key not found in .env file")
                print("Please configure ETHERSCAN_API_KEY first")
                return
            print("✅ Using Etherscan API")
            break
        else:
            print("Invalid choice. Please enter 1 or 2.")
    
    # Get token address
    token_address = input("\nEnter the token address (0x...): ").strip()
    if not token_address.startswith("0x") or len(token_address) != 42:
        print("Invalid token address format. Must be a 42-character hex string starting with 0x.")
        return
    
    # Get pool address
    pool_address = input("Enter the Uniswap pool address (0x...): ").strip()
    if not pool_address.startswith("0x") or len(pool_address) != 42:
        print("Invalid pool address format. Must be a 42-character hex string starting with 0x.")
        return
    
    # Get Uniswap version preference
    version_input = input("Enter Uniswap version (v2/v3) or press Enter for auto-detect: ").strip().lower()
    uniswap_version = None
    if version_input in ['v2', 'v3']:
        uniswap_version = version_input
        print(f"Using Uniswap {version_input.upper()}")
        print("⚠️  Note: Make sure the pool address matches the specified version!")
        print("   If unsure, press Enter for auto-detection.")
    elif version_input == "":
        print("Auto-detecting Uniswap version...")
    else:
        print("Invalid version. Auto-detecting...")
    
    # Get number of blocks
    while True:
        try:
            num_blocks_input = input("Enter number of blocks to analyze (default 15000): ").strip()
            if num_blocks_input == "":
                num_blocks = 15000
            else:
                num_blocks = int(num_blocks_input)
                if num_blocks <= 0:
                    print("Number of blocks must be positive.")
                    continue
            break
        except ValueError:
            print("Please enter a valid number.")
    
    # Show time estimate
    if num_blocks >= 1000:
        days = num_blocks / 7200  # ~7200 blocks per day
        print(f"\nTime estimate: ~{days:.1f} days of blockchain data")
    else:
        hours = num_blocks / 300  # ~300 blocks per hour
        print(f"\nTime estimate: ~{hours:.1f} hours of blockchain data")
    
    # Confirm and proceed
    confirm = input(f"\nProceed with analysis? (y/n): ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("Analysis cancelled.")
        return
    
    # Create data directory
    os.makedirs("data", exist_ok=True)
    
    # Extract data
    success = extract_token_data(
        token_address=token_address,
        pool_address=pool_address,
        num_blocks=num_blocks,
        uniswap_version=uniswap_version,
        use_node=(choice == "1")  # True if using Archive Node
    )
    
    if success:
        print("\n✅ Analysis completed successfully!")
    else:
        print("\n❌ Analysis failed. Check the error messages above.")

if __name__ == "__main__":
    main()
