from src.uniswap import UniswapExtractorFactory
from src.pricing.object_csv_writer import ObjectCSVWriter
from src.pricing.detailed_csv_writer import DetailedCSVWriter
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

def update_eth_historical_prices():
    """
    Actualiza los precios históricos de ETH ejecutando el script de descarga
    """
    import subprocess
    
    eth_script_path = "historical_price_eth/download_eth_history.py"
    
    if not os.path.exists(eth_script_path):
        print("⚠️  Script de actualización de precios ETH no encontrado")
        return False
    
    try:
        print("🔄 Actualizando precios históricos de ETH...")
        result = subprocess.run(
            ["python3", eth_script_path],
            cwd=os.getcwd(),
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos máximo
        )
        
        if result.returncode == 0:
            print("✅ Precios históricos de ETH actualizados exitosamente")
            return True
        else:
            print(f"❌ Error actualizando precios ETH: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("⏰ Timeout actualizando precios ETH (más de 5 minutos)")
        return False
    except Exception as e:
        print(f"❌ Error ejecutando actualización de precios ETH: {e}")
        return False

def process_token_list(csv_file_path, num_blocks=648000, use_node=False):
    """
    Procesa una lista de tokens desde un archivo CSV
    
    Args:
        csv_file_path: Ruta al archivo CSV con formato: version,nombre,par
        num_blocks: Número de bloques a analizar (por defecto 3 meses)
        use_node: Si usar el nodo Archive o Etherscan
    """
    import pandas as pd
    
    if not os.path.exists(csv_file_path):
        print(f"❌ Archivo CSV no encontrado: {csv_file_path}")
        return False
    
    try:
        # Leer CSV
        df = pd.read_csv(csv_file_path)
        
        # Verificar columnas requeridas
        required_columns = ['version', 'nombre', 'par']
        if not all(col in df.columns for col in required_columns):
            print(f"❌ El CSV debe tener las columnas: {required_columns}")
            print(f"Columnas encontradas: {list(df.columns)}")
            return False
        
        print(f"📋 Procesando {len(df)} tokens del archivo {csv_file_path}")
        
        # Crear directorio para resultados
        results_dir = "data/batch_results"
        os.makedirs(results_dir, exist_ok=True)
        
        # Lista para consolidar todos los resultados
        all_results = []
        
        # Procesar cada token
        for index, row in df.iterrows():
            version = row['version'].strip().lower()
            token_name = row['nombre'].strip()
            token_address = row['nombre'].strip()  # Asumiendo que 'nombre' es la dirección
            pool_address = row['par'].strip()
            
            print(f"\n{'='*60}")
            print(f"📊 Procesando token {index + 1}/{len(df)}")
            print(f"🏷️  Nombre: {token_name}")
            print(f"🔗 Token: {token_address}")
            print(f"🏊 Pool: {pool_address}")
            print(f"🔧 Versión: {version.upper()}")
            print(f"{'='*60}")
            
            try:
                # Extraer datos para este token usando la función batch
                result = extract_token_data_for_batch(
                    token_address=token_address,
                    pool_address=pool_address,
                    num_blocks=num_blocks,
                    uniswap_version=version,
                    use_node=use_node,
                    token_name=token_name
                )
                
                if result:
                    print(f"✅ Token {token_name} procesado exitosamente")
                    all_results.append(result)
                else:
                    print(f"❌ Error procesando token {token_name}")
                    
            except Exception as e:
                print(f"❌ Error procesando token {token_name}: {e}")
                continue
        
        # Crear Excel consolidado
        if all_results:
            print(f"\n📊 Creando Excel consolidado con {len(all_results)} tokens...")
            excel_path = "data/consolidated_token_analysis.xlsx"
            create_consolidated_excel(all_results, excel_path)
        
        print(f"\n🎉 Procesamiento completado!")
        print(f"📁 Resultados individuales en: {results_dir}")
        if all_results:
            print(f"📈 Excel consolidado: data/consolidated_token_analysis.xlsx")
        return True
        
    except Exception as e:
        print(f"❌ Error procesando lista de tokens: {e}")
        return False

def create_consolidated_excel(results_data, output_file="data/consolidated_analysis.xlsx"):
    """
    Crea un archivo Excel consolidado con todos los resultados
    
    Args:
        results_data: Lista de diccionarios con los resultados de cada token
        output_file: Ruta del archivo Excel de salida
    """
    import pandas as pd
    
    if not results_data:
        print("⚠️  No hay datos para consolidar")
        return False
    
    try:
        # Crear DataFrame con los resultados
        df = pd.DataFrame(results_data)
        
        # Crear archivo Excel con múltiples hojas
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Hoja principal con resumen
            df.to_excel(writer, sheet_name='Resumen', index=False)
            
            # Hoja por cada token con datos detallados
            for i, result in enumerate(results_data):
                if 'detailed_prices' in result and result['detailed_prices']:
                    prices_df = pd.DataFrame(result['detailed_prices'])
                    sheet_name = f"Token_{i+1}_{result.get('token_name', 'Unknown')[:10]}"
                    prices_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✅ Excel consolidado creado: {output_file}")
        return True
        
    except Exception as e:
        print(f"❌ Error creando Excel consolidado: {e}")
        return False

def extract_token_data_for_batch(token_address, pool_address, num_blocks=648000, uniswap_version=None, use_node=False, token_name="Unknown"):
    """
    Versión modificada de extract_token_data que retorna los datos en lugar de solo guardar CSV
    """
    print(f"\n=== Extracting data for token {token_name} ({token_address}) ===")
    print(f"Pool: {pool_address}")
    print(f"Blocks to analyze: {num_blocks}")
    
    try:
        # Get API credentials based on source
        if use_node:
            node_api_key = os.getenv('NODE_API_KEY')
            node_rpc_url = os.getenv('NODE_RPC_URL')
            if not all([node_api_key, node_rpc_url]):
                print("Error: NODE_API_KEY and NODE_RPC_URL environment variables not set")
                return None
            api_key = node_api_key
            print("Using Archive Node for data extraction")
        else:
            etherscan_api_key = os.getenv('ETHERSCAN_API_KEY')
            if not etherscan_api_key:
                print("Error: ETHERSCAN_API_KEY environment variable not set")
                return None
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
        
        # Get latest block number and analyze backwards from there
        latest_block = extractor.get_latest_block()
        end_block = latest_block  # Último bloque minado
        start_block = latest_block - num_blocks + 1  # Hacia atrás
        
        print(f"🔗 Último bloque de Ethereum: {latest_block}")
        print(f"📊 Analizando bloques {start_block} to {end_block} (últimos {num_blocks} bloques)")
        
        # Mostrar información temporal
        if num_blocks >= 1000:
            days = num_blocks / 7200  # ~7200 blocks per day
            print(f"⏰ Esto representa aproximadamente {days:.1f} días de datos recientes")
        else:
            hours = num_blocks / 300  # ~300 blocks per hour
            print(f"⏰ Esto representa aproximadamente {hours:.1f} horas de datos recientes")
        
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
            return None
        
        # Preparar datos para retorno
        prices = result.get('prices', [])
        price_stats = result.get('price_stats', {})
        big_buy_analysis = result.get('big_buy_analysis', {})
        
        # Guardar datos detallados usando el nuevo writer
        detailed_writer = DetailedCSVWriter()
        detailed_csv_path = os.path.join("data", f"detailed_{token_name.replace('0x', '')[:10]}.csv")
        
        detailed_writer.save_detailed_transactions(
            prices=prices,
            big_buy_analysis=big_buy_analysis,
            token_address=token_address,
            pool_address=pool_address,
            uniswap_version=detected_version,
            stats=price_stats,
            output_file=detailed_csv_path
        )
        
        # También guardar CSV compacto como antes (para compatibilidad)
        csv_writer = ObjectCSVWriter()
        csv_path = os.path.join("data", f"compact_{token_name.replace('0x', '')[:10]}.csv")
        
        csv_writer.save_prices_to_object_csv(
            prices=prices,
            output_file=csv_path,
            token_address=token_address,
            pool_address=pool_address,
            uniswap_version=detected_version,
            stats=price_stats,
            big_buy_analysis=big_buy_analysis,
            append=False  # Archivo individual, no append
        )
        
        # Calcular métricas adicionales para el resumen
        big_buys_count = len(big_buy_analysis.get('big_buys', []))
        total_big_buy_eth = sum(float(buy.get('ethAmount', 0)) for buy in big_buy_analysis.get('big_buys', []))
        total_big_buy_usd = sum(float(buy.get('usd_value', 0)) for buy in big_buy_analysis.get('big_buys', []) if buy.get('usd_value'))
        avg_big_buy_eth = total_big_buy_eth / max(1, big_buys_count)
        
        # Actualizar resumen del token con métricas adicionales
        token_summary = {
            'token_name': token_name,
            'token_address': token_address,
            'pool_address': pool_address,
            'uniswap_version': detected_version,
            'blocks_analyzed': num_blocks,
            'start_block': start_block,
            'end_block': end_block,
            'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            
            # Métricas de precio
            'total_swaps': price_stats.get('total_swaps', 0),
            'current_price_usd': price_stats.get('current_price_usd'),
            'lowest_price_usd': price_stats.get('lowest_price_usd'),
            'highest_price_usd': price_stats.get('highest_price_usd'),
            'price_change_from_low_pct': f"{price_stats.get('price_change_from_low', 0):.2f}%",
            'price_change_from_high_pct': f"{price_stats.get('price_change_from_high', 0):.2f}%",
            
            # Métricas de big buys
            'big_buys_count': big_buys_count,
            'total_big_buy_eth': f"{total_big_buy_eth:.6f}",
            'total_big_buy_usd': f"{total_big_buy_usd:.2f}",
            'avg_big_buy_eth': f"{avg_big_buy_eth:.6f}",
            'largest_big_buy_eth': f"{max((float(buy.get('ethAmount', 0)) for buy in big_buy_analysis.get('big_buys', [])), default=0):.6f}",
            
            # Archivos generados
            'detailed_csv_file': detailed_csv_path,
            'compact_csv_file': csv_path,
            'summary_csv_file': detailed_csv_path.replace('.csv', '_summary.csv'),
            
            # Datos para Excel (mantener para compatibilidad)
            'detailed_prices': prices,
            'big_buy_analysis': big_buy_analysis
        }
        
        # Show summary
        print(f"\n=== Analysis Complete ===")
        print(f"Total swaps analyzed: {price_stats.get('total_swaps', 0)}")
        print(f"Total ETH volume: {total_big_buy_eth:.6f} ETH")
        print(f"Total USD volume: ${total_big_buy_usd:.2f}")
        
        big_buys_count = len(big_buy_analysis.get('big_buys', []))
        print(f"Big buys found: {big_buys_count}")
        
        if big_buys_count > 0:
            print(f"Average big buy: {avg_big_buy_eth:.6f} ETH")
            print("\nTop Big Buys:")
            big_buys = big_buy_analysis.get('big_buys', [])
            for i, buy in enumerate(big_buys[:5], 1):  # Mostrar solo los primeros 5
                eth_amount = buy.get('ethAmount', 'N/A')
                usd_value = buy.get('usd_value', 'N/A')
                print(f"  {i}. Block {buy.get('blockNumber', 'N/A')} - {eth_amount} ETH (${usd_value})")
            if len(big_buys) > 5:
                print(f"  ... y {len(big_buys) - 5} más")
        
        print(f"\n📁 Archivos generados:")
        print(f"   • Datos detallados: {detailed_csv_path}")
        print(f"   • Resumen: {detailed_csv_path.replace('.csv', '_summary.csv')}")
        print(f"   • Formato compacto: {csv_path}")
        return token_summary
        
    except Exception as e:
        print(f"Error during extraction: {e}")
        return None

def extract_token_data(token_address, pool_address, num_blocks=648000, uniswap_version=None, use_node=False):
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
        
        # Get latest block number and analyze backwards from there
        latest_block = extractor.get_latest_block()
        end_block = latest_block  # Último bloque minado
        start_block = latest_block - num_blocks + 1  # Hacia atrás
        
        print(f"🔗 Último bloque de Ethereum: {latest_block}")
        print(f"📊 Analizando bloques {start_block} to {end_block} (últimos {num_blocks} bloques)")
        
        # Mostrar información temporal
        if num_blocks >= 1000:
            days = num_blocks / 7200  # ~7200 blocks per day
            print(f"⏰ Esto representa aproximadamente {days:.1f} días de datos recientes")
        else:
            hours = num_blocks / 300  # ~300 blocks per hour
            print(f"⏰ Esto representa aproximadamente {hours:.1f} horas de datos recientes")
        
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
    print("This tool will extract price data and analyze big buys for tokens")
    print("Supports both Uniswap V2 and V3 pools")
    
    # Verificar si se pasó un archivo CSV como argumento
    csv_file = None
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        print(f"\n📋 Modo BATCH detectado: {csv_file}")
        print("🔄 Se procesarán todos los tokens del archivo CSV")
        
        if not os.path.exists(csv_file):
            print(f"❌ Archivo CSV no encontrado: {csv_file}")
            return
    else:
        print("\n💡 Modos disponibles:")
        print("   • Modo individual: python3 main.py")
        print("   • Modo batch: python3 main.py lista.csv")
    
    # Actualizar precios históricos de ETH
    print(f"\n{'='*60}")
    print("📈 PASO 1: Actualizando precios históricos de ETH")
    print(f"{'='*60}")
    
    eth_update_success = update_eth_historical_prices()
    if not eth_update_success:
        print("⚠️  Continuando sin actualizar precios ETH (usando datos existentes)")
    
    # Select data source
    print(f"\n{'='*60}")
    print("📡 PASO 2: Seleccionar fuente de datos")
    print(f"{'='*60}")
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
            use_node = True
            break
        elif choice == "2":
            if not os.getenv('ETHERSCAN_API_KEY'):
                print("❌ Etherscan API key not found in .env file")
                print("Please configure ETHERSCAN_API_KEY first")
                return
            print("✅ Using Etherscan API")
            use_node = False
            break
        else:
            print("Invalid choice. Please enter 1 or 2.")
    
    # Procesar según el modo
    if csv_file:
        # Modo BATCH
        print(f"\n{'='*60}")
        print("📊 PASO 3: Procesamiento BATCH")
        print(f"{'='*60}")
        
        success = process_token_list(csv_file, num_blocks=648000, use_node=use_node)
        
        if success:
            print("\n🎉 ¡Procesamiento BATCH completado exitosamente!")
        else:
            print("\n❌ Error en el procesamiento BATCH.")
    else:
        # Modo INDIVIDUAL (código existente)
        print(f"\n{'='*60}")
        print("📊 PASO 3: Análisis individual")
        print(f"{'='*60}")
        
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
        print(f"\n📈 Configuración de análisis:")
        print(f"El análisis siempre comienza desde el último bloque minado de Ethereum")
        print(f"y va hacia atrás en el tiempo según el número de bloques que especifiques.")
        
        while True:
            try:
                num_blocks_input = input("\nEnter number of blocks to analyze (default 648000 - 3 months): ").strip()
                if num_blocks_input == "":
                    num_blocks = 648000  # ~3 months of data (90 days * 7200 blocks/day)
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
            print(f"\n⏰ Analizarás aproximadamente {days:.1f} días de datos más recientes")
        else:
            hours = num_blocks / 300  # ~300 blocks per hour
            print(f"\n⏰ Analizarás aproximadamente {hours:.1f} horas de datos más recientes")
        
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
            use_node=use_node
        )
        
        if success:
            print("\n✅ Analysis completed successfully!")
        else:
            print("\n❌ Analysis failed. Check the error messages above.")

if __name__ == "__main__":
    main()
