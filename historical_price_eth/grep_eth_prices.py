#!/usr/bin/env python3
"""
Script para buscar precios de Ethereum usando grep en el archivo de texto
"""

import subprocess
import sys
from datetime import datetime

def grep_eth_price(timestamp_str, file_path='eth_prices_grepable.txt'):
    """
    Busca el precio de ETH usando grep para un timestamp específico
    
    Args:
        timestamp_str: Timestamp en formato ISO (ej: "2025-07-10T21:23:59")
        file_path: Ruta al archivo de precios
    
    Returns:
        float: Precio en USD, o None si no se encuentra
    """
    try:
        # Buscar línea que contenga el timestamp
        cmd = f"grep '{timestamp_str}' {file_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout:
            # Parsear la línea encontrada
            line = result.stdout.strip().split('\n')[0]  # Tomar la primera línea
            timestamp, price = line.split('|')
            return float(price)
        else:
            print(f"❌ No se encontró precio para {timestamp_str}")
            return None
            
    except Exception as e:
        print(f"❌ Error usando grep: {e}")
        return None

def grep_eth_prices_batch(timestamps, file_path='eth_prices_grepable.txt'):
    """
    Busca precios para múltiples timestamps usando grep
    
    Args:
        timestamps: Lista de timestamps en formato ISO
        file_path: Ruta al archivo de precios
    
    Returns:
        dict: {timestamp: price}
    """
    prices = {}
    for ts in timestamps:
        prices[ts] = grep_eth_price(ts, file_path)
    return prices

def grep_eth_price_range(start_timestamp, end_timestamp, file_path='eth_prices_grepable.txt'):
    """
    Busca todos los precios en un rango de fechas usando grep
    
    Args:
        start_timestamp: Timestamp inicial (ISO)
        end_timestamp: Timestamp final (ISO)
        file_path: Ruta al archivo de precios
    
    Returns:
        list: Lista de tuplas (timestamp, price)
    """
    try:
        # Convertir timestamps a formato de fecha para grep
        start_date = datetime.fromisoformat(start_timestamp).strftime('%Y-%m-%d')
        end_date = datetime.fromisoformat(end_timestamp).strftime('%Y-%m-%d')
        
        # Buscar todas las líneas en el rango
        cmd = f"awk -F'|' '$1 >= \"{start_timestamp}\" && $1 <= \"{end_timestamp}\" {{print $0}}' {file_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout:
            prices = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    timestamp, price = line.split('|')
                    prices.append((timestamp, float(price)))
            return prices
        else:
            print(f"❌ No se encontraron precios en el rango {start_timestamp} - {end_timestamp}")
            return []
            
    except Exception as e:
        print(f"❌ Error buscando rango: {e}")
        return []

def main():
    """
    Función principal para probar el script
    """
    print("🔍 BUSCADOR DE PRECIOS ETH CON GREP")
    print("=" * 40)
    
    # Verificar que el archivo existe
    file_path = 'eth_prices_grepable.txt'
    try:
        with open(file_path, 'r') as f:
            first_line = f.readline().strip()
            print(f"✅ Archivo encontrado: {file_path}")
            print(f"📊 Formato: {first_line}")
    except FileNotFoundError:
        print(f"❌ Archivo {file_path} no encontrado")
        print("💡 Ejecuta primero download_eth_history.py para descargar los datos")
        return
    
    # Ejemplos de uso
    print("\n🧪 Probando búsquedas...")
    
    # 1. Búsqueda individual
    timestamp = "2025-07-10T21:23:59"
    price = grep_eth_price(timestamp)
    if price:
        print(f"💰 {timestamp}: ${price}")
    
    # 2. Búsqueda múltiple
    timestamps = [
        "2025-07-10T21:23:59",
        "2025-07-10T21:56:35",
        "2025-07-10T22:15:00"
    ]
    print(f"\n🔍 Buscando {len(timestamps)} timestamps...")
    prices = grep_eth_prices_batch(timestamps)
    for ts, price in prices.items():
        if price:
            print(f"💰 {ts}: ${price}")
    
    # 3. Búsqueda por rango
    print(f"\n📅 Buscando rango de precios...")
    range_prices = grep_eth_price_range("2025-07-10T21:00:00", "2025-07-10T22:00:00")
    print(f"📊 Encontrados {len(range_prices)} precios en el rango")
    for ts, price in range_prices[:5]:  # Mostrar solo los primeros 5
        print(f"💰 {ts}: ${price}")
    if len(range_prices) > 5:
        print(f"... y {len(range_prices) - 5} más")

if __name__ == "__main__":
    main() 