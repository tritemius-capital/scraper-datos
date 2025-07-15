#!/usr/bin/env python3
"""
Script para descargar el histórico de precios de Ethereum de los últimos 2 años
Usa CryptoCompare API para obtener datos por hora desde hace 2 años hasta hoy
"""

import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import os
import json

def download_eth_historical_data():
    """
    Descarga el histórico de precios de ETH de los últimos 2 años (datos por hora)
    """
    print("🚀 Iniciando descarga del histórico de precios de Ethereum (últimos 2 años, por hora)...")
    
    # Calcular timestamp de hace 2 años
    two_years_ago = datetime.now() - timedelta(days=2*365)
    start_timestamp = int(two_years_ago.timestamp())
    current_ts = int(datetime.now().timestamp())
    
    print(f"📅 Descargando datos desde {datetime.fromtimestamp(start_timestamp)} hasta {datetime.fromtimestamp(current_ts)}")
    print(f"📊 Período: Últimos 2 años (~730 días)")
    print(f"⏰ Frecuencia: Datos por hora")
    
    # Configuración - usar histohour en lugar de histominute
    base_url = "https://min-api.cryptocompare.com/data/v2/histohour"
    all_data = []
    total_calls = 0
    
    while current_ts > start_timestamp:
        params = {
            'fsym': 'ETH',
            'tsym': 'USD',
            'limit': 2000,  # Máximo por llamada (2000 horas = ~83 días)
            'aggregate': 1,  # 1 hora
            'toTs': current_ts
        }
        
        try:
            print(f"📊 Llamada #{total_calls + 1}: Descargando datos hasta {datetime.fromtimestamp(current_ts)}")
            response = requests.get(base_url, params=params)
            
            if response.status_code != 200:
                print(f"❌ Error en la API: {response.status_code}")
                break
                
            data = response.json()
            
            if not data['Data']['Data']:
                print("✅ No hay más datos históricos disponibles")
                break
                
            batch_data = data['Data']['Data']
            
            # Filtrar solo datos de los últimos 2 años
            filtered_data = [tx for tx in batch_data if tx['time'] >= start_timestamp]
            all_data.extend(filtered_data)
            
            # Actualizar timestamp para la siguiente llamada
            current_ts = batch_data[0]['time'] - 1
            total_calls += 1
            
            print(f"   ✅ Descargados {len(filtered_data)} registros (Total: {len(all_data)})")
            
            # Pausa para no sobrecargar la API
            time.sleep(0.1)
            
            # Si ya tenemos suficientes datos, parar
            if current_ts < start_timestamp:
                print("✅ Llegamos al límite de 2 años, descarga completa")
                break
                
        except Exception as e:
            print(f"❌ Error durante la descarga: {e}")
            print(f"   Detalles del error: {str(e)}")
            break
    
    print(f"\n📈 Descarga completada!")
    print(f"   📊 Total de registros: {len(all_data)}")
    if all_data:
        print(f"   📅 Rango temporal: {datetime.fromtimestamp(all_data[-1]['time'])} - {datetime.fromtimestamp(all_data[0]['time'])}")
    print(f"   🔄 Total de llamadas API: {total_calls}")
    
    return all_data

def process_and_save_data(all_data):
    """
    Procesa los datos descargados y los guarda en diferentes formatos
    """
    print("\n🔄 Procesando datos...")
    
    # Convertir a DataFrame
    df = pd.DataFrame(all_data)
    
    # Agregar columnas de fecha legibles
    df['datetime'] = pd.to_datetime(df['time'], unit='s')
    df['date'] = df['datetime'].dt.date
    df['hour'] = df['datetime'].dt.hour
    df['day'] = df['datetime'].dt.day
    df['month'] = df['datetime'].dt.month
    df['year'] = df['datetime'].dt.year
    
    # Reordenar columnas
    columns_order = ['time', 'datetime', 'date', 'year', 'month', 'day', 'hour', 'close', 'high', 'low', 'open', 'volumefrom', 'volumeto']
    df = df[columns_order]
    
    # Guardar en diferentes formatos
    print("💾 Guardando archivos...")
    
    # 1. CSV completo
    csv_path = 'eth_historical_prices_complete.csv'
    df.to_csv(csv_path, index=False)
    print(f"   ✅ CSV completo: {csv_path} ({len(df)} registros)")
    
    # 2. CSV optimizado para grep (solo timestamp y precio)
    grep_df = df[['datetime', 'close']].copy()
    grep_df['datetime_iso'] = grep_df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    grep_csv_path = 'eth_prices_grepable.csv'
    grep_df[['datetime_iso', 'close']].to_csv(grep_csv_path, index=False, header=False)
    print(f"   ✅ CSV para grep: {grep_csv_path}")
    
    # 3. JSON para uso programático
    json_path = 'eth_historical_prices.json'
    with open(json_path, 'w') as f:
        json.dump(all_data, f, indent=2)
    print(f"   ✅ JSON: {json_path}")
    
    # 4. Archivo de texto simple para grep
    txt_path = 'eth_prices_grepable.txt'
    with open(txt_path, 'w') as f:
        for _, row in df.iterrows():
            timestamp = row['datetime'].strftime('%Y-%m-%dT%H:%M:%S')
            f.write(f"{timestamp}|{row['close']}\n")
    print(f"   ✅ TXT para grep: {txt_path}")
    
    # 5. Resumen estadístico
    stats = {
        'total_records': len(df),
        'date_range': {
            'start': df['datetime'].min().isoformat(),
            'end': df['datetime'].max().isoformat()
        },
        'price_stats': {
            'min': float(df['close'].min()),
            'max': float(df['close'].max()),
            'mean': float(df['close'].mean()),
            'median': float(df['close'].median())
        },
        'file_sizes': {
            'csv_complete': os.path.getsize(csv_path),
            'csv_grepable': os.path.getsize(grep_csv_path),
            'json': os.path.getsize(json_path),
            'txt_grepable': os.path.getsize(txt_path)
        }
    }
    
    stats_path = 'download_stats.json'
    with open(stats_path, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"   ✅ Estadísticas: {stats_path}")
    
    return df, stats

def create_lookup_class():
    """
    Crea una clase de ejemplo para usar los datos descargados
    """
    lookup_code = '''
import pandas as pd
import numpy as np
from datetime import datetime

class ETHPriceLookup:
    """
    Clase para buscar precios históricos de Ethereum de forma rápida
    Datos de los últimos 2 años por hora
    """
    
    def __init__(self, csv_path='eth_historical_prices_complete.csv'):
        print(f"📖 Cargando datos históricos (últimos 2 años, por hora) desde {csv_path}...")
        self.df = pd.read_csv(csv_path)
        self.df['datetime'] = pd.to_datetime(self.df['datetime'])
        # Crear índice para búsqueda rápida
        self.df.set_index('datetime', inplace=True)
        self.df.sort_index(inplace=True)
        print(f"✅ Datos cargados: {len(self.df)} registros")
        print(f"📅 Rango: {self.df.index.min()} - {self.df.index.max()}")
        print(f"📊 Período: Últimos 2 años por hora")
    
    def get_price_at_timestamp(self, timestamp_str, tolerance_hours=2):
        """
        Busca el precio más cercano al timestamp dado
        
        Args:
            timestamp_str: Timestamp en formato ISO (ej: "2025-07-10T21:23:59")
            tolerance_hours: Tolerancia en horas para considerar válido (por defecto 2 horas)
        
        Returns:
            float: Precio en USD, o None si no se encuentra
        """
        try:
            target_time = pd.to_datetime(timestamp_str)
            
            # Verificar que el timestamp esté dentro del rango de datos
            if target_time < self.df.index.min() or target_time > self.df.index.max():
                print(f"⚠️  Timestamp {timestamp_str} fuera del rango de datos disponibles")
                print(f"   Rango disponible: {self.df.index.min()} - {self.df.index.max()}")
                return None
            
            # Buscar el registro más cercano
            closest_idx = self.df.index.get_indexer([target_time], method='nearest')[0]
            closest_time = self.df.index[closest_idx]
            
            # Verificar si está dentro de la tolerancia (ahora en horas)
            time_diff = abs((target_time - closest_time).total_seconds() / 3600)
            if time_diff > tolerance_hours:
                print(f"⚠️  Timestamp {timestamp_str} muy lejano del más cercano ({time_diff:.1f} horas)")
                return None
                
            price = self.df.iloc[closest_idx]['close']
            return float(price)
            
        except Exception as e:
            print(f"❌ Error buscando precio para {timestamp_str}: {e}")
            return None
    
    def get_prices_batch(self, timestamps, tolerance_hours=2):
        """
        Obtiene precios para múltiples timestamps de una vez
        
        Args:
            timestamps: Lista de timestamps en formato ISO
            tolerance_hours: Tolerancia en horas
        
        Returns:
            dict: {timestamp: price}
        """
        prices = {}
        for ts in timestamps:
            prices[ts] = self.get_price_at_timestamp(ts, tolerance_hours)
        return prices
    
    def get_price_range(self, start_timestamp, end_timestamp):
        """
        Obtiene todos los precios en un rango de tiempo
        
        Args:
            start_timestamp: Timestamp inicial (ISO)
            end_timestamp: Timestamp final (ISO)
        
        Returns:
            DataFrame: Precios en el rango especificado
        """
        start_time = pd.to_datetime(start_timestamp)
        end_time = pd.to_datetime(end_timestamp)
        
        mask = (self.df.index >= start_time) & (self.df.index <= end_time)
        return self.df.loc[mask]

# Ejemplo de uso:
if __name__ == "__main__":
    # Cargar datos
    eth_lookup = ETHPriceLookup()
    
    # Ejemplo: buscar precio en un timestamp específico
    timestamp = "2025-07-10T21:23:59"
    price = eth_lookup.get_price_at_timestamp(timestamp)
    print(f"💰 Precio ETH en {timestamp}: ${price}")
    
    # Ejemplo: buscar múltiples precios
    timestamps = [
        "2025-07-10T21:23:59",
        "2025-07-10T21:56:35",
        "2025-07-10T22:15:00"
    ]
    prices = eth_lookup.get_prices_batch(timestamps)
    for ts, price in prices.items():
        print(f"💰 {ts}: ${price}")
'''
    
    with open('eth_price_lookup.py', 'w') as f:
        f.write(lookup_code)
    print(f"   ✅ Clase de lookup: eth_price_lookup.py")

def main():
    """
    Función principal que ejecuta todo el proceso
    """
    print("=" * 60)
    print("🔽 DESCARGADOR DE HISTÓRICO DE PRECIOS DE ETHEREUM")
    print("📅 PERÍODO: ÚLTIMOS 2 AÑOS")
    print("⏰ FRECUENCIA: DATOS POR HORA")
    print("=" * 60)
    
    # Descargar datos
    all_data = download_eth_historical_data()
    
    if not all_data:
        print("❌ No se pudieron descargar datos")
        return
    
    # Procesar y guardar
    df, stats = process_and_save_data(all_data)
    
    # Crear clase de lookup
    create_lookup_class()
    
    # Mostrar resumen final
    print("\n" + "=" * 60)
    print("🎉 ¡DESCARGA COMPLETADA!")
    print("📅 PERÍODO: ÚLTIMOS 2 AÑOS")
    print("⏰ FRECUENCIA: DATOS POR HORA")
    print("=" * 60)
    print(f"📊 Total de registros: {stats['total_records']:,}")
    print(f"📅 Rango temporal: {stats['date_range']['start']} - {stats['date_range']['end']}")
    print(f"💰 Rango de precios: ${stats['price_stats']['min']:.2f} - ${stats['price_stats']['max']:.2f}")
    print(f"📁 Archivos creados:")
    print(f"   • eth_historical_prices_complete.csv ({stats['file_sizes']['csv_complete']:,} bytes)")
    print(f"   • eth_prices_grepable.csv ({stats['file_sizes']['csv_grepable']:,} bytes)")
    print(f"   • eth_historical_prices.json ({stats['file_sizes']['json']:,} bytes)")
    print(f"   • eth_prices_grepable.txt ({stats['file_sizes']['txt_grepable']:,} bytes)")
    print(f"   • eth_price_lookup.py (clase para usar los datos)")
    print(f"   • download_stats.json (estadísticas)")
    print("\n🚀 ¡Ahora puedes usar estos datos en tu proyecto!")
    print("💡 Los datos cubren los últimos 2 años con resolución por hora")
    print("⏰ Tolerancia por defecto: 2 horas (puedes ajustarla)")

if __name__ == "__main__":
    main() 