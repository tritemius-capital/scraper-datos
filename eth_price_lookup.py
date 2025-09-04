
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
