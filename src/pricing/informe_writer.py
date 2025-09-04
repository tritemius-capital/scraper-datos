"""
Informe Writer Module

Handles writing a single consolidated report file that gets updated with each token analysis.
"""

import pandas as pd
import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class InformeWriter:
    """
    Writer para generar un informe único con JSON objects en celdas
    Una fila = Un pool, con datos complejos en formato JSON
    """
    
    def __init__(self, output_file: str = "data/informe.csv"):
        self.output_file = output_file
        self.ensure_data_directory()
    
    def ensure_data_directory(self):
        """Crear directorio data si no existe"""
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
    
    def initialize_file(self):
        """
        Inicializar el archivo CSV con headers optimizados para JSON objects
        """
        headers = [
            # Campos atómicos (columnas normales)
            'token_address',
            'token_name', 
            'pool_address',
            'uniswap_version',
            'analysis_timestamp',
            'blocks_analyzed',
            'start_block',
            'end_block',
            'total_swaps',
            
            # Liquidez spot
            'liquidity_weth',
            'liquidity_token',
            'liquidity_usd',
            
            # Campos JSON (objetos complejos en celdas)
            'price_summary_json',      # {first, last, min, max} con timestamps
            'swaps_json',              # Lista de swaps recientes (limitada)
            'big_buys_json',           # Lista de big buys filtrados
            'liquidity_history_json',  # Snapshots de liquidez (opcional)
            'analysis_metadata_json'   # Metadata del análisis
        ]
        
        df = pd.DataFrame(columns=headers)
        df.to_csv(self.output_file, index=False)
        logger.info(f"Initialized informe file: {self.output_file}")
    
    def prepare_token_data(self, token_summary: Dict) -> Dict:
        """
        Convierte el token_summary al nuevo formato con JSON objects usando datos reales
        
        Args:
            token_summary: Datos del análisis del token con datos estructurados
            
        Returns:
            Dict con datos preparados para el informe JSON
        """
        try:
            # Campos atómicos básicos
            data = {
                'token_address': token_summary.get('token_address', ''),
                'token_name': token_summary.get('token_name', ''),
                'pool_address': token_summary.get('pool_address', ''),
                'uniswap_version': token_summary.get('uniswap_version', ''),
                'analysis_timestamp': token_summary.get('analysis_timestamp', ''),
                'blocks_analyzed': token_summary.get('blocks_analyzed', 0),
                'start_block': token_summary.get('start_block', 0),
                'end_block': token_summary.get('end_block', 0),
                'total_swaps': token_summary.get('total_swaps', 0),
                
                # Liquidez spot (placeholder - se puede obtener del pool)
                'liquidity_weth': 0.0,  # TODO: Obtener del pool info
                'liquidity_token': 0.0,  # TODO: Obtener del pool info  
                'liquidity_usd': 0.0,    # TODO: Calcular
            }
            
            # JSON Objects en celdas usando datos reales
            
            # 1. Price Summary JSON (usar datos reales estructurados)
            price_summary_data = token_summary.get('price_summary_data', {})
            data['price_summary_json'] = json.dumps(price_summary_data, separators=(',', ':'))
            
            # 2. Swaps JSON (usar datos reales - últimos 50 swaps)
            swaps_data = token_summary.get('swaps_data', [])
            data['swaps_json'] = json.dumps(swaps_data, separators=(',', ':'))
            
            # 3. Big Buys JSON (usar datos reales)
            big_buys_data = token_summary.get('big_buys_data', [])
            data['big_buys_json'] = json.dumps(big_buys_data, separators=(',', ':'))
            
            # 4. Liquidity History JSON (placeholder por ahora)
            liquidity_history = []  # TODO: Implementar snapshots de liquidez
            data['liquidity_history_json'] = json.dumps(liquidity_history, separators=(',', ':'))
            
            # 5. Analysis Metadata JSON (métricas calculadas)
            metadata = {
                "analysis_version": "2.0_json",
                "big_buy_threshold_eth": 0.1,
                "price_change_from_low_pct": token_summary.get('price_change_from_low_pct', '0.00%'),
                "price_change_from_high_pct": token_summary.get('price_change_from_high_pct', '0.00%'),
                "total_big_buy_eth": token_summary.get('total_big_buy_eth', '0.000000'),
                "total_big_buy_usd": token_summary.get('total_big_buy_usd', '0.00'),
                "avg_big_buy_eth": token_summary.get('avg_big_buy_eth', '0.000000'),
                "largest_big_buy_eth": token_summary.get('largest_big_buy_eth', '0.000000'),
                "big_buys_count": token_summary.get('big_buys_count', 0),
                "current_price_usd": token_summary.get('current_price_usd', 0),
                "lowest_price_usd": token_summary.get('lowest_price_usd', 0),
                "highest_price_usd": token_summary.get('highest_price_usd', 0)
            }
            data['analysis_metadata_json'] = json.dumps(metadata, separators=(',', ':'))
            
            return data
            
        except Exception as e:
            logger.error(f"Error preparing token data: {e}")
            return {}
    
    def update_or_add_token(self, token_data: Dict):
        """
        Actualiza o añade un token al informe
        
        Args:
            token_data: Datos preparados del token
        """
        try:
            # Leer archivo existente
            if os.path.exists(self.output_file):
                df = pd.read_csv(self.output_file)
            else:
                # Si no existe, crear con headers
                self.initialize_file()
                df = pd.read_csv(self.output_file)
            
            token_address = token_data.get('token_address', '')
            pool_address = token_data.get('pool_address', '')
            
            # Buscar si ya existe (por token + pool)
            existing_mask = (
                (df['token_address'] == token_address) & 
                (df['pool_address'] == pool_address)
            )
            
            if existing_mask.any():
                # Actualizar fila existente
                for col, value in token_data.items():
                    df.loc[existing_mask, col] = value
                logger.info(f"Updated existing token: {token_address}")
            else:
                # Añadir nueva fila
                new_row = pd.DataFrame([token_data])
                df = pd.concat([df, new_row], ignore_index=True)
                logger.info(f"Added new token: {token_address}")
            
            # Guardar archivo
            df.to_csv(self.output_file, index=False)
            logger.info(f"Informe updated: {self.output_file}")
            
        except Exception as e:
            logger.error(f"Error updating informe: {e}")
    
    def read_token_json_data(self, token_address: str, pool_address: str, json_field: str) -> Any:
        """
        Utilidad para leer y parsear un campo JSON específico de un token
        
        Args:
            token_address: Dirección del token
            pool_address: Dirección del pool  
            json_field: Campo JSON a leer ('price_summary_json', 'swaps_json', etc.)
            
        Returns:
            Objeto Python parseado del JSON, o None si no existe
        """
        try:
            if not os.path.exists(self.output_file):
                return None
                
            df = pd.read_csv(self.output_file)
            
            mask = (
                (df['token_address'] == token_address) & 
                (df['pool_address'] == pool_address)
            )
            
            if not mask.any():
                return None
            
            json_str = df.loc[mask, json_field].iloc[0]
            if pd.isna(json_str) or json_str == '':
                return None
                
            return json.loads(json_str)
            
        except Exception as e:
            logger.error(f"Error reading JSON field {json_field}: {e}")
            return None 