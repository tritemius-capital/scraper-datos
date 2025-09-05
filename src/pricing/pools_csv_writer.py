"""
Pools CSV Writer Module

Generates a pools.csv file with pool metadata to complement JSONL swap files.
This avoids repeating token addresses in every swap line.
"""

import csv
import logging
from typing import List, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class PoolsCSVWriter:
    """Writes pool metadata to CSV format"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def save_pools_csv(self, results: List[Dict], output_file: str = "data/pools.csv") -> bool:
        """
        Save pool metadata to CSV file
        
        Args:
            results: List of analysis results containing pool info
            output_file: Output CSV file path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Define fieldnames for pools CSV
            fieldnames = [
                'pool_address',
                'version',
                'token0_address',
                'token1_address', 
                'token0_symbol',
                'token1_symbol',
                'token0_name',
                'token1_name',
                'token0_decimals',
                'token1_decimals',
                'token0_total_supply',
                'token1_total_supply',
                'fee_tier',  # V3 only
                'creation_block',
                'creation_timestamp'
            ]
            
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                pools_written = 0
                
                for result in results:
                    try:
                        pool_row = self._extract_pool_metadata(result)
                        if pool_row:
                            writer.writerow(pool_row)
                            pools_written += 1
                            
                    except Exception as e:
                        self.logger.warning(f"Error processing pool metadata: {e}")
                        continue
            
            self.logger.info(f"Written {pools_written} pools to {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving pools CSV: {e}")
            return False
    
    def _extract_pool_metadata(self, result: Dict) -> Dict:
        """Extract pool metadata from analysis result"""
        
        try:
            pool_address = result.get('pool_address', '')
            version = result.get('uniswap_version', '')
            pool_info = result.get('pool_info', {})
            advanced_analytics = result.get('advanced_analytics', {})
            pool_creation = advanced_analytics.get('pool_creation', {})
            
            # Basic pool info
            row = {
                'pool_address': pool_address.lower(),
                'version': version.upper(),
                'token0_address': pool_info.get('token0', '').lower(),
                'token1_address': pool_info.get('token1', '').lower(),
                'token0_symbol': pool_info.get('token0_symbol', 'UNKNOWN'),
                'token1_symbol': pool_info.get('token1_symbol', 'UNKNOWN'),
                'token0_name': pool_info.get('token0_name', 'Unknown'),
                'token1_name': pool_info.get('token1_name', 'Unknown'),
                'token0_decimals': pool_info.get('token0_decimals', 18),
                'token1_decimals': pool_info.get('token1_decimals', 18),
                'token0_total_supply': self._format_supply(pool_info.get('token0_total_supply', 0)),
                'token1_total_supply': self._format_supply(pool_info.get('token1_total_supply', 0)),
            }
            
            # V3 specific data
            if version.upper() == 'V3':
                slot0_data = pool_info.get('slot0', {})
                row['fee_tier'] = slot0_data.get('fee', 0)
            else:
                row['fee_tier'] = 'N/A'
            
            # Pool creation info
            row.update({
                'creation_block': pool_creation.get('creation_block', 0),
                'creation_timestamp': pool_creation.get('creation_timestamp', 0)
            })
            
            return row
            
        except Exception as e:
            self.logger.warning(f"Error extracting pool metadata: {e}")
            return {}
    
    def _format_supply(self, supply: Any) -> str:
        """Format token supply in a readable way"""
        try:
            supply = float(supply)
            if supply == 0:
                return "0"
            elif supply >= 1e18:
                return f"{supply/1e18:.2f}E18"
            elif supply >= 1e15:
                return f"{supply/1e15:.2f}E15"
            elif supply >= 1e12:
                return f"{supply/1e12:.2f}E12"
            elif supply >= 1e9:
                return f"{supply/1e9:.2f}E9"
            elif supply >= 1e6:
                return f"{supply/1e6:.2f}E6"
            else:
                return f"{supply:.0f}"
        except:
            return "0"
    
    def append_pool_csv(self, result: Dict, output_file: str = "data/pools.csv") -> bool:
        """
        Append a single pool to the pools CSV file
        
        Args:
            result: Single analysis result containing pool info
            output_file: Output CSV file path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if file exists
            file_exists = Path(output_file).exists()
            
            # Define fieldnames
            fieldnames = [
                'pool_address', 'version', 'token0_address', 'token1_address', 
                'token0_symbol', 'token1_symbol', 'token0_name', 'token1_name',
                'token0_decimals', 'token1_decimals', 'token0_total_supply', 
                'token1_total_supply', 'fee_tier', 'creation_block', 'creation_timestamp'
            ]
            
            with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header if file is new
                if not file_exists:
                    writer.writeheader()
                
                # Extract and write pool metadata
                pool_row = self._extract_pool_metadata(result)
                if pool_row:
                    writer.writerow(pool_row)
                    self.logger.info(f"Appended pool {result.get('pool_address', '')} to {output_file}")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error appending to pools CSV: {e}")
            return False 