#!/usr/bin/env python3
"""
Swap Data Normalization Script

Validates and normalizes JSONL swap files to production schema:
- Removes empty t0/t1 fields (token addresses are in pools.csv)
- Ensures 0x prefix for hashes and addresses
- Validates pool addresses exist in pools.csv
- Checks data integrity and format compliance

Usage:
    python3 normalize_swaps.py data/swaps_*.jsonl.gz
"""

import gzip
import json
import pandas as pd
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Any

def load_pools_metadata(pools_csv: str) -> pd.DataFrame:
    """Load pools metadata for validation"""
    try:
        pools = pd.read_csv(pools_csv)
        pools['pool_address'] = pools['pool_address'].str.lower()
        return pools.set_index('pool_address')
    except FileNotFoundError:
        print(f"❌ Pools CSV not found: {pools_csv}")
        print("💡 Run the main analysis first to generate pools.csv")
        sys.exit(1)

def validate_swap(swap: Dict[str, Any], pools_df: pd.DataFrame, line_num: int) -> Dict[str, str]:
    """Validate and return list of issues with a swap record"""
    issues = []
    
    # Required fields
    required_fields = ['t', 'b', 'h', 'p', 'v', 'a0', 'a1', 'sd', 'rc']
    for field in required_fields:
        if field not in swap:
            issues.append(f"Missing required field: {field}")
    
    if issues:  # Skip further validation if basic structure is wrong
        return issues
    
    # Validate pool exists
    if swap['p'] not in pools_df.index:
        issues.append(f"Unknown pool: {swap['p']}")
    
    # Validate hash format
    if not isinstance(swap['h'], str) or len(swap['h']) != 66 or not swap['h'].startswith('0x'):
        issues.append(f"Invalid hash format: {swap['h']}")
    
    # Validate version
    if swap['v'] not in [2, 3]:
        issues.append(f"Invalid version: {swap['v']}")
    
    # Validate addresses have 0x prefix
    for addr_field in ['p', 'sd', 'rc']:
        addr = swap.get(addr_field, '')
        if addr and (not isinstance(addr, str) or not addr.startswith('0x') or len(addr) != 42):
            issues.append(f"Invalid address format for {addr_field}: {addr}")
    
    # Check for deprecated fields
    deprecated_fields = ['t0', 't1', 's', 'r']
    for field in deprecated_fields:
        if field in swap:
            issues.append(f"Deprecated field found: {field}")
    
    # Validate amounts are strings (for precision)
    for amt_field in ['a0', 'a1']:
        if not isinstance(swap.get(amt_field), str):
            issues.append(f"Amount {amt_field} should be string, got {type(swap.get(amt_field))}")
        else:
            try:
                int(swap[amt_field])  # Should be parseable as integer
            except ValueError:
                issues.append(f"Amount {amt_field} not a valid integer: {swap[amt_field]}")
    
    return issues

def normalize_swap(swap: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a swap record to production schema"""
    normalized = {}
    
    # Copy core fields
    for field in ['t', 'b', 'v', 'a0', 'a1']:
        normalized[field] = swap.get(field)
    
    # Normalize hash (ensure 0x prefix)
    h = swap.get('h', '')
    if h and not h.startswith('0x'):
        h = '0x' + h
    normalized['h'] = h
    
    # Normalize addresses (ensure 0x prefix and lowercase)
    for addr_field in ['p', 'sd', 'rc']:
        addr = swap.get(addr_field, '')
        if addr:
            if not addr.startswith('0x'):
                addr = '0x' + addr
            normalized[addr_field] = addr.lower()
        else:
            normalized[addr_field] = addr
    
    # Convert amounts to strings for precision
    for amt_field in ['a0', 'a1']:
        amt = swap.get(amt_field)
        if amt is not None:
            normalized[amt_field] = str(amt)
    
    # Remove deprecated fields (t0, t1, s, r)
    # They're already not copied above
    
    return normalized

def process_file(input_file: str, output_file: str, pools_df: pd.DataFrame, 
                dry_run: bool = False) -> Dict[str, int]:
    """Process a single JSONL.gz file"""
    
    stats = {
        'total_swaps': 0,
        'valid_swaps': 0,
        'invalid_swaps': 0,
        'normalized_swaps': 0
    }
    
    issues_summary = {}
    
    print(f"📄 Processing: {input_file}")
    
    try:
        # Read input file
        with gzip.open(input_file, 'rt', encoding='utf-8') as f_in:
            swaps = []
            
            for line_num, line in enumerate(f_in, 1):
                stats['total_swaps'] += 1
                
                try:
                    swap = json.loads(line.strip())
                    
                    # Validate swap
                    issues = validate_swap(swap, pools_df, line_num)
                    
                    if issues:
                        stats['invalid_swaps'] += 1
                        for issue in issues:
                            issues_summary[issue] = issues_summary.get(issue, 0) + 1
                        
                        if len(issues_summary) <= 10:  # Only show first 10 types of issues
                            print(f"  ⚠️  Line {line_num}: {', '.join(issues)}")
                    else:
                        stats['valid_swaps'] += 1
                    
                    # Normalize swap
                    normalized_swap = normalize_swap(swap)
                    swaps.append(normalized_swap)
                    stats['normalized_swaps'] += 1
                    
                except json.JSONDecodeError as e:
                    stats['invalid_swaps'] += 1
                    print(f"  ❌ Line {line_num}: JSON decode error: {e}")
                except Exception as e:
                    stats['invalid_swaps'] += 1
                    print(f"  ❌ Line {line_num}: Unexpected error: {e}")
        
        # Write normalized output
        if not dry_run and swaps:
            with gzip.open(output_file, 'wt', encoding='utf-8') as f_out:
                for swap in swaps:
                    f_out.write(json.dumps(swap, separators=(',', ':')) + '\n')
            print(f"  ✅ Normalized file saved: {output_file}")
        
        return stats
        
    except Exception as e:
        print(f"  ❌ Error processing file: {e}")
        return stats

def main():
    parser = argparse.ArgumentParser(description='Normalize JSONL swap files to production schema')
    parser.add_argument('files', nargs='+', help='JSONL.gz files to normalize')
    parser.add_argument('--pools-csv', default='data/pools.csv', help='Pools metadata CSV file')
    parser.add_argument('--output-dir', default='data/normalized/', help='Output directory for normalized files')
    parser.add_argument('--dry-run', action='store_true', help='Validate only, do not write output files')
    parser.add_argument('--suffix', default='_normalized', help='Suffix for output files')
    
    args = parser.parse_args()
    
    print("🔧 Swap Data Normalization Tool")
    print("=" * 50)
    
    # Load pools metadata
    print(f"📊 Loading pools metadata from: {args.pools_csv}")
    pools_df = load_pools_metadata(args.pools_csv)
    print(f"✅ Loaded {len(pools_df)} pools")
    
    # Create output directory
    output_dir = Path(args.output_dir)
    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Output directory: {output_dir}")
    
    # Process files
    total_stats = {
        'total_swaps': 0,
        'valid_swaps': 0,
        'invalid_swaps': 0,
        'normalized_swaps': 0
    }
    
    for file_path in args.files:
        input_file = Path(file_path)
        
        if not input_file.exists():
            print(f"❌ File not found: {file_path}")
            continue
        
        # Generate output filename
        output_filename = input_file.stem.replace('.jsonl', f'{args.suffix}.jsonl') + '.gz'
        output_file = output_dir / output_filename
        
        # Process file
        stats = process_file(str(input_file), str(output_file), pools_df, args.dry_run)
        
        # Update totals
        for key in total_stats:
            total_stats[key] += stats[key]
        
        print(f"  📈 Stats: {stats['valid_swaps']}/{stats['total_swaps']} valid, {stats['normalized_swaps']} normalized")
        print()
    
    # Summary
    print("📊 SUMMARY")
    print("=" * 20)
    print(f"Total swaps processed: {total_stats['total_swaps']:,}")
    print(f"Valid swaps: {total_stats['valid_swaps']:,}")
    print(f"Invalid swaps: {total_stats['invalid_swaps']:,}")
    print(f"Normalized swaps: {total_stats['normalized_swaps']:,}")
    
    if total_stats['invalid_swaps'] > 0:
        validity_rate = (total_stats['valid_swaps'] / total_stats['total_swaps']) * 100
        print(f"Validity rate: {validity_rate:.1f}%")
    
    if args.dry_run:
        print("\n🔍 DRY RUN - No files were written")
    else:
        print(f"\n✅ Normalized files saved to: {args.output_dir}")
    
    print("\n📖 Schema documentation: data/JSONL_SCHEMA.md")

if __name__ == "__main__":
    main() 