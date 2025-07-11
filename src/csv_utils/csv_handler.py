import os
import shutil
import csv
from datetime import datetime
import pandas as pd
import json

def backup_csv_if_exists(csv_path):
    """
    If the CSV file exists, create a backup with a timestamp.
    """
    if os.path.exists(csv_path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = csv_path.replace(".csv", f".backup_{timestamp}.csv")
        shutil.copy2(csv_path, backup_path)
        print(f"Backup created: {backup_path}")

def update_or_append_address_row(csv_path, address, analysis_data):
    """
    Update the row for the given address by adding new columns with incremented suffixes,
    or append a new row if the address does not exist.
    analysis_data: dict with keys like 'Timestamp', 'HasBuyOver0.1ETH', ... (no suffix)
    """
    # Read existing CSV or create empty DataFrame
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, dtype=str)
    else:
        df = pd.DataFrame()

    # Check if address exists
    if not df.empty and address in df['Address'].values:
        row_idx = df.index[df['Address'] == address][0]
        # Find the highest suffix for this address
        suffixes = [1]
        for col in df.columns:
            if col.startswith('Timestamp_'):
                try:
                    suf = int(col.split('_')[1])
                    if not pd.isna(df.at[row_idx, col]):
                        suffixes.append(suf)
                except Exception:
                    pass
        next_suffix = max(suffixes) + 1
        # Add new columns if needed
        for key, value in analysis_data.items():
            colname = f"{key}_{next_suffix}"
            if colname not in df.columns:
                df[colname] = None
            df.at[row_idx, colname] = value
        updated = True
    else:
        # New address: create row with _1 suffix
        row = {'Address': address}
        for key, value in analysis_data.items():
            row[f"{key}_1"] = value
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        updated = False
    # Save back
    df.to_csv(csv_path, index=False)
    return updated 

def get_last_tx_hashes_for_address(csv_path, address):
    """
    Returns a set of transaction hashes from the most recent BigBuyDetails_N column for the given address.
    """
    if not os.path.exists(csv_path):
        return set()
    df = pd.read_csv(csv_path, dtype=str)
    if df.empty or address not in df['Address'].values:
        return set()
    row_idx = df.index[df['Address'] == address][0]
    # Find all BigBuyDetails_N columns for this row
    suffixes = []
    for col in df.columns:
        if col.startswith('BigBuyDetails_'):
            try:
                suf = int(col.split('_')[1])
                if not pd.isna(df.at[row_idx, col]):
                    suffixes.append(suf)
            except Exception:
                pass
    if not suffixes:
        return set()
    last_suffix = max(suffixes)
    colname = f"BigBuyDetails_{last_suffix}"
    try:
        txs = json.loads(df.at[row_idx, colname])
        hashes = {tx['hash'] for tx in txs if 'hash' in tx}
        return hashes
    except Exception:
        return set() 