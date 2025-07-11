from src.client.etherscan_client import EtherscanClient
from src.filters.big_buy_filter import filter_big_buys, has_big_buy
import os
import json
from datetime import datetime
from src.csv_utils.csv_handler import backup_csv_if_exists, update_or_append_address_row, get_last_tx_hashes_for_address
import pandas as pd

def update_address(token_address, client, csv_path):
    try:
        txs = client.get_token_transactions(token_address, limit=50)
        formatted = client.format_transactions(txs)
        threshold = 0.1
        big_buys = filter_big_buys(formatted, threshold_eth=threshold)
        has_big = has_big_buy(formatted, threshold_eth=threshold)
        if has_big:
            print(f"\nThere {'is' if len(big_buys)==1 else 'are'} {len(big_buys)} buy(s) >= {threshold} ETH in the last {len(formatted)} transactions for {token_address}:")
            for tx in big_buys:
                print(tx)
        else:
            print(f"\nThis token has NOT had any buy >= {threshold} ETH in the last {len(formatted)} transactions for {token_address}.")
        # Solo guardar si hay transacciones
        if len(formatted) > 0:
            backup_csv_if_exists(csv_path)
            prev_hashes = get_last_tx_hashes_for_address(csv_path, token_address)
            if prev_hashes:
                new_txs = [tx for tx in formatted if tx['hash'] not in prev_hashes]
            else:
                new_txs = formatted
            analysis_data = {
                "Timestamp": datetime.now().isoformat(),
                "HasBuyOver0.1ETH": has_big,
                "BigBuyDetails": json.dumps(new_txs, ensure_ascii=False),
                "NumTransactions": len(new_txs),
                "Error": ""
            }
            updated = update_or_append_address_row(csv_path, token_address, analysis_data)
            if updated:
                print(f"Updated row for address {token_address} in {csv_path}")
            else:
                print(f"Added new row for address {token_address} in {csv_path}")
        else:
            print(f"No transactions found for {token_address}. Nothing saved to CSV.")
    except Exception as e:
        print(f"Error querying Etherscan for {token_address}: {e}")
        if "No transactions found" not in str(e):
            backup_csv_if_exists(csv_path)
            analysis_data = {
                "Timestamp": datetime.now().isoformat(),
                "HasBuyOver0.1ETH": False,
                "BigBuyDetails": "[]",
                "NumTransactions": 0,
                "Error": str(e)
            }
            updated = update_or_append_address_row(csv_path, token_address, analysis_data)
            if updated:
                print(f"Updated row for address {token_address} in {csv_path}")
            else:
                print(f"Added new row for address {token_address} in {csv_path}")

def main():
    print("=== Ethereum Token Analyzer ===")
    print("1. Track/update a single address")
    print("2. Update ALL addresses in the CSV (batch mode)")
    option = input("Select an option (1 or 2): ").strip()

    DATA_DIR = "data"
    os.makedirs(DATA_DIR, exist_ok=True)
    CSV_FILENAME = "informe.csv"
    CSV_PATH = os.path.join(DATA_DIR, CSV_FILENAME)
    client = EtherscanClient()

    if option == "1":
        token_address = input("Enter the token address (0x...): ").strip()
        if not token_address.startswith("0x") or len(token_address) != 42:
            print("Invalid address.")
            return
        update_address(token_address, client, CSV_PATH)
    elif option == "2":
        if not os.path.exists(CSV_PATH):
            print(f"CSV file {CSV_PATH} does not exist. Run option 1 first to create it.")
            return
        df = pd.read_csv(CSV_PATH, dtype=str)
        if df.empty or "Address" not in df.columns:
            print(f"CSV file {CSV_PATH} is empty or missing the Address column.")
            return
        addresses = df["Address"].dropna().unique()
        print(f"Found {len(addresses)} addresses to update.")
        updated_count = 0
        for i, addr in enumerate(addresses, 1):
            print(f"\n[{i}/{len(addresses)}] Updating address: {addr}")
            update_address(addr, client, CSV_PATH)
            updated_count += 1
        print(f"\nBatch update complete. {updated_count} addresses processed.")
    else:
        print("Invalid option. Exiting.")

if __name__ == "__main__":
    main()
