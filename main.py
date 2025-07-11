from src.client.etherscan_client import EtherscanClient
from src.filters.big_buy_filter import filter_big_buys, has_big_buy
import os
import json
from datetime import datetime
from src.csv_utils.csv_handler import backup_csv_if_exists, update_or_append_address_row, get_last_tx_hashes_for_address

def main():
    print("=== Query ERC-20 Token Transactions (Etherscan) ===")
    token_address = input("Enter the token address (0x...): ").strip()
    if not token_address.startswith("0x") or len(token_address) != 42:
        print("Invalid address.")
        return

    client = EtherscanClient()
    try:
        txs = client.get_token_transactions(token_address, limit=50)
        formatted = client.format_transactions(txs)
        threshold = 0.1
        big_buys = filter_big_buys(formatted, threshold_eth=threshold)
        has_big = has_big_buy(formatted, threshold_eth=threshold)
        if has_big:
            print(f"\nThere {'is' if len(big_buys)==1 else 'are'} {len(big_buys)} buy(s) >= {threshold} ETH in the last {len(formatted)} transactions:")
            for tx in big_buys:
                print(tx)
        else:
            print(f"\nThis token has NOT had any buy >= {threshold} ETH in the last {len(formatted)} transactions.")
        # Solo guardar si hay transacciones
        if len(formatted) > 0:
            DATA_DIR = "data"
            os.makedirs(DATA_DIR, exist_ok=True)
            CSV_FILENAME = "informe.csv"
            CSV_PATH = os.path.join(DATA_DIR, CSV_FILENAME)
            backup_csv_if_exists(CSV_PATH)

            # Deduplicate: only save new transactions since last analysis for this address
            prev_hashes = get_last_tx_hashes_for_address(CSV_PATH, token_address)
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
            updated = update_or_append_address_row(CSV_PATH, token_address, analysis_data)
            if updated:
                print(f"Updated row for address {token_address} in {CSV_PATH}")
            else:
                print(f"Added new row for address {token_address} in {CSV_PATH}")
        else:
            print("No transactions found. Nothing saved to CSV.")
    except Exception as e:
        print("Error querying Etherscan:", e)
        # Solo guardar errores útiles
        if "No transactions found" not in str(e):
            DATA_DIR = "data"
            os.makedirs(DATA_DIR, exist_ok=True)
            CSV_FILENAME = "informe.csv"
            CSV_PATH = os.path.join(DATA_DIR, CSV_FILENAME)
            backup_csv_if_exists(CSV_PATH)
            analysis_data = {
                "Timestamp": datetime.now().isoformat(),
                "HasBuyOver0.1ETH": False,
                "BigBuyDetails": "[]",
                "NumTransactions": 0,
                "Error": str(e)
            }
            updated = update_or_append_address_row(CSV_PATH, token_address, analysis_data)
            if updated:
                print(f"Updated row for address {token_address} in {CSV_PATH}")
            else:
                print(f"Added new row for address {token_address} in {CSV_PATH}")

if __name__ == "__main__":
    main()
