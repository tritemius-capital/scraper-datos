from src.client.etherscan_client import EtherscanClient
from src.filters.big_buy_filter import filter_big_buys, has_big_buy
import os
import json
from datetime import datetime
from src.csv_utils.csv_handler import backup_csv_if_exists, append_row_to_csv

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
        # CSV logic
        DATA_DIR = "data"
        os.makedirs(DATA_DIR, exist_ok=True)
        CSV_FILENAME = "informe.csv"
        CSV_PATH = os.path.join(DATA_DIR, CSV_FILENAME)
        backup_csv_if_exists(CSV_PATH)
        row = {
            "Address": token_address,
            "Date": datetime.now().isoformat(),
            "HasBuyOver0.1ETH_Last50TX": has_big,
            "BigBuyDetails": json.dumps(big_buys, ensure_ascii=False),
            "NumTransactions": len(formatted),
            "Error": ""
        }
        header = list(row.keys())
        append_row_to_csv(CSV_PATH, row, header)
        print(f"Row added to {CSV_PATH}")
    except Exception as e:
        print("Error querying Etherscan:", e)
        # CSV logic for error
        DATA_DIR = "data"
        os.makedirs(DATA_DIR, exist_ok=True)
        CSV_FILENAME = "informe.csv"
        CSV_PATH = os.path.join(DATA_DIR, CSV_FILENAME)
        backup_csv_if_exists(CSV_PATH)
        row = {
            "Address": token_address,
            "Date": datetime.now().isoformat(),
            "HasBuyOver0.1ETH_Last50TX": False,
            "BigBuyDetails": "[]",
            "NumTransactions": 0,
            "Error": str(e)
        }
        header = list(row.keys())
        append_row_to_csv(CSV_PATH, row, header)
        
if __name__ == "__main__":
    main()
