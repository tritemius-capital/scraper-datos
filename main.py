from src.client.etherscan_client import EtherscanClient

def main():
    print("=== Query ERC-20 Token Transactions (Etherscan) ===")
    token_address = input("Enter the token address (0x...): ").strip()
    if not token_address.startswith("0x") or len(token_address) != 42:
        print("Invalid address.")
        return

    client = EtherscanClient()
    try:
        txs = client.get_token_transactions(token_address, limit=10)
        formatted = client.format_transactions(txs)
        print(f"\nLast {len(formatted)} transactions for {token_address}:")
        for tx in formatted:
            print(tx)
    except Exception as e:
        print("Error querying Etherscan:", e)

if __name__ == "__main__":
    main()
