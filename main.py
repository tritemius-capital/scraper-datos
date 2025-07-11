from src.client.etherscan_client import EtherscanClient
from src.filters.big_buy_filter import filter_big_buys, has_big_buy

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
        if has_big_buy(formatted, threshold_eth=threshold):
            print(f"\nThere {'is' if len(big_buys)==1 else 'are'} {len(big_buys)} buy(s) >= {threshold} ETH in the last {len(formatted)} transactions:")
            for tx in big_buys:
                print(tx)
        else:
            print(f"\nThis token has NOT had any buy >= {threshold} ETH in the last {len(formatted)} transactions.")
    except Exception as e:
        print("Error querying Etherscan:", e)

if __name__ == "__main__":
    main()
