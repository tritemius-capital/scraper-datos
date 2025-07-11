def filter_big_buys(transactions, threshold_eth=0.1):
    """
    Given a list of formatted transactions (dicts with 'valueETH'),
    return a list of those with valueETH >= threshold_eth.
    """
    return [tx for tx in transactions if tx.get('valueETH', 0) >= threshold_eth]


def has_big_buy(transactions, threshold_eth=0.1):
    """
    Returns True if there is at least one transaction with valueETH >= threshold_eth.
    """
    return any(tx.get('valueETH', 0) >= threshold_eth for tx in transactions) 