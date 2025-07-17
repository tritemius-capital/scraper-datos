# Uniswap Price Extraction System

This package provides a unified interface for extracting price data from both Uniswap V2 and V3 pools. It automatically handles the differences between versions and provides a consistent API.

## 🏗️ Architecture

```
src/uniswap/
├── __init__.py              # Main package exports
├── factory.py               # Factory for creating extractors
├── common/
│   ├── __init__.py
│   └── base_extractor.py    # Abstract base class
├── v2/
│   ├── __init__.py
│   └── extractor.py         # Uniswap V2 implementation
└── v3/
    ├── __init__.py
    └── extractor.py         # Uniswap V3 implementation
```

## 🔧 Key Components

### BaseUniswapExtractor (Abstract Base Class)
Defines the interface that all Uniswap extractors must implement:

- `get_pool_info()` - Get pool information (tokens, decimals)
- `decode_swap_event()` - Decode swap events from blockchain
- `calculate_token_price()` - Calculate token prices from events
- `extract_prices()` - Main extraction method
- `get_swap_events()` - Fetch events from Etherscan
- `analyze_token_complete()` - Complete analysis with big buy detection

### UniswapV2Extractor
Handles Uniswap V2's simple swap events:
- **Event Structure**: `Swap(sender, amount0In, amount1In, amount0Out, amount1Out, to)`
- **Price Calculation**: Direct ratio from input/output amounts
- **Liquidity**: Uniform across entire price range

### UniswapV3Extractor
Handles Uniswap V3's complex swap events:
- **Event Structure**: `Swap(sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick)`
- **Price Calculation**: Uses `sqrtPriceX96` with decimal adjustments
- **Liquidity**: Concentrated in specific price ranges

### UniswapExtractorFactory
Factory class that automatically selects the correct extractor:
- `create_extractor(version)` - Create specific version extractor
- `detect_version_from_pool()` - Auto-detect pool version
- `create_auto_extractor()` - Create extractor with auto-detection

## 🚀 Usage

### Basic Usage with Auto-Detection

```python
from src.uniswap import UniswapExtractorFactory

# Create factory
factory = UniswapExtractorFactory()

# Auto-detect and create extractor
extractor = factory.create_auto_extractor(
    pool_address="0x...",
    etherscan_api_key="your_api_key"
)

# Extract prices
prices = extractor.extract_prices(
    token_address="0x...",
    pool_address="0x...",
    start_block=1000000,
    end_block=1001000
)
```

### Manual Version Selection

```python
# Create V2 extractor
v2_extractor = factory.create_extractor('v2', etherscan_api_key)

# Create V3 extractor
v3_extractor = factory.create_extractor('v3', etherscan_api_key)
```

### Complete Analysis

```python
# Get complete analysis including big buys
result = extractor.analyze_token_complete(
    token_address="0x...",
    pool_address="0x...",
    start_block=1000000,
    end_block=1001000,
    threshold_eth=0.1
)

# Access results
prices = result['prices']
price_stats = result['price_stats']
big_buy_analysis = result['big_buy_analysis']
```

## 📊 Data Structure

### Price Data Points
Each price point contains:
```python
{
    'timestamp': 1234567890,
    'block_number': 1000000,
    'token_price_eth': 0.00123,
    'token_price_usd': 2.45,
    'eth_price_usd': 2000.0
}
```

### Price Statistics
```python
{
    'lowest_price_usd': 1.50,
    'current_price_usd': 2.45,
    'highest_price_usd': 3.20,
    'price_change_from_low': 63.33,
    'price_change_from_high': -23.44,
    'total_swaps': 150
}
```

### Big Buy Analysis
```python
{
    'total_big_buys': 5,
    'threshold_eth': 0.1,
    'big_buys': [
        {
            'blockNumber': 1000001,
            'ethAmount': 0.5,
            'tokenAmount': 1000,
            'price': 0.0005
        }
    ]
}
```

## 🔄 Differences Between V2 and V3

| Feature | Uniswap V2 | Uniswap V3 |
|---------|------------|------------|
| **Event Structure** | Simple amounts | Complex with sqrtPriceX96 |
| **Price Calculation** | Direct ratios | sqrtPriceX96 conversion |
| **Liquidity** | Uniform | Concentrated |
| **Gas Efficiency** | Lower | Higher |
| **Price Precision** | Lower | Higher |

## 🧪 Testing

Run the test script to verify the system works:

```bash
python test_uniswap_system.py
```

This will test:
1. V2 extractor creation and basic functionality
2. V3 extractor creation and basic functionality
3. Auto-detection system
4. Small-scale price extraction

## 🔧 Configuration

### Environment Variables
- `ETHERSCAN_API_KEY` - Required for fetching blockchain data

### Dependencies
- `web3` - Blockchain interaction
- `tqdm` - Progress bars
- `pandas` - Data manipulation (optional)

## 📝 Integration with Main System

The new system is integrated into `main.py` and provides:

1. **Unified Interface** - Same API for V2 and V3
2. **Auto-Detection** - Automatically selects correct version
3. **Manual Override** - Can specify version if needed
4. **Append Mode** - Adds new tokens to existing CSV
5. **Complete Analysis** - Prices + big buy detection

## 🎯 Benefits

1. **Future-Proof** - Easy to add new Uniswap versions
2. **Consistent API** - Same interface regardless of version
3. **Auto-Detection** - No need to know pool version beforehand
4. **Modular** - Each version is self-contained
5. **Extensible** - Easy to add new features to base class 