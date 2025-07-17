# Ethereum Token Price Extractor & Big Buy Analyzer

A comprehensive Python tool for analyzing Ethereum ERC-20 tokens by extracting price data from Uniswap V2 and V3 pools, identifying significant purchases (big buys), and saving detailed analysis results. Designed for research, trading analysis, and token monitoring.

---

## 🚀 Features

### **Core Functionality**
- **Uniswap V2 & V3 Support**: Automatically detects and analyzes both Uniswap versions
- **Price Extraction**: Extracts token prices in ETH and USD from swap events
- **Big Buy Detection**: Identifies significant purchases (≥ 0.1 ETH by default)
- **Historical Analysis**: Analyzes specified number of blocks for comprehensive data
- **Auto-Version Detection**: Automatically detects Uniswap version from pool address

### **Advanced Analysis**
- **Price Statistics**: Calculates lowest, highest, current prices and price changes
- **Big Buy Enrichment**: Each big buy includes token price at time of purchase
- **USD Value Calculation**: Automatically calculates USD value of each big buy
- **Transaction Details**: Full transaction hashes and swap event details
- **Source Classification**: Distinguishes between swap events and direct transactions

### **Data Output**
- **Human-Readable CSV**: One row per token with all transactions in JSON format
- **ML-Optimized Parquet**: Machine learning friendly format with detailed transaction data
- **Automatic Backups**: Creates backup files in organized subdirectories
- **Incremental Updates**: Appends new data without overwriting existing analysis

---

## 📁 Project Structure

```
datos/
├── main.py                          # Main analysis script
├── requirements.txt                 # Python dependencies
├── README.md                        # This documentation
├── data/                            # Output directory
│   ├── token_analysis.csv          # Human-readable analysis results
│   ├── backups/                    # Automatic backups
│   │   ├── csv/                    # CSV backups
│   │   └── parquet/                # Parquet backups
│   └── detailed_transactions/      # Individual transaction files
├── historical_price_eth/           # ETH historical price data
└── src/
    ├── uniswap/                    # Uniswap analysis modules
    │   ├── factory.py              # Extractor factory for V2/V3
    │   ├── common/
    │   │   └── base_extractor.py   # Abstract base class
    │   ├── v2/
    │   │   └── extractor.py        # Uniswap V2 implementation
    │   └── v3/
    │       └── extractor.py        # Uniswap V3 implementation
    ├── pricing/                    # Price analysis modules
    │   ├── big_buy_analyzer.py     # Big buy detection and analysis
    │   ├── object_csv_writer.py    # CSV output formatting
    │   ├── eth_price_reader.py     # ETH price data handling
    │   └── price_calculator.py     # Price calculation utilities
    ├── client/                     # External API clients
    │   ├── etherscan_client.py     # Etherscan API integration
    │   └── dextools_client.py      # DEXTools API integration
    └── config.py                   # Configuration management
```

---

## ⚙️ Installation & Setup

### 1. **Clone and Setup Environment**

```bash
git clone <repo-url>
cd datos
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. **Install Dependencies**

```bash
pip install -r requirements.txt
```

### 3. **Configure API Keys**

Create a `.env` file in the root directory:

```env
ETHERSCAN_API_KEY=your_etherscan_api_key_here
```

**Required API Keys:**
- **Etherscan API Key**: Required for blockchain data access
  - Get free key at: https://etherscan.io/apis
  - Rate limits: 5 calls/sec for free tier

### 4. **ETH Price Data Setup**

The system uses historical ETH price data for USD calculations. The data file should be located at:
```
historical_price_eth/eth_historical_prices_complete.csv
```

---

## 🏃 Usage

### **Basic Usage**

Run the main analysis script:

```bash
python main.py
```

The script will prompt you for:
1. **Token Address**: ERC-20 token contract address (0x...)
2. **Pool Address**: Uniswap pool address (0x...)
3. **Uniswap Version**: v2, v3, or auto-detect
4. **Number of Blocks**: How many blocks to analyze (default: 15000)

### **Example Session**

```
=== Ethereum Token Price Extractor & Big Buy Analyzer ===
This tool will extract price data and analyze big buys for a token
Supports both Uniswap V2 and V3 pools

Enter the token address (0x...): 0x1234567890123456789012345678901234567890
Enter the Uniswap pool address (0x...): 0xabcdefabcdefabcdefabcdefabcdefabcdefabcd
Enter Uniswap version (v2/v3) or press Enter for auto-detect: 
Auto-detecting Uniswap version...
Detected Uniswap V3 for pool 0xabcdef...

Enter number of blocks to analyze (default 15000): 1000

Time estimate: ~0.1 days of blockchain data

Proceed with analysis? (y/n): y

=== Extracting data for token 0x1234... ===
Pool: 0xabcd...
Blocks to analyze: 1000
Analyzing blocks 19000000 to 19001000

Extracting swap events and analyzing big buys...

=== Analysis Complete ===
Total swaps analyzed: 45
Big buys found: 3

Big Buy Details:
  1. Block 19000001 - 0.5 ETH
  2. Block 19000015 - 0.3 ETH
  3. Block 19000023 - 0.8 ETH

Data saved to: data/token_analysis.csv
✅ Analysis completed successfully!
```

---

## 📊 Output Formats

### **1. Human-Readable CSV Format**

The main output file `data/token_analysis.csv` contains one row per token with the following columns:

| Column | Description |
|--------|-------------|
| `token_address` | ERC-20 token contract address |
| `pool_address` | Uniswap pool address |
| `uniswap_version` | Detected Uniswap version (v2/v3) |
| `price_summary` | JSON with price statistics |
| `big_buy_analysis` | Individual big buy blocks |
| `all_blocks` | All price data blocks |

**Example CSV Row:**
```csv
0x1234...,0xabcd...,v3,"{""lowest_price_usd"":""0.25"",""current_price_usd"":""0.26"",""highest_price_usd"":""0.26"",""price_change_from_low"":""4.00%"",""price_change_from_high"":""0.00%"",""total_swaps"":""45""}","big_buy1:{""blockNumber"":""19000001"",""timestamp"":""1752745091"",""ethAmount"":""0.5"",""transactionHash"":""0x1234..."",""source"":""swap_event"",""token_price_eth"":""0.000123"",""token_price_usd"":""0.25"",""eth_price_usd"":""2025.50"",""usd_value"":""1012.75""} big_buy2:{...}","bloque1:{""timestamp"":""1752745091"",""block_number"":""19000001"",""token_price_eth"":""0.000123"",""token_price_usd"":""0.25"",""eth_price_usd"":""2025.50""} bloque2:{...}"
```

### **2. Big Buy Block Format**

Each big buy is formatted as an individual block with complete information:

```json
big_buy1:{
  "blockNumber":"19000001",
  "timestamp":"1752745091",
  "ethAmount":"0.5",
  "transactionHash":"0x1234...",
  "source":"swap_event",
  "amount0In":"0",
  "amount1In":"500000000000000000",
  "amount0Out":"123456789",
  "amount1Out":"0",
  "token_price_eth":"0.000123",
  "token_price_usd":"0.25",
  "eth_price_usd":"2025.50",
  "usd_value":"1012.75"
}
```

### **3. Price Block Format**

Each price data point is formatted as:

```json
bloque1:{
  "timestamp":"1752745091",
  "block_number":"19000001",
  "token_price_eth":"0.000123",
  "token_price_usd":"0.25",
  "eth_price_usd":"2025.50"
}
```

---

## 🔍 Big Buy Analysis

### **Detection Criteria**
- **Default Threshold**: 0.1 ETH (configurable)
- **Sources**: 
  - Uniswap swap events (WETH/ETH amounts)
  - Direct ETH transactions
- **Enrichment**: Each big buy includes token price at time of purchase

### **Big Buy Information**
- **Block Number & Timestamp**: When the purchase occurred
- **ETH Amount**: Amount of ETH/WETH used
- **Transaction Hash**: Blockchain transaction identifier
- **Source**: Whether from swap event or direct transaction
- **Token Price**: Token price in ETH and USD at time of purchase
- **USD Value**: Total USD value of the purchase
- **Swap Details**: Amount0In, amount1In, amount0Out, amount1Out

---

## 🛠️ Configuration Options

### **Environment Variables**
```env
ETHERSCAN_API_KEY=your_key_here
```

### **Analysis Parameters**
- **Threshold ETH**: Minimum ETH amount for big buy detection (default: 0.1)
- **Block Range**: Number of blocks to analyze (default: 15000)
- **Uniswap Version**: Manual selection or auto-detection

### **Output Settings**
- **CSV Format**: Human-readable with JSON blocks
- **Backup Strategy**: Automatic backups in organized subdirectories
- **Append Mode**: New data appended without overwriting existing

---

## 🔧 Troubleshooting

### **Common Issues**

1. **Etherscan API Errors**
   ```
   Error: ETHERSCAN_API_KEY environment variable not set
   ```
   **Solution**: Check your `.env` file and API key validity

2. **Rate Limiting**
   ```
   NOTOK: Max rate limit reached
   ```
   **Solution**: Wait a few seconds between requests or upgrade API plan

3. **No Data Found**
   ```
   No data found or error: No prices found
   ```
   **Solution**: 
   - Verify pool address is correct
   - Check if pool has recent activity
   - Ensure token is in the specified pool

4. **Version Detection Issues**
   ```
   Could not detect Uniswap version for pool
   ```
   **Solution**: Manually specify version (v2/v3) or verify pool address

### **Performance Optimization**

- **Large Block Ranges**: For analysis of >50,000 blocks, consider breaking into smaller chunks
- **API Limits**: Free Etherscan tier has 5 calls/sec limit
- **Memory Usage**: Large datasets may require significant RAM

---

## 📈 Analysis Examples

### **Example 1: New Token Launch**
- **Blocks**: 1000
- **Result**: 45 swaps, 3 big buys
- **Biggest Buy**: 0.8 ETH ($1,620 USD)
- **Price Impact**: 15% price increase during analysis period

### **Example 2: Established Token**
- **Blocks**: 15000
- **Result**: 234 swaps, 12 big buys
- **Biggest Buy**: 2.5 ETH ($5,062 USD)
- **Price Stability**: 3% price change over period

---

## 🔄 Advanced Usage

### **Batch Analysis**
For analyzing multiple tokens, you can modify the main script or create custom scripts using the underlying modules:

```python
from src.uniswap import UniswapExtractorFactory
from src.pricing.object_csv_writer import ObjectCSVWriter

# Create factory and extractor
factory = UniswapExtractorFactory()
extractor = factory.create_auto_extractor(pool_address, etherscan_api_key)

# Analyze token
result = extractor.analyze_token_complete(
    token_address=token_address,
    pool_address=pool_address,
    start_block=start_block,
    end_block=end_block,
    threshold_eth=0.1
)
```

### **Custom Thresholds**
Modify the `threshold_eth` parameter to detect different sized purchases:

```python
# Detect purchases >= 1 ETH
result = extractor.analyze_token_complete(
    token_address=token_address,
    pool_address=pool_address,
    start_block=start_block,
    end_block=end_block,
    threshold_eth=1.0  # 1 ETH threshold
)
```

---

## 🤝 Contributing

### **Adding New Features**
1. **New Data Sources**: Add clients in `src/client/`
2. **New Analysis**: Extend `src/pricing/` modules
3. **New Output Formats**: Create new writers in `src/pricing/`

### **Code Structure**
- **Abstract Base Classes**: Use for consistent interfaces
- **Factory Pattern**: For version-specific implementations
- **Modular Design**: Easy to extend and maintain

---

## 📄 License

[MIT License](LICENSE)

---

## 🆘 Support

For issues, questions, or contributions:
1. Check the troubleshooting section above
2. Review the code structure and examples
3. Create an issue with detailed error information

---

**Note**: This tool is for research and analysis purposes. Always verify data independently and consider market conditions when making trading decisions. 