# Ethereum Token Analyzer

A Python tool to analyze Ethereum ERC-20 tokens by fetching their recent transactions and liquidity, filtering for significant buys, and saving results incrementally to a CSV. Designed for research, trading, and token monitoring.

---

## 🚀 Features

- **Fetches last N (default 50) token transactions** from Etherscan for any ERC-20 contract address.
- **Filters for big buys** (≥ 0.1 ETH by default) and shows details.
- **Saves results to a CSV**: one row per address, new columns for each new analysis (timestamped).
- **Deduplication**: Only new transactions (not seen in previous analyses) are saved in each new column set.
- **Backs up the CSV** before each write.
- **Batch update mode**: update all tracked addresses in one go.
- **Modular code**: easy to extend for new data sources or filters.

---

## 📁 Folder Structure

```
├── main.py                  # Main script to run the analysis
├── requirements.txt         # Python dependencies
├── README.md                # This file
├── data/
│   └── informe.csv          # Output CSV (created after first run)
└── src/
    ├── client/
    │   └── etherscan_client.py   # Etherscan API client
    ├── filters/
    │   └── big_buy_filter.py     # Filtering logic for big buys
    ├── csv_utils/
    │   └── csv_handler.py        # CSV backup, deduplication, and writing
    └── config.py                 # API key config (imported from .env)
```

---

## ⚙️ Installation & Setup

1. **Clone the repo**

```bash
git clone <repo-url>
cd <repo-folder>
```

2. **Create and activate a virtual environment**

```bash
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

4. **Configure API keys**

- Create a `.env` file in the root with your Etherscan API key:

```
ETHERSCAN_API_KEY=your_etherscan_key_here
```

---

## 🏃 Usage

Run the main script:

```bash
python3 main.py
```

When you run the script, you will see a menu:

```
=== Ethereum Token Analyzer ===
1. Track/update a single address
2. Update ALL addresses in the CSV (batch mode)
Select an option (1 or 2):
```

- **Option 1:**
  - Enter an ERC-20 token address (format: `0x...`).
  - The script will fetch, analyze, and save results for that address only.
  - If the address already exists in the CSV, new columns will be added for the new analysis.

- **Option 2:**
  - The script will read all addresses currently tracked in `data/informe.csv`.
  - It will fetch, analyze, and update each address in sequence, adding new columns for each new analysis.
  - Progress and a summary will be shown at the end.
  - If the CSV does not exist or is empty, you will be notified.

---

## 📝 CSV Output Format

- **One row per address**
- **Columns:**
    - `Address`
    - `Timestamp_1`, `HasBuyOver0.1ETH_1`, `BigBuyDetails_1`, `NumTransactions_1`, ...
    - `Timestamp_2`, `HasBuyOver0.1ETH_2`, ... (for each new analysis)
- **Deduplication:** Only new transactions (not seen in previous analyses) are saved in each new column set.

**Example:**

| Address | Timestamp_1 | HasBuyOver0.1ETH_1 | BigBuyDetails_1 | NumTransactions_1 | Timestamp_2 | HasBuyOver0.1ETH_2 | BigBuyDetails_2 | NumTransactions_2 |
|---------|-------------|--------------------|-----------------|-------------------|-------------|--------------------|-----------------|-------------------|
| 0xABC   | ...         | TRUE               | [...]           | 50                | ...         | FALSE              | []              | 3                 |

---

## 🔄 Typical User Flows

1. **Analyze a new address:**
    - New row is created in CSV with `_1` columns.
2. **Re-analyze the same address:**
    - New columns (`_2`, `_3`, ...) are added to the same row, only for new transactions since the last analysis.
3. **Batch update:**
    - Use option 2 to update all tracked addresses in one go.
4. **Analyze another address:**
    - New row is created.

---

## 🛠️ Troubleshooting

- **Binary incompatibility error (numpy/pandas):**
    - Run:
      ```bash
      pip install --upgrade --force-reinstall numpy pandas
      ```
    - If the error persists, recreate your virtual environment:
      ```bash
      deactivate
      rm -rf venv
      python3 -m venv venv
      source venv/bin/activate
      pip install -r requirements.txt
      ```
- **API errors:**
    - Check your `.env` file and API key validity.
    - Etherscan rate limits may apply.
- **No transactions found:**
    - The token may be new or have little activity.

---

## 🧩 Extending & Contributing

- Add new filters in `src/filters/`.
- Add new data sources (e.g., DEXTools, Coingecko) in `src/client/`.
- PRs and issues welcome!

---

## 📄 License

[MIT License](LICENSE) (placeholder) 