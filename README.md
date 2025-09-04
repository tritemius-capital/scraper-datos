# Token Price Analyzer

Analiza precios y transacciones de tokens en Uniswap V2/V3.

## Estructura

```
datos/
  ├── tokens_ejemplo_real.csv    # Lista de tokens a analizar
  ├── historical_price_eth/      # Precios históricos de ETH
  │   └── eth_historical_prices.csv
  └── data/                      # Resultados del análisis
      └── token_analysis.csv
```

## Formato de archivos

### tokens_ejemplo_real.csv
```csv
version,nombre,par
v2,0x8cdda18f0fd28096c839efc487456b50702f7d09,0xcf072d3e71a7799b235f126dd7c1afbbf65c3555
```

### eth_historical_prices.csv
```csv
timestamp,datetime,price_usd
1752745091,2025-09-04 16:38:11,2992.87
```

### token_analysis.csv
```csv
token_address,pool_address,version,timestamp,block,price_eth,price_usd,volume_eth,volume_usd,type
0x8cdda18f0fd28096c839efc487456b50702f7d09,0xcf072d3e71a7799b235f126dd7c1afbbf65c3555,v2,1752745091,23290418,0.000000013438768651,0.000040220487531366,0.19752475247524753,591.23,BUY
```

## Uso

```bash
# Analizar tokens
python3 main.py tokens_ejemplo_real.csv
``` 