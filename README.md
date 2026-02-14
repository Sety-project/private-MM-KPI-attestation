# Trade Monitor

A CCXT-based Python application that monitors private trades and live orders via WebSockets, calculates liquidity metrics, and sends them to a remote database.

## Features

- **Real-time Monitoring**: Listens to private trades and orders using CCXT Pro.
- **Local Data Management**: Maintains a rolling window of trades and order book snapshots.
- **`verify_trades`**: Calculates total traded volume within the `compute_window`.
- **`verify_orders`**: Calculates the distribution (quantiles) of open order sizes within specific price depths from the mid-price.
- **Automated Reporting**: Sends metrics to a remote DB every `compute_window`.

## Installation

1. Clone the repository.
2. Create a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install ccxt python-dotenv PyYAML pandas numpy
   ```
4. Set up your environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your API credentials
   ```

## Configuration

Modify `config.yaml` to adjust:
- `exchange_name`: The exchange to monitor (e.g., `binance`).
- `symbol`: The trading pair (e.g., `BTC/USDT:USDT`).
- `depths`: Price distances from the mid-price to analyze.
- `quantiles`: Quantile values for size distribution analysis.
- `compute_window`: Frequency of calculations and reporting (seconds).
- `record_window`: Maximum history to keep locally (seconds).

## Usage

Start the monitor:
```bash
python monitor.py
```

## Logic Overview

### Trade Verification
`verify_trades` sums up the value (amount * price) of all private trades detected within the `compute_window` (e.g., last 60 seconds).

### Order Verification
The app takes a snapshot of your open orders every second. For each snapshot, it sums the size of orders that are within `depth` of the current market mid-price (Average of Bid and Ask). 
`verify_orders` then looks at the history of these sums over the `record_window` and calculates the specified quantiles (e.g., p50, p90).
