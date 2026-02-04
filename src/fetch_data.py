"""
fetch_data.py
--------------
Fetches End-of-Day (EOD) OHLCV data for a defined stock universe
and stores raw data for downstream screening & AI pipelines.
"""

import os
from datetime import datetime
import pandas as pd
import yfinance as yf

from config import STOCKS, DATA_LOOKBACK_DAYS


RAW_DATA_DIR = "data/raw"


def ensure_dirs():
    """Ensure required directories exist."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)


def fetch_eod_data():
    """
    Fetch EOD data for all stocks in the universe.
    Returns a dictionary: {symbol: DataFrame}
    """
    print("Fetching EOD market data...")

    data = yf.download(
        tickers=STOCKS,
        period=f"{DATA_LOOKBACK_DAYS}d",
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        threads=True
    )

    return data


def split_symbol_data(data):
    """
    Split multi-ticker dataframe into per-symbol dataframes.
    """
    symbol_dfs = {}

    for symbol in data.columns.levels[0]:
        df = data[symbol].copy()
        df.dropna(inplace=True)

        if len(df) < 50:
            # Not enough data for indicators
            continue

        df.reset_index(inplace=True)
        df["symbol"] = symbol

        symbol_dfs[symbol] = df

    return symbol_dfs


def save_raw_data(symbol_dfs):
    """
    Save raw OHLCV data to CSV (one file per symbol per day).
    """
    trade_date = datetime.now().strftime("%Y-%m-%d")

    for symbol, df in symbol_dfs.items():
        file_name = f"{symbol.replace('.', '_')}_{trade_date}.csv"
        file_path = os.path.join(RAW_DATA_DIR, file_name)

        df.to_csv(file_path, index=False)

    print(f"Saved raw data for {len(symbol_dfs)} symbols.")


def run():
    ensure_dirs()

    data = fetch_eod_data()
    symbol_dfs = split_symbol_data(data)
    save_raw_data(symbol_dfs)

    print("EOD data fetch completed successfully.")


if __name__ == "__main__":
    run()
