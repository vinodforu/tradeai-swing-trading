import os
from datetime import datetime

import pandas as pd
import yfinance as yf

from config import STOCKS, DATA_LOOKBACK_DAYS
from src.db_utils import get_connection


def fetch_eod_data():
    """
    Fetch historical EOD data for all symbols.
    """
    data = yf.download(
        tickers=STOCKS,
        period=f"{DATA_LOOKBACK_DAYS}d",
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        threads=True
    )
    return data


def insert_raw_price(trade_date, symbol, row):
    """
    Insert one EOD candle into raw_prices table.
    Idempotent because of PRIMARY KEY.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT OR REPLACE INTO raw_prices
        (trade_date, symbol, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_date,
            symbol,
            float(row["Open"]),
            float(row["High"]),
            float(row["Low"]),
            float(row["Close"]),
            int(row["Volume"]),
        ),
    )

    conn.commit()
    conn.close()


def run():
    print("📥 Fetching EOD data...")
    data = fetch_eod_data()

    inserted = 0

    for symbol in data.columns.levels[0]:
        df = data[symbol].dropna()

        if df.empty:
            continue

        # Take the latest completed candle
        latest_row = df.iloc[-1]
        trade_date = latest_row.name.strftime("%Y-%m-%d")

        insert_raw_price(trade_date, symbol, latest_row)
        inserted += 1

    print(f"✅ Inserted EOD data for {inserted} symbols.")


if __name__ == "__main__":
    run()
