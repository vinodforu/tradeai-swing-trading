"""
indicators.py
-------------
Computes technical indicators from raw_prices
and stores daily snapshots into the indicators table.
"""

import pandas as pd

from src.db_utils import get_connection


def compute_and_store_indicators():
    conn = get_connection()

    # Load raw prices
    df = pd.read_sql(
        """
        SELECT trade_date, symbol, open, high, low, close, volume
        FROM raw_prices
        ORDER BY symbol, trade_date
        """,
        conn,
        parse_dates=["trade_date"],
    )

    if df.empty:
        print("⚠️ No raw price data found. Skipping indicators.")
        conn.close()
        return

    indicators_rows = []

    for symbol, sdf in df.groupby("symbol"):
        sdf = sdf.sort_values("trade_date").copy()

        # === Indicators ===
        sdf["ema20"] = sdf["close"].ewm(span=20).mean()
        sdf["ema50"] = sdf["close"].ewm(span=50).mean()

        delta = sdf["close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.rolling(14).mean()
        avg_loss = loss.rolling(14).mean()
        rs = avg_gain / avg_loss
        sdf["rsi"] = 100 - (100 / (1 + rs))

        tr = (
            sdf["high"] - sdf["low"]
        ).abs()
        sdf["atr"] = tr.rolling(14).mean()

        # Take only the latest EOD snapshot
        latest = sdf.iloc[-1]

        indicators_rows.append(
            (
                latest["trade_date"].strftime("%Y-%m-%d"),
                symbol,
                float(latest["rsi"]),
                float(latest["ema20"]),
                float(latest["ema50"]),
                float(latest["atr"]),
            )
        )

    cursor = conn.cursor()

    cursor.executemany(
        """
        INSERT OR REPLACE INTO indicators
        (trade_date, symbol, rsi, ema20, ema50, atr)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        indicators_rows,
    )

    conn.commit()
    conn.close()

    print(f"📊 Indicators stored for {len(indicators_rows)} symbols")
