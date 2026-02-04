"""
screeners.py
------------
Defines swing trading screeners and stores signals in DB.
"""

import pandas as pd

from src.db_utils import get_connection


# =============================
# Screener Rules
# =============================

def pullback_in_uptrend(row):
    """
    Pullback in an uptrend:
    - Price above EMA50
    - EMA20 above EMA50
    - RSI between 40 and 60
    """
    return (
        row["close"] > row["ema50"]
        and row["ema20"] > row["ema50"]
        and 40 <= row["rsi"] <= 60
    )


def momentum_breakout(row):
    """
    Momentum continuation:
    - RSI > 60
    - Close above EMA20
    """
    return (
        row["rsi"] > 60
        and row["close"] > row["ema20"]
    )


# =============================
# Main Runner
# =============================

def run_screeners():
    conn = get_connection()

    df = pd.read_sql(
        """
        SELECT
            i.trade_date,
            i.symbol,
            i.rsi,
            i.ema20,
            i.ema50,
            i.atr,
            r.close
        FROM indicators i
        JOIN raw_prices r
          ON i.trade_date = r.trade_date
         AND i.symbol = r.symbol
        """,
        conn,
        parse_dates=["trade_date"],
    )

    if df.empty:
        print("⚠️ No indicator data found. Skipping screeners.")
        conn.close()
        return

    signals = []

    for _, row in df.iterrows():
        trade_date = row["trade_date"].strftime("%Y-%m-%d")

        # --- Pullback Screener ---
        if pullback_in_uptrend(row):
            signals.append(
                (
                    trade_date,
                    row["symbol"],
                    "PULLBACK_UPTREND",
                    1.0,
                    row["close"],
                )
            )

        # --- Momentum Screener ---
        if momentum_breakout(row):
            signals.append(
                (
                    trade_date,
                    row["symbol"],
                    "MOMENTUM_BREAKOUT",
                    1.2,
                    row["close"],
                )
            )

    if not signals:
        print("ℹ️ No swing signals generated today.")
        conn.close()
        return

    cursor = conn.cursor()

    cursor.executemany(
        """
        INSERT INTO signals
        (trade_date, symbol, strategy, score, close)
        VALUES (?, ?, ?, ?, ?)
        """,
        signals,
    )

    conn.commit()
    conn.close()

    print(f"📈 Generated {len(signals)} swing trade signals")
