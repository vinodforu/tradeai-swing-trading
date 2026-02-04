"""
screeners.py
------------
Generates swing trading signals from indicators and raw prices
and stores them in the signals table.

Design goals:
- NULL-safe
- Idempotent per day
- Explainable rules
- Stable interface for run_eod.py
"""

import pandas as pd
from src.db_utils import get_connection


# ============================================================
# Screener Rule Functions (ALL must be NULL-safe)
# ============================================================

def pullback_in_uptrend(row):
    """
    Swing Pullback in Uptrend:
    - Price above EMA50
    - EMA20 above EMA50
    - RSI between 40 and 60
    """
    if pd.isna(row["rsi"]) or pd.isna(row["ema20"]) or pd.isna(row["ema50"]):
        return False

    return (
        row["close"] > row["ema50"]
        and row["ema20"] > row["ema50"]
        and 40 <= row["rsi"] <= 60
    )


def momentum_breakout(row):
    """
    Momentum Continuation:
    - RSI above 60
    - Price above EMA20
    """
    if pd.isna(row["rsi"]) or pd.isna(row["ema20"]):
        return False

    return (
        row["rsi"] > 60
        and row["close"] > row["ema20"]
    )


# ============================================================
# Main Runner (Called by run_eod.py)
# ============================================================

def run_screeners():
    conn = get_connection()

    # Join indicators with prices (EOD snapshot)
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

    # Drop rows where core indicators are missing
    df = df.dropna(subset=["rsi", "ema20", "ema50", "close"])

    if df.empty:
        print("⚠️ No valid rows after indicator filtering.")
        conn.close()
        return

    signals = []

    for _, row in df.iterrows():
        trade_date = row["trade_date"].strftime("%Y-%m-%d")
        symbol = row["symbol"]

        # -----------------------------
        # Pullback in Uptrend
        # -----------------------------
        if pullback_in_uptrend(row):
            signals.append(
                (
                    trade_date,
                    symbol,
                    "PULLBACK_UPTREND",
                    1.0,
                    float(row["close"]),
                )
            )

        # -----------------------------
        # Momentum Breakout
        # -----------------------------
        if momentum_breakout(row):
            signals.append(
                (
                    trade_date,
                    symbol,
                    "MOMENTUM_BREAKOUT",
                    1.2,
                    float(row["close"]),
                )
            )

    if not signals:
        print("ℹ️ No swing trade signals generated today.")
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
