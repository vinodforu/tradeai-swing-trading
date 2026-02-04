import yfinance as yf
from config import STOCKS, DATA_LOOKBACK_DAYS
from src.db_utils import get_connection


def fetch_and_store_raw_prices():
    data = yf.download(
        tickers=STOCKS,
        period=f"{DATA_LOOKBACK_DAYS}d",
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        threads=True
    )

    conn = get_connection()
    cursor = conn.cursor()

    for symbol in data.columns.levels[0]:
        df = data[symbol].dropna()
        if df.empty:
            continue

        latest = df.iloc[-1]
        trade_date = latest.name.strftime("%Y-%m-%d")

        cursor.execute(
            """
            INSERT OR REPLACE INTO raw_prices
            (trade_date, symbol, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_date,
                symbol,
                float(latest["Open"]),
                float(latest["High"]),
                float(latest["Low"]),
                float(latest["Close"]),
                int(latest["Volume"]),
            ),
        )

    conn.commit()
    conn.close()

    print("📥 Raw prices stored")
