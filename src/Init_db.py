import sqlite3
import os

DB_DIR = "db"
DB_PATH = os.path.join(DB_DIR, "tradeai.db")


def init_db():
    os.makedirs(DB_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1️⃣ Raw OHLCV data
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_prices (
        trade_date TEXT,
        symbol TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        PRIMARY KEY (trade_date, symbol)
    );
    """)

    # 2️⃣ Indicator snapshot (EOD)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS indicators (
        trade_date TEXT,
        symbol TEXT,
        rsi REAL,
        ema20 REAL,
        ema50 REAL,
        atr REAL,
        PRIMARY KEY (trade_date, symbol)
    );
    """)

    # 3️⃣ Screener signals
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        trade_date TEXT,
        symbol TEXT,
        strategy TEXT,
        score REAL,
        close REAL
    );
    """)

    conn.commit()
    conn.close()

    print("✅ Database initialized successfully.")


if __name__ == "__main__":
    init_db()
