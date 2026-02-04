"""
run_eod.py
----------
End-of-Day orchestration script.
This file should remain STABLE.
"""

from src.fetch_data import fetch_and_store_raw_prices
from src.indicators import compute_and_store_indicators
from src.screeners import run_screeners


def run_eod():
    print("🚀 Starting EOD pipeline")

    fetch_and_store_raw_prices()
    compute_and_store_indicators()
    run_screeners()

    print("✅ EOD pipeline completed")


if __name__ == "__main__":
    run_eod()
