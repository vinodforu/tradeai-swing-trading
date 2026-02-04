from src.fetch_data import fetch_eod_data
from src.indicators import add_indicators
from src.screeners import pullback_screener

def run_eod():
    data = fetch_eod_data()
    shortlisted = []

    for symbol in data.columns.levels[0]:
        df = data[symbol].dropna()
        df = add_indicators(df)
        
        if pullback_screener(df):
            shortlisted.append(symbol)

    print("Swing trade candidates:", shortlisted)

if __name__ == "__main__":
    run_eod()
