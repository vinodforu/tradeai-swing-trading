def pullback_screener(df):
    latest = df.iloc[-1]
    
    conditions = [
        latest["Close"] > latest["EMA50"],
        latest["EMA20"] > latest["EMA50"],
        40 <= latest["RSI"] <= 60,
        latest["Volume"] > df["Volume"].rolling(20).mean().iloc[-1]
    ]
    
    return all(conditions)
