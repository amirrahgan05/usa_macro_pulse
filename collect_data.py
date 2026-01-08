
import os
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

RAW_PATH = os.path.join("data", "raw")
os.makedirs(RAW_PATH, exist_ok=True)

# ------------------------------
# 1) Fetch Stocks & Commodities
# ------------------------------
def fetch_yf(symbol, period="1mo", interval="1d"):
    """Fetch price history for stocks or commodities using yfinance"""
    try:
        df = yf.download(symbol, period=period, interval=interval)
        if df.empty:
            print(f"⚠ No data returned for {symbol}")
            return None
        df.reset_index(inplace=True)
        df = df.rename(columns={"Date": "date", "Close": "price"})
        df["symbol"] = symbol
        return df[["date", "symbol", "price"]]
    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        return None

# ------------------------------
# 2) Fetch Crypto (CoinGecko history)
# ------------------------------
def fetch_crypto(coin_id="bitcoin", days=30):
    """Fetch crypto daily history for last N days from CoinGecko"""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days, "interval": "daily"}
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            print(f"⚠ Failed to fetch crypto data: {coin_id}")
            return None
        data = response.json()["prices"]
        df = pd.DataFrame(data, columns=["timestamp", "price"])
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["symbol"] = coin_id.upper()
        return df[["date", "symbol", "price"]]
    except Exception as e:
        print(f"❌ Error fetching crypto {coin_id}: {e}")
        return None

# ------------------------------
# 3) Add % changes (daily, weekly, monthly)
# ------------------------------
def add_changes(df):
    df = df.sort_values("date").copy()
    df["Daily_Change_%"] = df["price"].pct_change(1) * 100
    df["Weekly_Change_7d_%"] = df["price"].pct_change(7) * 100
    df["Monthly_Change_30d_%"] = df["price"].pct_change(30) * 100
    return df

# ------------------------------
# 4) Save to data/raw
# ------------------------------
def save_raw(df, filename):
    if df is None or df.empty:
        print(f"⚠ No data to save for {filename}")
        return
    out_path = os.path.join(RAW_PATH, f"{filename}.csv")
    df.to_csv(out_path, index=False)
    print(f"✅ Saved {filename} → {out_path}")

# ------------------------------
# 5) Main Runner
# ------------------------------
def main():
    print("📈 Fetching stock & commodity data...")
    targets_yf = {
        "AAPL": "Apple",
        "MSFT": "Microsoft",
        "GC=F": "Gold",
        "CL=F": "Crude Oil",
        "^GSPC": "S&P500"
    }

    for symbol, name in targets_yf.items():
        print(f" → Getting {name} ({symbol})...")
        df = fetch_yf(symbol)
        if df is not None:
            df = add_changes(df)
        save_raw(df, symbol)

    print("\n💰 Fetching crypto data...")
    cryptos = ["bitcoin", "ethereum", "solana"]

    for coin in cryptos:
        print(f" → Getting {coin}...")
        df = fetch_crypto(coin, days=30)
        if df is not None:
            df = add_changes(df)
        save_raw(df, coin.upper())

    print("\n✨ All data saved to data/raw/")

if __name__ == "__main__":
    main()