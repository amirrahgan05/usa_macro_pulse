
import os
import time
import requests
import pandas as pd

RAW_PATH = os.path.join("data", "raw")
os.makedirs(RAW_PATH, exist_ok=True)

CRYPTO_MAP = {
    "BITCOIN": "bitcoin",
    "ETHEREUM": "ethereum",
    "BINANCECOIN": "binancecoin",
    "SOLANA": "solana",
    "RIPPLE": "ripple",
}

def fetch_crypto(coin_id, symbol, days=30, retries=3):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days, "interval": "daily"}

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=20)

            if r.status_code != 200:
                print(f"❌ {symbol} failed (status {r.status_code})")
                time.sleep(2)
                continue

            prices = r.json().get("prices", [])
            if not prices:
                print(f"⚠ {symbol} empty data (attempt {attempt})")
                time.sleep(2)
                continue

            df = pd.DataFrame(prices, columns=["timestamp", "price"])
            df["date"] = pd.to_datetime(df["timestamp"], unit="ms")
            df["symbol"] = symbol
            return df[["date", "symbol", "price"]]

        except Exception as e:
            print(f"❌ {symbol} error (attempt {attempt}): {e}")
            time.sleep(2)

    print(f"🚫 {symbol} skipped after {retries} attempts")
    return None


def main():
    for symbol, coin_id in CRYPTO_MAP.items():
        print(f"🪙 Fetching {symbol}")
        df = fetch_crypto(coin_id, symbol)

        if df is not None:
            out = os.path.join(RAW_PATH, f"{symbol}.csv")
            df.to_csv(out, index=False)
            print(f"✅ Saved {symbol}")
        else:
            print(f"⚠ No data saved for {symbol}")


if __name__ == "__main__":
    main()