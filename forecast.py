
# src/forecast.py
import os
import glob
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DIR = os.path.join(ROOT, "data", "processed")
FORECAST_DIR = os.path.join(ROOT, "data", "forecasted")
os.makedirs(FORECAST_DIR, exist_ok=True)

FORECAST_STEPS = 30
ARIMA_ORDER = (1, 1, 1)

# -------------------------------
# Time series preparation
# -------------------------------
def prepare_series(df):
    if not {"date", "price"}.issubset(df.columns):
        print("⚠ Missing required columns in dataframe")
        return None

    s = df.copy()
    s["date"] = pd.to_datetime(s["date"], errors="coerce", utc=True)
    s["date"] = s["date"].dt.tz_convert(None)

    s = s.dropna(subset=["date", "price"]).sort_values("date")
    s = s.set_index("date").asfreq("D")
    s["price"] = pd.to_numeric(s["price"], errors="coerce").ffill().bfill()

    if len(s["price"].dropna()) < 20:
        print("⚠ Not enough data points")
        return None

    return s["price"]

# -------------------------------
#Predicting a symbol
# -------------------------------
def forecast_one_symbol(symbol, df_symbol):
    y = prepare_series(df_symbol)
    if y is None:
        print(f"⚠ Skipping forecast for {symbol}")
        return None

    try:
        model = ARIMA(y, order=ARIMA_ORDER)
        fit = model.fit()
        fc = fit.get_forecast(steps=FORECAST_STEPS)
        fc_mean = fc.predicted_mean
        fc_ci = fc.conf_int()

        out = pd.DataFrame({
            "date": fc_mean.index,
            "symbol": symbol,
            "forecast": fc_mean.values,
            "ci_lower": fc_ci.iloc[:, 0].values,
            "ci_upper": fc_ci.iloc[:, 1].values
        })

        out_path = os.path.join(FORECAST_DIR, f"forecast_{symbol}.csv")
        out.to_csv(out_path, index=False)
        print(f"✅ Forecast saved: {out_path}")
        return out
    except Exception as e:
        print(f"❌ Forecast failed for {symbol}: {e}")
        return None

# -------------------------------
# main runner
# -------------------------------
def main():
    files = glob.glob(os.path.join(PROCESSED_DIR, "*.csv"))
    if not files:
        print(f"⚠ No processed files found in {PROCESSED_DIR}")
        return

    all_data = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_data.append(df)
        except Exception as e:
            print(f"⚠ Failed to read {f}: {e}")

    if not all_data:
        print("⚠ No readable processed files.")
        return

    data = pd.concat(all_data, ignore_index=True)

    if not {"date", "symbol", "price"}.issubset(data.columns):
        print("❌ Missing required columns in processed data.")
        return

    for symbol, df_symbol in data.groupby("symbol"):
        forecast_one_symbol(symbol, df_symbol)

    print("\n✨ All forecasts saved to data/forecasted/")

# -------------------------------
# Direct implementation
# -------------------------------
if __name__ == "__main__":
    main()