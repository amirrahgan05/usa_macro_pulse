
# src/processed_data.py
import os
import glob
import pandas as pd

RAW_PATH = os.path.join("data", "raw")
PROCESSED_PATH = os.path.join("data", "processed")
os.makedirs(PROCESSED_PATH, exist_ok=True)

def infer_symbol_from_filename(file_path):
    name = os.path.basename(file_path)
    sym = os.path.splitext(name)[0]
    return sym

def validate_and_fix_columns(df, file_path):
    cols = set(df.columns.str.lower())
    possible_date_cols = [c for c in df.columns if c.lower() in {"date", "datetime"}]
    if possible_date_cols and "date" not in df.columns:
        df = df.rename(columns={possible_date_cols[0]: "date"})
    possible_price_cols = [c for c in df.columns if c.lower() in {"price", "close", "adj close"}]
    if possible_price_cols and "price" not in df.columns:
        df = df.rename(columns={possible_price_cols[0]: "price"})
    if "symbol" not in df.columns:
        df["symbol"] = infer_symbol_from_filename(file_path)
    return df

def process_file(file_path):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ Cannot read CSV {file_path}: {e}")
        return None

    if df is None or df.empty:
        print(f"⚠ Empty or None data in {file_path}")
        return None

    # Validating and correcting column names
    df = validate_and_fix_columns(df, file_path)

    missing = {"date", "symbol", "price"} - set(df.columns)
    if missing:
        print(f"⚠ Missing columns in {file_path}: {missing}. Columns present: {list(df.columns)}")
        return None

    # Convert dates and remove tz
    try:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
        df["date"] = df["date"].dt.tz_convert(None)
    except Exception as e:
        print(f"⚠ Date parsing/tz error in {file_path}: {e}")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Clearing the base
    before = len(df)
    df = df.dropna(subset=["date", "price"]).sort_values("date")
    after = len(df)
    if after == 0:
        print(f"⚠ After cleaning, no rows left in {file_path}.")
        return None
    if after < before:
        print(f"ℹ Dropped {before - after} rows due to missing date/price in {file_path}.")

    #Convert to daily frequency and fill in the gaps
    df = df.set_index("date").asfreq("D")
    df["symbol"] = df["symbol"].ffill().bfill()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["price"] = df["price"].ffill().bfill()

    # Percentage change calculations
    df["Daily_Change_%"] = df["price"].pct_change(1) * 100
    df["Weekly_Change_7d_%"] = df["price"].pct_change(7) * 100
    df["Monthly_Change_30d_%"] = df["price"].pct_change(30) * 100

    df.reset_index(inplace=True)

    # Debug report
    print(f"✅ Processed {file_path}: rows={len(df)}, "
          f"date range={df['date'].min().date()} → {df['date'].max().date()}, "
          f"symbol={df['symbol'].iloc[0]}")

    return df

def save_processed(df, filename):
    if df is None or df.empty:
        print(f"⚠ Skipping save for {filename} (empty).")
        return
    out_path = os.path.join(PROCESSED_PATH, filename)
    try:
        df.to_csv(out_path, index=False)
        print(f"💾 Saved processed → {out_path}")
    except Exception as e:
        print(f"❌ Failed to save {filename}: {e}")

def main():
    files = glob.glob(os.path.join(RAW_PATH, "*.csv"))
    if not files:
        print(f"⚠ No raw files found in {RAW_PATH}")
        return

    print(f"🔎 Found {len(files)} raw files.")
    for f in files:
        print(f"📂 Processing {f} ...")
        df = process_file(f)
        filename = os.path.basename(f)
        save_processed(df, filename)

    print("\n✨ All processed data saved to data/processed/")

if __name__ == "__main__":
    main()