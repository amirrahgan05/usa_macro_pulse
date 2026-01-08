
import os
import glob
import pandas as pd

# --------------------------------
# Absolute paths (DEPLOY SAFE)
# --------------------------------
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_PATH = os.path.join(ROOT_DIR, "data", "raw")
PROCESSED_PATH = os.path.join(ROOT_DIR, "data", "processed")

os.makedirs(PROCESSED_PATH, exist_ok=True)

# --------------------------------
# Helpers
# --------------------------------
def infer_symbol_from_filename(file_path):
    return os.path.splitext(os.path.basename(file_path))[0]

def validate_and_fix_columns(df, file_path):
    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Date column
    possible_date_cols = [c for c in df.columns if c.lower() in {"date", "datetime"}]
    if possible_date_cols and "date" not in df.columns:
        df = df.rename(columns={possible_date_cols[0]: "date"})

    # Price column
    possible_price_cols = [c for c in df.columns if c.lower() in {"price", "close", "adj close"}]
    if possible_price_cols and "price" not in df.columns:
        df = df.rename(columns={possible_price_cols[0]: "price"})

    # Symbol column
    if "symbol" not in df.columns:
        df["symbol"] = infer_symbol_from_filename(file_path)

    return df

# --------------------------------
# Core processing
# --------------------------------
def process_file(file_path):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ Cannot read CSV {file_path}: {e}")
        return None

    if df.empty:
        print(f"⚠ Empty file: {file_path}")
        return None

    df = validate_and_fix_columns(df, file_path)

    required = {"date", "symbol", "price"}
    if not required.issubset(df.columns):
        print(f"⚠ Missing columns in {file_path}: {required - set(df.columns)}")
        return None

    # Date handling
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["date"] = df["date"].dt.tz_convert(None)

    # Cleaning
    before = len(df)
    df = df.dropna(subset=["date", "price"]).sort_values("date")
    after = len(df)

    if after == 0:
        print(f"⚠ No valid rows after cleaning: {file_path}")
        return None

    if before != after:
        print(f"ℹ Dropped {before - after} rows from {file_path}")

    # Daily frequency
    df = df.set_index("date").asfreq("D")
    df["symbol"] = df["symbol"].ffill().bfill()
    df["price"] = pd.to_numeric(df["price"], errors="coerce").ffill().bfill()

    # Percentage changes
    df["Daily_Change_%"] = df["price"].pct_change(1) * 100
    df["Weekly_Change_7d_%"] = df["price"].pct_change(7) * 100
    df["Monthly_Change_30d_%"] = df["price"].pct_change(30) * 100

    df.reset_index(inplace=True)

    print(
        f"✅ Processed {os.path.basename(file_path)} | "
        f"rows={len(df)} | "
        f"{df['date'].min().date()} → {df['date'].max().date()}"
    )

    return df

# --------------------------------
# Save
# --------------------------------
def save_processed(df, filename):
    if df is None or df.empty:
        print(f"⚠ Skipping save for {filename}")
        return

    out_path = os.path.join(PROCESSED_PATH, filename)
    df.to_csv(out_path, index=False)
    print(f"💾 Saved → {out_path}")

# --------------------------------
# Main runner
# --------------------------------
def main():
    files = glob.glob(os.path.join(RAW_PATH, "*.csv"))
    if not files:
        print(f"⚠ No raw files found in {RAW_PATH}")
        return

    print(f"🔎 Found {len(files)} raw files")

    for f in files:
        df = process_file(f)
        save_processed(df, os.path.basename(f))

    print("\n✨ All processed data ready")

# --------------------------------
# Entry point
# --------------------------------
if __name__ == "__main__":
    main()