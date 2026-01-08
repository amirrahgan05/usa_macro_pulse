
import os
import sys
import glob
from datetime import date
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

# -------------------------------------------------
# Paths
# -------------------------------------------------
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT, "src")
DATA_DIR = os.path.join(ROOT, "data")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
FORECAST_DIR = os.path.join(DATA_DIR, "forecasted")

sys.path.append(SRC_DIR)

# -------------------------------------------------
# Import pipeline steps
# -------------------------------------------------
from collect_data import main as collect_main
from processed_data import main as process_main
from forecast_data import main as forecast_main

# -------------------------------------------------
# Data freshness check
# -------------------------------------------------
def data_is_fresh():
    if not os.path.exists(PROCESSED_DIR):
        return False

    files = glob.glob(os.path.join(PROCESSED_DIR, "*.csv"))
    if not files:
        return False

    latest = max(files, key=os.path.getmtime)
    latest_date = date.fromtimestamp(os.path.getmtime(latest))
    return latest_date == date.today()

# -------------------------------------------------
# Run pipeline if needed
# -------------------------------------------------
if not data_is_fresh():
    print("🔄 Running daily data pipeline...")
    collect_main()
    process_main()
    forecast_main()
else:
    print("✅ Using cached daily data")

# -------------------------------------------------
# Load CSV helpers
# -------------------------------------------------
def load_csvs(path):
    files = glob.glob(os.path.join(path, "*.csv"))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    # 
    df = df.dropna(subset=["date"]).sort_values("date")
    return df

processed = load_csvs(PROCESSED_DIR)
forecasted = load_csvs(FORECAST_DIR)

# -------------------------------------------------
# Symbols & Labels
# -------------------------------------------------
symbols = []

if not processed.empty and "symbol" in processed.columns:
    symbols.extend(processed["symbol"].unique())

if not forecasted.empty and "symbol" in forecasted.columns:
    symbols.extend(forecasted["symbol"].unique())

symbols = sorted(set(symbols))

LABELS = {
     "BITCOIN": "bitcoin",
    "ETHEREUM": "ethereum",
    "BINANCECOIN": "binancecoin",
    "SOLANA": "solana",
    "RIPPLE": "ripple",
}

# -------------------------------------------------
# Style & palette
# -------------------------------------------------
BG = "#0e1117"
CARD_BG = "#1a1f2b"
ACCENT = "#11cdef"       # 
FORECAST_COLOR = "#ffa500"  # 
POS = "#2ecc71"          #
NEG = "#e74c3c"          # 
GRID = "#333"

def base_layout(fig):
    fig.update_layout(
        template="plotly_dark",
        margin=dict(l=20, r=20, t=50, b=30),
        legend=dict(orientation="h", x=0.5, xanchor="center", y=-0.2),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor=GRID),
        font=dict(family="Segoe UI, Roboto, Helvetica, Arial, sans-serif"),
        plot_bgcolor=BG,
        paper_bgcolor=BG,
    )
    return fig

# -------------------------------------------------
# Figure builders
# -------------------------------------------------
def build_price_figure(df_real, df_fc, symbol):
    fig = go.Figure()

    # Actual
    if not df_real.empty and "price" in df_real.columns:
        fig.add_trace(go.Scatter(
            x=df_real["date"],
            y=df_real["price"],
            mode="lines",
            name="Actual",
            line=dict(color=ACCENT, width=2.4),
            hovertemplate="Date: %{x|%Y-%m-%d}<br>Price: %{y:.2f}<extra></extra>",
        ))

    # Forecast
    if not df_fc.empty and "forecast" in df_fc.columns:
        fig.add_trace(go.Scatter(
            x=df_fc["date"],
            y=df_fc["forecast"],
            mode="lines",
            name="Forecast",
            line=dict(color=FORECAST_COLOR, dash="dash", width=2.2),
            hovertemplate="Date: %{x|%Y-%m-%d}<br>Forecast: %{y:.2f}<extra></extra>",
        ))

        # Confidence band
        if {"ci_lower", "ci_upper"}.issubset(df_fc.columns):
            fig.add_trace(go.Scatter(
                x=df_fc["date"],
                y=df_fc["ci_upper"],
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip"
            ))
            fig.add_trace(go.Scatter(
                x=df_fc["date"],
                y=df_fc["ci_lower"],
                fill="tonexty",
                fillcolor="rgba(255,165,0,0.18)",
                name="Confidence band",
                line=dict(width=0),
                hoverinfo="skip"
            ))

    title_symbol = LABELS.get(symbol, symbol)
    fig.update_layout(title=f"{title_symbol} — Price & Forecast")
    return base_layout(fig)

def build_change_figure(df_real, symbol):
    fig = go.Figure()
    cols = ["Daily_Change_%", "Weekly_Change_7d_%", "Monthly_Change_30d_%"]

    # 
    available = [c for c in cols if c in df_real.columns]
    if not available:
        fig.update_layout(title="Percentage changes (data not available)")
        return base_layout(fig)

    # 
    for col in available:
        values = df_real[col]
        colors = [POS if v >= 0 else NEG for v in values]
        fig.add_trace(go.Bar(
            x=df_real["date"],
            y=values,
            name=col.replace("_", " "),
            marker=dict(color=colors),
            hovertemplate="Date: %{x|%Y-%m-%d}<br>% Change: %{y:.2f}%<extra></extra>",
        ))

    title_symbol = LABELS.get(symbol, symbol)
    fig.update_layout(
        barmode="group",
        title=f"{title_symbol} — Percentage changes",
        yaxis=dict(title="%"),
    )
    return base_layout(fig)

# -------------------------------------------------
# Dash app
# -------------------------------------------------
app = Dash(__name__)
app.title = "Macro Pulse"

app.layout = html.Div(
    style={"backgroundColor": BG, "minHeight": "100vh", "padding": "18px"},
    children=[
        html.Div(
            style={
                "backgroundColor": CARD_BG,
                "padding": "18px",
                "borderRadius": "12px",
                "boxShadow": "0 6px 16px rgba(0,0,0,0.35)"
            },
            children=[
                html.H2(
                    "🌍 Macro Pulse Dashboard",
                    style={"textAlign": "center", "color": ACCENT, "marginBottom": "16px"}
                ),

                html.Div(
                    style={"display": "flex", "gap": "12px", "marginBottom": "12px"},
                    children=[
                        dcc.Dropdown(
                            id="symbol",
                            options=[{"label": LABELS.get(s, s), "value": s} for s in symbols],
                            value=symbols[0] if symbols else None,
                            clearable=False,
                            style={"flex": "1"}
                        ),
                        dcc.Tabs(
                            id="tabs",
                            value="price",
                            children=[
                                dcc.Tab(label="📈 Price & Forecast", value="price"),
                                dcc.Tab(label="📊 Percentage Changes", value="changes"),
                            ],
                            style={"flex": "2"}
                        ),
                    ]
                ),

                dcc.Loading(
                    type="circle",
                    color=ACCENT,
                    children=dcc.Graph(id="chart", style={"height": "74vh"})
                )
            ]
        )
    ]
)

# -------------------------------------------------
# Callbacks
# -------------------------------------------------
@app.callback(
    Output("chart", "figure"),
    Input("symbol", "value"),
    Input("tabs", "value")
)
def update_chart(symbol, tab):
    # 
    if not symbol:
        fig = go.Figure()
        fig.update_layout(
            template="plotly_dark",
            title="⚠ Data not available",
            title_font=dict(color="#FF6B6B", size=20),
            plot_bgcolor=BG,
            paper_bgcolor=BG
        )
        return fig

    df_real = processed[processed["symbol"] == symbol] if not processed.empty else pd.DataFrame()
    df_fc = forecasted[forecasted["symbol"] == symbol] if not forecasted.empty else pd.DataFrame()

    if tab == "price":
        return build_price_figure(df_real, df_fc, symbol)
    else:
        return build_change_figure(df_real, symbol)

# -------------------------------------------------
# Entry point
# -------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)