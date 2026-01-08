
# src/app.py
import os
import glob
import pandas as pd
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objects as go

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DIR = os.path.join(ROOT, "data", "processed")
FORECAST_DIR = os.path.join(ROOT, "data", "forecasted")

def normalize_dates(df, col="date"):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        df[col] = df[col].dt.tz_convert(None)
    return df

def load_processed():
    files = glob.glob(os.path.join(PROCESSED_DIR, "*.csv"))
    if not files:
        return pd.DataFrame(columns=["date","symbol","price"])
    dfs = [pd.read_csv(f) for f in files]
    data = pd.concat(dfs, ignore_index=True)
    data = normalize_dates(data, "date")
    return data.dropna(subset=["date","price"]).sort_values(["symbol","date"])

def load_forecast():
    files = glob.glob(os.path.join(FORECAST_DIR, "*.csv"))
    if not files:
        return pd.DataFrame(columns=["date","symbol","forecast"])
    dfs = [pd.read_csv(f) for f in files]
    data = pd.concat(dfs, ignore_index=True)
    data = normalize_dates(data, "date")
    return data.dropna(subset=["date","forecast"]).sort_values(["symbol","date"])

processed = load_processed()
forecasted = load_forecast()
symbols = sorted(set(processed["symbol"].unique()).union(set(forecasted["symbol"].unique())))

app = Dash(__name__)
app.title = "Macro Pulse Dashboard"

app.layout = html.Div([
    html.H2("Macro Pulse Dashboard", style={"textAlign":"center"}),

    html.Div([
        dcc.Dropdown(
            id="symbol-dropdown",
            options=[{"label":s,"value":s} for s in symbols],
            value=symbols[0] if symbols else None,
            clearable=False
        ),
        dcc.DatePickerRange(
            id="date-range",
            start_date=str(processed["date"].min().date()) if not processed.empty else None,
            end_date=str(processed["date"].max().date()) if not processed.empty else None,
            display_format="YYYY-MM-DD"
        ),
        dcc.Checklist(
            id="view-options",
            options=[
                {"label":"Show forecast","value":"forecast"},
                {"label":"Show confidence band","value":"band"},
                {"label":"Show daily change","value":"daily"},
                {"label":"Show weekly change","value":"weekly"},
                {"label":"Show monthly change","value":"monthly"},
            ],
            value=["forecast","band","daily","weekly","monthly"],
            labelStyle={"display":"inline-block","marginRight":"10px"}
        )
    ], style={"marginBottom":"20px"}),

    dcc.Graph(id="price-chart"),
    dcc.Graph(id="change-chart"),
    html.Div(id="status-msg", style={"textAlign":"center","color":"#666"})
])

@app.callback(
    [Output("price-chart","figure"),
     Output("change-chart","figure"),
     Output("status-msg","children")],
    [Input("symbol-dropdown","value"),
     Input("date-range","start_date"),
     Input("date-range","end_date"),
     Input("view-options","value")]
)
def update_charts(symbol, start_date, end_date, options):
    if not symbol:
        return go.Figure(), go.Figure(), "⚠ No symbol selected"

    df_real = processed[processed["symbol"]==symbol].copy()
    df_fc = forecasted[forecasted["symbol"]==symbol].copy()

    if start_date and end_date:
        mask_real = (df_real["date"]>=pd.to_datetime(start_date)) & (df_real["date"]<=pd.to_datetime(end_date))
        mask_fc = (df_fc["date"]>=pd.to_datetime(start_date)) & (df_fc["date"]<=pd.to_datetime(end_date))
        df_real = df_real.loc[mask_real]
        df_fc = df_fc.loc[mask_fc]

    fig_price = go.Figure()
    if not df_real.empty:
        fig_price.add_trace(go.Scatter(x=df_real["date"], y=df_real["price"], mode="lines", name="Actual"))
    if "forecast" in options and not df_fc.empty:
        if "band" in options and {"ci_lower","ci_upper"}.issubset(df_fc.columns):
            fig_price.add_trace(go.Scatter(x=df_fc["date"], y=df_fc["ci_upper"], mode="lines",
                                           line=dict(color="rgba(255,127,14,0.2)"), showlegend=False))
            fig_price.add_trace(go.Scatter(x=df_fc["date"], y=df_fc["ci_lower"], mode="lines",
                                           line=dict(color="rgba(255,127,14,0.2)"), fill="tonexty",
                                           fillcolor="rgba(255,127,14,0.15)", name="Confidence band"))
        fig_price.add_trace(go.Scatter(x=df_fc["date"], y=df_fc["forecast"], mode="lines",
                                       name="Forecast", line=dict(dash="dash", color="#ff7f0e")))
    fig_price.update_layout(title=f"{symbol} Price & Forecast", xaxis_title="Date", yaxis_title="Price")

    fig_change = go.Figure()
    if "daily" in options and "Daily_Change_%" in df_real.columns:
        fig_change.add_trace(go.Bar(x=df_real["date"], y=df_real["Daily_Change_%"], name="Daily %"))
    if "weekly" in options and "Weekly_Change_7d_%" in df_real.columns:
        fig_change.add_trace(go.Bar(x=df_real["date"], y=df_real["Weekly_Change_7d_%"], name="Weekly %"))
    if "monthly" in options and "Monthly_Change_30d_%" in df_real.columns:
        fig_change.add_trace(go.Bar(x=df_real["date"], y=df_real["Monthly_Change_30d_%"], name="Monthly %"))
    fig_change.update_layout(title=f"{symbol} % Changes", xaxis_title="Date", yaxis_title="% Change")

    status = f"Symbol: {symbol} | Range: {start_date} → {end_date}"
    return fig_price, fig_change, status

if __name__ == "__main__":
    app.run(debug=True)