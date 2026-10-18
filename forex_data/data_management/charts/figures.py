from typing import Any, List
import pandas as pd
import plotly.graph_objects as go
import polars as pl
from forex_data.data_management.charts.common import SupportedChartType
from forex_data.data_management.common import COLUMN_NAME


def _format_volume_val(val: Any) -> str:
    """Format volume value cleanly with comma separators."""
    if val is None or pd.isna(val):
        return "N/A"
    try:
        fval = float(val)
        if fval.is_integer():
            return f"{int(fval):,}"
        return f"{fval:,.2f}"
    except (ValueError, TypeError):
        return str(val)


def _build_volume_hover_text(df: pd.DataFrame) -> List[str]:
    """
    Construct hover text strings with volume, ask volume, and bid volume
    depending on available columns in the dataframe.
    """
    ask_vol_col = (
        COLUMN_NAME.ASK_VOLUME
        if COLUMN_NAME.ASK_VOLUME in df.columns
        else ("ask_vol" if "ask_vol" in df.columns else None)
    )
    bid_vol_col = (
        COLUMN_NAME.BID_VOLUME
        if COLUMN_NAME.BID_VOLUME in df.columns
        else ("bid_vol" if "bid_vol" in df.columns else None)
    )
    vol_col = (
        COLUMN_NAME.VOLUME
        if COLUMN_NAME.VOLUME in df.columns
        else ("vol" if "vol" in df.columns else None)
    )

    if ask_vol_col and bid_vol_col:
        ask_series = df[ask_vol_col]
        bid_series = df[bid_vol_col]
        tot_series = df[vol_col] if vol_col else (ask_series + bid_series)

        return [
            f"Vol: {_format_volume_val(tot)}<br>"
            f"Ask Vol: {_format_volume_val(ask)}<br>"
            f"Bid Vol: {_format_volume_val(bid)}"
            for tot, ask, bid in zip(tot_series, ask_series, bid_series)
        ]
    elif ask_vol_col:
        return [
            f"Ask Vol: {_format_volume_val(ask)}"
            for ask in df[ask_vol_col]
        ]
    elif bid_vol_col:
        return [
            f"Bid Vol: {_format_volume_val(bid)}"
            for bid in df[bid_vol_col]
        ]
    elif vol_col:
        return [
            f"Volume: {_format_volume_val(v)}"
            for v in df[vol_col]
        ]
    return []


def create_historical_figure(
    df: pl.DataFrame | pl.LazyFrame | pd.DataFrame,
    ticker: str,
    chart_type: str = SupportedChartType.OHLC
) -> go.Figure:
    """
    Creates a plotly figure based on the provided dataframe.

    Args:
        df: DataFrame containing the market data.
        ticker: Symbol name for the title.
        chart_type: The type of chart to plot.

    Returns:
        go.Figure: The constructed plotly figure.
    """
    if hasattr(df, "collect"):
        df = df.collect()

    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()

    if COLUMN_NAME.TIMESTAMP in df.columns:
        x_data = df[COLUMN_NAME.TIMESTAMP]
    elif df.index.name == COLUMN_NAME.TIMESTAMP:
        x_data = df.index
    else:
        # Fallback
        x_data = df.index

    fig = go.Figure()

    if chart_type == SupportedChartType.OHLC:
        # Determine if we have bid/ask data or just standard OHLC
        req_cols = [
            COLUMN_NAME.OPEN, COLUMN_NAME.HIGH,
            COLUMN_NAME.LOW, COLUMN_NAME.CLOSE
        ]
        if all(col in df.columns for col in req_cols):
            hover_text = _build_volume_hover_text(df)

            candlestick_kwargs: dict[str, Any] = {
                "x": x_data,
                "open": df[COLUMN_NAME.OPEN],
                "high": df[COLUMN_NAME.HIGH],
                "low": df[COLUMN_NAME.LOW],
                "close": df[COLUMN_NAME.CLOSE],
                "name": ticker,
            }
            if hover_text:
                candlestick_kwargs["text"] = hover_text
                candlestick_kwargs["hoverinfo"] = "x+y+text"
            else:
                candlestick_kwargs["hoverinfo"] = "x+y"

            fig.add_trace(go.Candlestick(**candlestick_kwargs))
        else:
            # Maybe ask/bid specific?
            # Not standard in this context, just plot whatever is available
            pass

    elif chart_type == SupportedChartType.LINE:
        if COLUMN_NAME.CLOSE in df.columns:
            y_col = COLUMN_NAME.CLOSE
        elif COLUMN_NAME.ASK in df.columns:
            y_col = COLUMN_NAME.ASK
        else:
            y_col = df.columns[0]

        hover_text = _build_volume_hover_text(df)
        scatter_kwargs: dict[str, Any] = {
            "x": x_data,
            "y": df[y_col],
            "mode": "lines",
            "name": f"{ticker} {y_col}",
        }
        if hover_text:
            scatter_kwargs["text"] = hover_text
            scatter_kwargs["hoverinfo"] = "x+y+text"

        fig.add_trace(go.Scatter(**scatter_kwargs))

    elif chart_type == SupportedChartType.SCATTER:
        hover_text = _build_volume_hover_text(df)
        common_kwargs: dict[str, Any] = {"x": x_data, "mode": "markers"}
        if hover_text:
            common_kwargs["text"] = hover_text
            common_kwargs["hoverinfo"] = "x+y+text"

        if COLUMN_NAME.ASK in df.columns and COLUMN_NAME.BID in df.columns:
            fig.add_trace(go.Scatter(
                y=df[COLUMN_NAME.ASK], name='Ask', **common_kwargs))
            fig.add_trace(go.Scatter(
                y=df[COLUMN_NAME.BID], name='Bid', **common_kwargs))
        elif COLUMN_NAME.CLOSE in df.columns:
            fig.add_trace(go.Scatter(
                y=df[COLUMN_NAME.CLOSE], name='Close', **common_kwargs))

    fig.update_layout(
        title=f"{ticker} Historical Data",
        yaxis_title="Price",
        xaxis_title="Time",
        xaxis_rangeslider_visible=False,
        template="plotly_dark"
    )

    return fig
