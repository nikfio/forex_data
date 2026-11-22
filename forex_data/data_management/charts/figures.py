import os
import subprocess
import sys
from typing import Any, List
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import polars as pl
from forex_data.data_management.charts.common import SupportedChartType
from forex_data.data_management.common import COLUMN_NAME


def detect_system_theme() -> str:
    """
    Detect system UI appearance ('dark' or 'light').
    Checks SYSTEM_THEME env var, macOS AppleInterfaceStyle, and COLORFGBG.
    """
    env_theme = os.environ.get("SYSTEM_THEME", "").strip().lower()
    if env_theme in ("dark", "light"):
        return env_theme

    if sys.platform == "darwin":
        try:
            res = subprocess.run(
                ["defaults", "read", "-g", "AppleInterfaceStyle"],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
            if res.returncode == 0 and "dark" in res.stdout.lower():
                return "dark"
            return "light"
        except Exception:
            pass

    colorfgbg = os.environ.get("COLORFGBG", "")
    if ";" in colorfgbg:
        bg_part = colorfgbg.split(";")[-1].strip()
        try:
            bg_idx = int(bg_part)
            return "light" if bg_idx >= 7 else "dark"
        except ValueError:
            pass

    return "dark"


def resolve_chart_theme_palette(theme: str = "auto") -> dict[str, Any]:
    """Resolve Plotly template and OHLC/volume colors for dark or light themes."""
    norm = str(theme or "auto").strip().lower()
    if norm == "auto":
        norm = detect_system_theme()
    if norm == "light":
        return {
            "theme": "light",
            "template": "plotly_white",
            "bg_color": "#FFFFFF",
            "paper_color": "#FAFAFA",
            "grid_color": "rgba(15, 23, 42, 0.08)",
            "text_color": "#0F172A",
            "bull_color": "#00897B",
            "bear_color": "#D32F2F",
            "line_color": "#0288D1",
            "vol_ask_color": "rgba(0, 137, 123, 0.55)",
            "vol_bid_color": "rgba(211, 47, 47, 0.55)",
            "vol_bull_color": "rgba(0, 137, 123, 0.55)",
            "vol_bear_color": "rgba(211, 47, 47, 0.55)",
            "vwmp_color": "rgba(106, 27, 154, 0.72)",
        }
    return {
        "theme": "dark",
        "template": "plotly_dark",
        "bg_color": "#0E1117",
        "paper_color": "#0E1117",
        "grid_color": "rgba(255, 255, 255, 0.08)",
        "text_color": "#E6EDF3",
        "bull_color": "#00E676",
        "bear_color": "#FF5252",
        "line_color": "#40C4FF",
        "vol_ask_color": "rgba(0, 230, 118, 0.50)",
        "vol_bid_color": "rgba(255, 82, 82, 0.50)",
        "vol_bull_color": "rgba(0, 230, 118, 0.55)",
        "vol_bear_color": "rgba(255, 82, 82, 0.55)",
        "vwmp_color": "rgba(179, 136, 255, 0.75)",
    }


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


def _resolve_volume_columns(
    df: pd.DataFrame,
) -> tuple[str | None, str | None, str | None]:
    """Identify ask, bid, and total volume columns present in dataframe."""
    ask_col = (
        COLUMN_NAME.ASK_VOLUME
        if COLUMN_NAME.ASK_VOLUME in df.columns
        else ("ask_vol" if "ask_vol" in df.columns else None)
    )
    bid_col = (
        COLUMN_NAME.BID_VOLUME
        if COLUMN_NAME.BID_VOLUME in df.columns
        else ("bid_vol" if "bid_vol" in df.columns else None)
    )
    vol_col = (
        COLUMN_NAME.VOLUME
        if COLUMN_NAME.VOLUME in df.columns
        else ("vol" if "vol" in df.columns else None)
    )
    return ask_col, bid_col, vol_col


def _build_volume_hover_text(df: pd.DataFrame) -> List[str]:
    """
    Construct hover text strings with volume, ask volume, and bid volume
    depending on available columns in the dataframe.
    """
    ask_vol_col, bid_vol_col, vol_col = _resolve_volume_columns(df)

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


def _add_volume_subplot_traces(
    fig: go.Figure,
    df: pd.DataFrame,
    x_data: Any,
    palette: dict[str, Any],
    row: int = 2,
    col: int = 1,
) -> bool:
    """Add volume bar trace(s) to subplot row and return True if added."""
    ask_col, bid_col, vol_col = _resolve_volume_columns(df)
    if ask_col and bid_col:
        tot_vol = df[vol_col] if vol_col else (df[ask_col] + df[bid_col])
        ask_hover = [
            f"Ask Vol: {_format_volume_val(a)}<br>Total Vol: {_format_volume_val(t)}"
            for a, t in zip(df[ask_col], tot_vol)
        ]
        bid_hover = [
            f"Bid Vol: {_format_volume_val(b)}<br>Total Vol: {_format_volume_val(t)}"
            for b, t in zip(df[bid_col], tot_vol)
        ]
        fig.add_trace(
            go.Bar(
                x=x_data,
                y=df[ask_col],
                name="Ask Volume",
                marker_color=palette["vol_ask_color"],
                text=ask_hover,
                textposition="none",
                hoverinfo="x+text",
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Bar(
                x=x_data,
                y=df[bid_col],
                name="Bid Volume",
                marker_color=palette["vol_bid_color"],
                text=bid_hover,
                textposition="none",
                hoverinfo="x+text",
            ),
            row=row,
            col=col,
        )
        return True

    single_col = vol_col or ask_col or bid_col
    if single_col:
        if COLUMN_NAME.OPEN in df.columns and COLUMN_NAME.CLOSE in df.columns:
            colors = [
                palette["vol_bull_color"] if c >= o else palette["vol_bear_color"]
                for o, c in zip(df[COLUMN_NAME.OPEN], df[COLUMN_NAME.CLOSE])
            ]
        else:
            colors = palette["vol_bull_color"]
        fig.add_trace(
            go.Bar(
                x=x_data,
                y=df[single_col],
                name="Volume",
                marker_color=colors,
            ),
            row=row,
            col=col,
        )
        return True

    return False


def create_historical_figure(
    df: pl.DataFrame | pl.LazyFrame | pd.DataFrame,
    ticker: str,
    chart_type: str = SupportedChartType.OHLC,
    show_volume: bool = False,
    theme: str = "dark",
    title: str | None = None,
    show_vwmp: bool = False,
) -> go.Figure:
    """
    Creates a plotly figure based on the provided dataframe.

    Args:
        df: DataFrame containing the market data.
        ticker: Symbol name for the title.
        chart_type: The type of chart to plot.
        show_volume: Whether to render a synced volume bar subplot below price.
        theme: Color theme ('auto', 'dark', or 'light').
        title: Optional custom chart title.
        show_vwmp: Overlay VWMP line when present in dataframe.

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

    palette = resolve_chart_theme_palette(theme)
    ask_col, bid_col, vol_col = _resolve_volume_columns(df)
    has_volume = bool(show_volume and (ask_col or bid_col or vol_col))

    if has_volume:
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            row_heights=[0.74, 0.26],
        )
        trace_pos: dict[str, int] = {"row": 1, "col": 1}
    else:
        fig = go.Figure()
        trace_pos = {}

    if chart_type == SupportedChartType.OHLC:
        req_cols = [
            COLUMN_NAME.OPEN, COLUMN_NAME.HIGH,
            COLUMN_NAME.LOW, COLUMN_NAME.CLOSE
        ]
        if all(c in df.columns for c in req_cols):
            hover_text = _build_volume_hover_text(df)

            candlestick_kwargs: dict[str, Any] = {
                "x": x_data,
                "open": df[COLUMN_NAME.OPEN],
                "high": df[COLUMN_NAME.HIGH],
                "low": df[COLUMN_NAME.LOW],
                "close": df[COLUMN_NAME.CLOSE],
                "name": ticker,
                "increasing_line_color": palette["bull_color"],
                "increasing_fillcolor": palette["bull_color"],
                "decreasing_line_color": palette["bear_color"],
                "decreasing_fillcolor": palette["bear_color"],
            }
            if hover_text:
                candlestick_kwargs["text"] = hover_text
                candlestick_kwargs["hoverinfo"] = "x+y+text"
            else:
                candlestick_kwargs["hoverinfo"] = "x+y"

            fig.add_trace(go.Candlestick(**candlestick_kwargs), **trace_pos)

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
            "line": {"color": palette["line_color"], "width": 1.8},
            "name": f"{ticker} {y_col}",
        }
        if hover_text:
            scatter_kwargs["text"] = hover_text
            scatter_kwargs["hoverinfo"] = "x+y+text"

        fig.add_trace(go.Scatter(**scatter_kwargs), **trace_pos)

    elif chart_type == SupportedChartType.SCATTER:
        hover_text = _build_volume_hover_text(df)
        common_kwargs: dict[str, Any] = {"x": x_data, "mode": "markers"}
        if hover_text:
            common_kwargs["text"] = hover_text
            common_kwargs["hoverinfo"] = "x+y+text"

        if COLUMN_NAME.ASK in df.columns and COLUMN_NAME.BID in df.columns:
            fig.add_trace(
                go.Scatter(y=df[COLUMN_NAME.ASK], name="Ask", **common_kwargs),
                **trace_pos,
            )
            fig.add_trace(
                go.Scatter(y=df[COLUMN_NAME.BID], name="Bid", **common_kwargs),
                **trace_pos,
            )
        elif COLUMN_NAME.CLOSE in df.columns:
            fig.add_trace(
                go.Scatter(y=df[COLUMN_NAME.CLOSE], name="Close", **common_kwargs),
                **trace_pos,
            )

    if show_vwmp and COLUMN_NAME.VWMP in df.columns:
        fig.add_trace(
            go.Scatter(
                x=x_data,
                y=df[COLUMN_NAME.VWMP],
                mode="lines",
                name="VWMP",
                line={"color": palette["vwmp_color"], "width": 1.3, "dash": "dot"},
            ),
            **trace_pos,
        )

    if has_volume:
        _add_volume_subplot_traces(fig, df, x_data, palette, row=2, col=1)
        fig.update_layout(barmode="stack")
        fig.update_yaxes(
            title_text="Price",
            gridcolor=palette["grid_color"],
            row=1,
            col=1,
        )
        fig.update_yaxes(
            title_text="Volume",
            gridcolor=palette["grid_color"],
            row=2,
            col=1,
        )
        fig.update_xaxes(
            gridcolor=palette["grid_color"],
            rangeslider_visible=False,
            row=1,
            col=1,
        )
        fig.update_xaxes(
            title_text="Time (UTC)",
            gridcolor=palette["grid_color"],
            rangeslider_visible=False,
            row=2,
            col=1,
        )
    else:
        fig.update_layout(
            yaxis_title="Price",
            xaxis_title="Time (UTC)",
        )
        fig.update_xaxes(gridcolor=palette["grid_color"])
        fig.update_yaxes(gridcolor=palette["grid_color"])

    fig.update_layout(
        title=title or f"{ticker} Historical Data",
        xaxis_rangeslider_visible=False,
        template=palette["template"],
        plot_bgcolor=palette["bg_color"],
        paper_bgcolor=palette["paper_color"],
        font={"color": palette["text_color"]},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.01,
            "xanchor": "right",
            "x": 1.0,
        },
        hovermode="x unified",
    )

    return fig
