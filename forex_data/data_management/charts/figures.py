import plotly.graph_objects as go
from forex_data.data_management.common import COLUMN_NAME
from forex_data.data_management.charts.common import SupportedChartType
import polars as pl
import pandas as pd


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
            hover_text = []
            if COLUMN_NAME.VOLUME in df.columns:
                hover_text = [f"Volume: {v}" for v in df[COLUMN_NAME.VOLUME]]

            fig.add_trace(go.Candlestick(
                x=x_data,
                open=df[COLUMN_NAME.OPEN],
                high=df[COLUMN_NAME.HIGH],
                low=df[COLUMN_NAME.LOW],
                close=df[COLUMN_NAME.CLOSE],
                name=ticker,
                text=hover_text,
                hoverinfo="x+y+text"
            ))
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

        fig.add_trace(go.Scatter(
            x=x_data,
            y=df[y_col],
            mode='lines',
            name=f"{ticker} {y_col}"
        ))

    elif chart_type == SupportedChartType.SCATTER:
        if COLUMN_NAME.ASK in df.columns and COLUMN_NAME.BID in df.columns:
            fig.add_trace(go.Scatter(
                x=x_data, y=df[COLUMN_NAME.ASK], mode='markers', name='Ask'))
            fig.add_trace(go.Scatter(
                x=x_data, y=df[COLUMN_NAME.BID], mode='markers', name='Bid'))
        elif COLUMN_NAME.CLOSE in df.columns:
            fig.add_trace(go.Scatter(
                x=x_data, y=df[COLUMN_NAME.CLOSE], mode='markers', name='Close'))

    fig.update_layout(
        title=f"{ticker} Historical Data",
        yaxis_title="Price",
        xaxis_title="Time",
        xaxis_rangeslider_visible=False,
        template="plotly_dark"
    )

    return fig
