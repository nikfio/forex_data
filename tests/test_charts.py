# -*- coding: utf-8 -*-
"""
Tests for charting figure generation and volume hover display.
"""

from datetime import datetime
import pandas as pd
import polars as pl

from forex_data.data_management.charts.common import SupportedChartType
from forex_data.data_management.charts.figures import (
    _build_volume_hover_text,
    _format_volume_val,
    create_historical_figure,
)
from forex_data.data_management.common import COLUMN_NAME


def test_format_volume_val():
    """Verify clean comma formatting of volume numbers."""
    assert _format_volume_val(None) == "N/A"
    assert _format_volume_val(float("nan")) == "N/A"
    assert _format_volume_val(33750000.0) == "33,750,000"
    assert _format_volume_val(1000) == "1,000"
    assert _format_volume_val(1234.56) == "1,234.56"


def test_build_volume_hover_text_with_ask_bid_volume():
    """Verify hover text contains Vol, Ask Vol, and Bid Vol when columns exist."""
    df = pd.DataFrame({
        COLUMN_NAME.TIMESTAMP: [datetime(2024, 10, 9, 23, 55)],
        COLUMN_NAME.OPEN: [1.09414],
        COLUMN_NAME.HIGH: [1.09417],
        COLUMN_NAME.LOW: [1.09410],
        COLUMN_NAME.CLOSE: [1.09411],
        COLUMN_NAME.ASK_VOLUME: [33750000.0],
        COLUMN_NAME.BID_VOLUME: [33750000.0],
    })
    hover = _build_volume_hover_text(df)
    assert len(hover) == 1
    assert "Vol: 67,500,000" in hover[0]
    assert "Ask Vol: 33,750,000" in hover[0]
    assert "Bid Vol: 33,750,000" in hover[0]


def test_build_volume_hover_text_single_volume():
    """Verify hover text contains Volume when only volume column exists."""
    df = pd.DataFrame({
        COLUMN_NAME.TIMESTAMP: [datetime(2024, 10, 9, 23, 55)],
        COLUMN_NAME.OPEN: [1.09414],
        COLUMN_NAME.HIGH: [1.09417],
        COLUMN_NAME.LOW: [1.09410],
        COLUMN_NAME.CLOSE: [1.09411],
        COLUMN_NAME.VOLUME: [50000.0],
    })
    hover = _build_volume_hover_text(df)
    assert len(hover) == 1
    assert hover[0] == "Volume: 50,000"


def test_build_volume_hover_text_empty():
    """Verify hover text is empty when no volume columns exist."""
    df = pd.DataFrame({
        COLUMN_NAME.TIMESTAMP: [datetime(2024, 10, 9, 23, 55)],
        COLUMN_NAME.OPEN: [1.09414],
        COLUMN_NAME.HIGH: [1.09417],
        COLUMN_NAME.LOW: [1.09410],
        COLUMN_NAME.CLOSE: [1.09411],
    })
    hover = _build_volume_hover_text(df)
    assert hover == []


def test_create_historical_figure_ohlc_with_volumes():
    """Verify Candlestick trace includes volume hover text and hoverinfo."""
    df = pl.DataFrame({
        COLUMN_NAME.TIMESTAMP: [datetime(2024, 10, 9, 23, 55)],
        COLUMN_NAME.OPEN: [1.09414],
        COLUMN_NAME.HIGH: [1.09417],
        COLUMN_NAME.LOW: [1.09410],
        COLUMN_NAME.CLOSE: [1.09411],
        COLUMN_NAME.ASK_VOLUME: [33750000.0],
        COLUMN_NAME.BID_VOLUME: [33750000.0],
    })
    fig = create_historical_figure(df, "EURUSD", chart_type=SupportedChartType.OHLC)
    assert len(fig.data) == 1
    trace = fig.data[0]
    assert trace.type == "candlestick"
    assert trace.hoverinfo == "x+y+text"
    assert "Vol: 67,500,000" in trace.text[0]
    assert "Ask Vol: 33,750,000" in trace.text[0]
    assert "Bid Vol: 33,750,000" in trace.text[0]


def test_create_historical_figure_line_and_scatter():
    """Verify LINE and SCATTER traces with volume hover text."""
    df = pl.DataFrame({
        COLUMN_NAME.TIMESTAMP: [datetime(2024, 10, 9, 23, 55)],
        COLUMN_NAME.CLOSE: [1.09411],
        COLUMN_NAME.ASK: [1.09412],
        COLUMN_NAME.BID: [1.09410],
        COLUMN_NAME.ASK_VOLUME: [1000.0],
        COLUMN_NAME.BID_VOLUME: [2000.0],
    })
    fig_line = create_historical_figure(
        df, "EURUSD", chart_type=SupportedChartType.LINE
    )
    assert len(fig_line.data) == 1
    assert fig_line.data[0].hoverinfo == "x+y+text"
    assert "Ask Vol: 1,000" in fig_line.data[0].text[0]

    fig_scatter = create_historical_figure(
        df, "EURUSD", chart_type=SupportedChartType.SCATTER
    )
    assert len(fig_scatter.data) == 2
    assert fig_scatter.data[0].hoverinfo == "x+y+text"
    assert "Bid Vol: 2,000" in fig_scatter.data[0].text[0]
