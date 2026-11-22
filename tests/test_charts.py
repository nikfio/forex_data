# -*- coding: utf-8 -*-
"""
Tests for charting figure generation and volume hover display.
"""

from datetime import datetime
import os
import unittest
from unittest.mock import patch
import pandas as pd
import polars as pl

from forex_data.data_management.charts.common import SupportedChartType
from forex_data.data_management.charts.datacharts import DataCharts
from forex_data.data_management.charts.figures import (
    _build_volume_hover_text,
    _format_volume_val,
    create_historical_figure,
    detect_system_theme,
    resolve_chart_theme_palette,
)
from forex_data.data_management.common import COLUMN_NAME


class TestCharts(unittest.TestCase):
    """Tests for charting figure generation and volume hover display."""

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_format_volume_val(self):
        """Verify clean comma formatting of volume numbers."""
        self.assertEqual(_format_volume_val(None), "N/A")
        self.assertEqual(_format_volume_val(float("nan")), "N/A")
        self.assertEqual(_format_volume_val(33750000.0), "33,750,000")
        self.assertEqual(_format_volume_val(1000), "1,000")
        self.assertEqual(_format_volume_val(1234.56), "1,234.56")

    def test_build_volume_hover_text_with_ask_bid_volume(self):
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
        self.assertEqual(len(hover), 1)
        self.assertIn("Vol: 67,500,000", hover[0])
        self.assertIn("Ask Vol: 33,750,000", hover[0])
        self.assertIn("Bid Vol: 33,750,000", hover[0])

    def test_build_volume_hover_text_single_volume(self):
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
        self.assertEqual(len(hover), 1)
        self.assertEqual(hover[0], "Volume: 50,000")

    def test_build_volume_hover_text_empty(self):
        """Verify hover text is empty when no volume columns exist."""
        df = pd.DataFrame({
            COLUMN_NAME.TIMESTAMP: [datetime(2024, 10, 9, 23, 55)],
            COLUMN_NAME.OPEN: [1.09414],
            COLUMN_NAME.HIGH: [1.09417],
            COLUMN_NAME.LOW: [1.09410],
            COLUMN_NAME.CLOSE: [1.09411],
        })
        hover = _build_volume_hover_text(df)
        self.assertEqual(hover, [])

    def test_create_historical_figure_ohlc_with_volumes(self):
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
        fig = create_historical_figure(
            df, "EURUSD", chart_type=SupportedChartType.OHLC
        )
        self.assertEqual(len(fig.data), 1)
        trace = fig.data[0]
        self.assertEqual(trace.type, "candlestick")
        self.assertEqual(trace.hoverinfo, "x+y+text")
        self.assertIn("Vol: 67,500,000", trace.text[0])
        self.assertIn("Ask Vol: 33,750,000", trace.text[0])
        self.assertIn("Bid Vol: 33,750,000", trace.text[0])

    def test_create_historical_figure_line_and_scatter(self):
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
        self.assertEqual(len(fig_line.data), 1)
        self.assertEqual(fig_line.data[0].hoverinfo, "x+y+text")
        self.assertIn("Ask Vol: 1,000", fig_line.data[0].text[0])

        fig_scatter = create_historical_figure(
            df, "EURUSD", chart_type=SupportedChartType.SCATTER
        )
        self.assertEqual(len(fig_scatter.data), 2)
        self.assertEqual(fig_scatter.data[0].hoverinfo, "x+y+text")
        self.assertIn("Bid Vol: 2,000", fig_scatter.data[0].text[0])


class TestChartsExtended(unittest.TestCase):
    """Tests for OHLC+Volume subplots, theme palettes, and DataCharts API."""

    @classmethod
    def setUpClass(cls):
        cls.sample_df = pl.DataFrame({
            COLUMN_NAME.TIMESTAMP: [
                datetime(2024, 10, 9, 23, 50),
                datetime(2024, 10, 9, 23, 55),
            ],
            COLUMN_NAME.OPEN: [1.09400, 1.09414],
            COLUMN_NAME.HIGH: [1.09420, 1.09430],
            COLUMN_NAME.LOW: [1.09390, 1.09405],
            COLUMN_NAME.CLOSE: [1.09414, 1.09408],
            COLUMN_NAME.ASK_VOLUME: [1000000.0, 1500000.0],
            COLUMN_NAME.BID_VOLUME: [900000.0, 1800000.0],
            COLUMN_NAME.VWMP: [1.09407, 1.09416],
        })

    @classmethod
    def tearDownClass(cls):
        pass

    def test_theme_detection_and_palettes(self):
        with patch.dict(os.environ, {"SYSTEM_THEME": "light"}):
            self.assertEqual(detect_system_theme(), "light")
        with patch.dict(os.environ, {"SYSTEM_THEME": "dark"}):
            self.assertEqual(detect_system_theme(), "dark")

        dark_pal = resolve_chart_theme_palette("dark")
        light_pal = resolve_chart_theme_palette("light")
        self.assertEqual(dark_pal["template"], "plotly_dark")
        self.assertEqual(light_pal["template"], "plotly_white")

    def test_create_ohlc_with_volume_and_vwmp_subplot(self):
        fig = create_historical_figure(
            self.sample_df,
            "EURUSD",
            chart_type=SupportedChartType.OHLC,
            show_volume=True,
            theme="light",
            show_vwmp=True,
            title="Custom Title",
        )
        trace_types = [t.type for t in fig.data]
        trace_names = [t.name for t in fig.data]
        self.assertIn("candlestick", trace_types)
        self.assertEqual(trace_types.count("bar"), 2)
        self.assertIn("Ask Volume", trace_names)
        self.assertIn("Bid Volume", trace_names)
        self.assertIn("VWMP", trace_names)
        self.assertEqual(fig.layout.plot_bgcolor, "#FFFFFF")
        self.assertEqual(fig.layout.title.text, "Custom Title")

    def test_create_ohlc_with_single_volume_column(self):
        df_vol = pl.DataFrame({
            COLUMN_NAME.TIMESTAMP: [
                datetime(2024, 10, 9, 23, 50),
                datetime(2024, 10, 9, 23, 55),
            ],
            COLUMN_NAME.OPEN: [1.1000, 1.1010],
            COLUMN_NAME.HIGH: [1.1020, 1.1020],
            COLUMN_NAME.LOW: [1.0990, 1.0995],
            COLUMN_NAME.CLOSE: [1.1010, 1.1000],
            COLUMN_NAME.VOLUME: [2500.0, 3100.0],
        })
        fig = create_historical_figure(
            df_vol,
            "EURUSD",
            chart_type=SupportedChartType.OHLC,
            show_volume=True,
            theme="dark",
        )
        bar_traces = [t for t in fig.data if t.type == "bar"]
        self.assertEqual(len(bar_traces), 1)
        self.assertEqual(bar_traces[0].name, "Volume")
        self.assertEqual(fig.layout.plot_bgcolor, "#0E1117")

    def test_datacharts_plot_historical_returns_figure(self):
        charts = DataCharts.__new__(DataCharts)
        charts.realtime_connector = None
        charts._realtime_apps = {}
        with patch.object(charts, "get_data", return_value=self.sample_df):
            fig = charts.plot_historical(
                ticker="EURUSD",
                timeframe="5m",
                start_date="2024-10-09 23:00:00",
                end_date="2024-10-10 00:00:00",
                show_volume=True,
                theme="light",
                show=False,
            )
        self.assertIsNotNone(fig)
        self.assertIn("candlestick", [t.type for t in fig.data])
        self.assertIn("bar", [t.type for t in fig.data])
