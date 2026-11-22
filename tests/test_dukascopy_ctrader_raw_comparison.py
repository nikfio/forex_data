# -*- coding: utf-8 -*-
"""Raw Dukascopy vs cTrader tick comparison tests."""

from datetime import datetime
from pathlib import Path
import unittest

import polars as pl

from forex_data.data_management.common import (
    COLUMN_NAME,
    POLARS_DTYPE_DICT,
    reframe_data,
)
from forex_data.data_management.remoteconnector import CTraderConnector


class TestDukascopyCtraderRawComparison(unittest.TestCase):
    """Compare Dukascopy reference ticks with synthesized cTrader ticks."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.tick_path = Path(
            "~/.database/HistoricalData/LocalDB/forex/EURUSD/tick/"
            "forex_eurusd_tick_2021.parquet"
        ).expanduser()

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def test_dukascopy_and_ctrader_raw_week_comparison(self) -> None:
        """
        Compare Dukascopy raw ticks for 2021 Week 4
        (2021-01-25 00:00:00 to 2021-01-29 23:59:59 UTC)
        with synthesized raw ticks from cTrader download_range_raw.
        """
        if not self.tick_path.exists():
            self.skipTest(f"Reference raw tick file not found: {self.tick_path}")

        start_dt = datetime(2021, 1, 25, 0, 0, 0)
        end_dt = datetime(2021, 1, 29, 23, 59, 59)

        duka_ticks = pl.read_parquet(self.tick_path).filter(
            (pl.col(COLUMN_NAME.TIMESTAMP) >= start_dt)
            & (pl.col(COLUMN_NAME.TIMESTAMP) <= end_dt)
        )
        self.assertGreater(duka_ticks.height, 100_000)

        ctrader = CTraderConnector()
        try:
            ctrader.connect()
        except Exception as exc:
            self.skipTest(f"cTrader connection failed: {exc}")

        ctrader_ticks = ctrader.download_range_raw(
            ticker="EURUSD",
            start_date=start_dt,
            end_date=end_dt,
            engine="polars",
        )
        if hasattr(ctrader_ticks, "collect"):
            ctrader_ticks = ctrader_ticks.collect()

        self.assertFalse(ctrader_ticks.is_empty())

        for col_name, expected_dtype in POLARS_DTYPE_DICT.TIME_TICK_DTYPE.items():
            self.assertIn(col_name, ctrader_ticks.columns)
            self.assertEqual(ctrader_ticks.schema[col_name], expected_dtype)

        feature_cols = [
            c for c in ctrader_ticks.columns if c != COLUMN_NAME.TIMESTAMP
        ]
        bad_ticks = ctrader_ticks.filter(
            pl.any_horizontal([
                pl.col(c).is_null() | pl.col(c).is_nan() | (pl.col(c) <= 0.0)
                for c in feature_cols
            ])
        )
        self.assertEqual(bad_ticks.height, 0)

        duka_1m = reframe_data(duka_ticks, "1m")
        ctrader_1m = reframe_data(ctrader_ticks, "1m")

        self.assertGreater(duka_1m.height, 6_500)
        self.assertGreater(ctrader_1m.height, 6_500)

        valid_ohlc = ctrader_1m.filter(
            (pl.col(COLUMN_NAME.HIGH) >= pl.col(COLUMN_NAME.LOW))
            & (pl.col(COLUMN_NAME.HIGH) >= pl.col(COLUMN_NAME.OPEN))
            & (pl.col(COLUMN_NAME.HIGH) >= pl.col(COLUMN_NAME.CLOSE))
            & (pl.col(COLUMN_NAME.LOW) <= pl.col(COLUMN_NAME.OPEN))
            & (pl.col(COLUMN_NAME.LOW) <= pl.col(COLUMN_NAME.CLOSE))
        )
        self.assertEqual(valid_ohlc.height, ctrader_1m.height)

        joined = duka_1m.join(
            ctrader_1m,
            on=COLUMN_NAME.TIMESTAMP,
            suffix="_ctrader",
        )
        self.assertGreater(joined.height, 6_800)

        stats = joined.select(
            pl.corr(
                COLUMN_NAME.CLOSE,
                f"{COLUMN_NAME.CLOSE}_ctrader",
            ).alias("close_corr"),
            (
                pl.col(COLUMN_NAME.CLOSE)
                - pl.col(f"{COLUMN_NAME.CLOSE}_ctrader")
            ).abs().mean().alias("mae_close"),
        ).to_dicts()[0]

        self.assertGreater(stats["close_corr"], 0.999)
        self.assertLess(stats["mae_close"], 0.0005)
