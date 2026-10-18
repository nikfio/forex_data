from datetime import datetime
from pathlib import Path
import polars as pl
import pytest
from forex_data.data_management.common import (
    COLUMN_NAME,
    POLARS_DTYPE_DICT,
    reframe_data,
)
from forex_data.data_management.remoteconnector import CTraderConnector


def test_dukascopy_and_ctrader_raw_week_comparison():
    """
    Compares Dukascopy valid raw tick data for 2021 Week 4
    (2021-01-25 00:00:00 to 2021-01-29 23:59:59 UTC)
    with synthesized raw tick data from cTrader download_range_raw.
    Verifies schema compliance, zero nulls/nans/non-positive values,
    and high correlation / low MAE at 1m timeframe.
    """
    tick_path = Path(
        '~/.test_vol_database/HistoricalData/LocalDB/forex/EURUSD/tick/'
        'forex_eurusd_tick_2021.parquet'
    ).expanduser()

    if not tick_path.exists():
        pytest.skip(f"Reference raw tick file not found: {tick_path}")

    # 1. Load Dukascopy raw tick data for 2021 week 4
    start_dt = datetime(2021, 1, 25, 0, 0, 0)
    end_dt = datetime(2021, 1, 29, 23, 59, 59)

    duka_ticks = pl.read_parquet(tick_path).filter(
        (pl.col(COLUMN_NAME.TIMESTAMP) >= start_dt)
        & (pl.col(COLUMN_NAME.TIMESTAMP) <= end_dt)
    )
    assert duka_ticks.height > 100_000, "Dukascopy should have > 100k ticks"

    # 2. Fetch cTrader synthesized raw tick data via download_range_raw
    ctrader = CTraderConnector()
    try:
        ctrader.connect()
    except Exception as exc:
        pytest.skip(f"cTrader connection failed: {exc}")

    ctrader_ticks = ctrader.download_range_raw(
        ticker='EURUSD',
        start_date=start_dt,
        end_date=end_dt,
        engine='polars'
    )
    if hasattr(ctrader_ticks, 'collect'):
        ctrader_ticks = ctrader_ticks.collect()

    assert not ctrader_ticks.is_empty(), "cTrader ticks should not be empty"

    # 3. Verify cTrader tick schema matches TIME_TICK_DTYPE
    for col_name, expected_dtype in POLARS_DTYPE_DICT.TIME_TICK_DTYPE.items():
        assert col_name in ctrader_ticks.columns, f"Missing {col_name}"
        assert ctrader_ticks.schema[col_name] == expected_dtype

    # 4. Universal Non-Zero Rule check on cTrader raw ticks
    feature_cols = [c for c in ctrader_ticks.columns if c != COLUMN_NAME.TIMESTAMP]
    bad_ticks = ctrader_ticks.filter(
        pl.any_horizontal([
            pl.col(c).is_null() | pl.col(c).is_nan() | (pl.col(c) <= 0.0)
            for c in feature_cols
        ])
    )
    assert bad_ticks.height == 0, f"Found {bad_ticks.height} non-positive ticks"

    # 5. Reframe both to 1m
    duka_1m = reframe_data(duka_ticks, '1m')
    ctrader_1m = reframe_data(ctrader_ticks, '1m')

    assert duka_1m.height > 6_500, "Dukascopy 1m should span the full week"
    assert ctrader_1m.height > 6_500, "cTrader 1m should span the full week"

    # 6. Verify 4-Point OHLC reconstruction integrity
    valid_ohlc = ctrader_1m.filter(
        (pl.col(COLUMN_NAME.HIGH) >= pl.col(COLUMN_NAME.LOW))
        & (pl.col(COLUMN_NAME.HIGH) >= pl.col(COLUMN_NAME.OPEN))
        & (pl.col(COLUMN_NAME.HIGH) >= pl.col(COLUMN_NAME.CLOSE))
        & (pl.col(COLUMN_NAME.LOW) <= pl.col(COLUMN_NAME.OPEN))
        & (pl.col(COLUMN_NAME.LOW) <= pl.col(COLUMN_NAME.CLOSE))
    )
    assert valid_ohlc.height == ctrader_1m.height, "Invalid OHLC ordering in 1m"

    # 7. Compare price correlation and MAE over overlapping bars
    joined = duka_1m.join(
        ctrader_1m,
        on=COLUMN_NAME.TIMESTAMP,
        suffix='_ctrader'
    )
    assert joined.height > 6_800, "Expected > 6,800 overlapping 1m bars"

    stats = joined.select(
        pl.corr(
            COLUMN_NAME.CLOSE,
            f"{COLUMN_NAME.CLOSE}_ctrader"
        ).alias("close_corr"),
        (
            pl.col(COLUMN_NAME.CLOSE)
            - pl.col(f"{COLUMN_NAME.CLOSE}_ctrader")
        ).abs().mean().alias("mae_close")
    ).to_dicts()[0]

    corr = stats["close_corr"]
    mae = stats["mae_close"]

    assert corr > 0.999, f"Correlation {corr} is below 0.999 threshold"
    assert mae < 0.0005, f"MAE {mae} exceeded 5 pips threshold (0.0005)"
