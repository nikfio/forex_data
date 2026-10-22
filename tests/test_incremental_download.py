# -*- coding: utf-8 -*-
"""
Tests for incremental database recognition and overlap margin.
"""

from datetime import datetime, timedelta
from unittest.mock import patch
import polars as pl
import pytest

from forex_data import (
    COLUMN_NAME,
    HistoricalManagerDB,
    POLARS_DTYPE_DICT,
)
from forex_data.data_management import (
    DukascopyConnector,
    LocalDBYearConnector,
)


@pytest.fixture
def sample_tick_df():
    """Create a sample tick DataFrame."""
    timestamps = [
        datetime(2026, 10, 19, 20, 0, 0),
        datetime(2026, 10, 19, 21, 0, 0),
        datetime(2026, 10, 19, 21, 59, 35),
    ]
    return pl.DataFrame({
        COLUMN_NAME.TIMESTAMP: timestamps,
        COLUMN_NAME.ASK: [1.0850, 1.0852, 1.0855],
        COLUMN_NAME.BID: [1.0848, 1.0850, 1.0853],
        COLUMN_NAME.ASK_VOLUME: [100.0, 120.0, 150.0],
        COLUMN_NAME.BID_VOLUME: [100.0, 120.0, 150.0],
        COLUMN_NAME.VWMP: [1.0849, 1.0851, 1.0854],
    }).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)


@pytest.fixture
def overlapping_tick_df():
    """Create an overlapping tick DataFrame with duplicate and new ticks."""
    timestamps = [
        datetime(2026, 10, 19, 21, 0, 0),  # Duplicate
        datetime(2026, 10, 19, 21, 59, 35),  # Duplicate
        datetime(2026, 10, 20, 10, 0, 0),  # New
        datetime(2026, 10, 22, 21, 0, 0),  # New
    ]
    return pl.DataFrame({
        COLUMN_NAME.TIMESTAMP: timestamps,
        COLUMN_NAME.ASK: [1.0852, 1.0855, 1.0860, 1.0870],
        COLUMN_NAME.BID: [1.0850, 1.0853, 1.0858, 1.0868],
        COLUMN_NAME.ASK_VOLUME: [120.0, 150.0, 200.0, 250.0],
        COLUMN_NAME.BID_VOLUME: [120.0, 150.0, 200.0, 250.0],
        COLUMN_NAME.VWMP: [1.0851, 1.0854, 1.0859, 1.0869],
    }).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)


def test_read_last_timestamp_case_insensitive_and_year(tmp_path, sample_tick_df):
    """Test read_last_timestamp with year parameter and case-insensitivity."""
    connector = LocalDBYearConnector(data_path=str(tmp_path), data_type='parquet')

    # Initial state: empty database
    assert connector.read_last_timestamp('forex', 'EURUSD', 'tick') is None
    assert connector.read_last_timestamp('forex', 'eurusd', 'tick', year=2026) is None

    # Write initial data for year 2026
    key = connector._db_key('forex', 'eurusd', 'tick', 2026)
    connector.write_data(key, sample_tick_df)

    # 1. Lowercase ticker lookup
    ts_lower = connector.read_last_timestamp('forex', 'eurusd', 'tick')
    assert ts_lower == datetime(2026, 10, 19, 21, 59, 35)

    # 2. Uppercase ticker lookup
    ts_upper = connector.read_last_timestamp('forex', 'EURUSD', 'tick')
    assert ts_upper == datetime(2026, 10, 19, 21, 59, 35)

    # 3. Explicit matching year
    ts_year = connector.read_last_timestamp('forex', 'EURUSD', 'tick', year=2026)
    assert ts_year == datetime(2026, 10, 19, 21, 59, 35)

    # 4. Non-matching year returns None
    ts_diff_year = connector.read_last_timestamp('forex', 'EURUSD', 'tick', year=2025)
    assert ts_diff_year is None


def test_incremental_write_deduplication(tmp_path, sample_tick_df, overlapping_tick_df):
    """Test that writing overlapping data cleanly merges and deduplicates timestamps."""
    connector = LocalDBYearConnector(data_path=str(tmp_path), data_type='parquet')
    key = connector._db_key('forex', 'eurusd', 'tick', 2026)

    # Write first chunk
    connector.write_data(key, sample_tick_df)
    first_read = connector.read_data('forex', 'eurusd', 'tick',
                                     datetime(2026, 10, 1),
                                     datetime(2026, 10, 31)).collect()
    assert len(first_read) == 3

    # Write overlapping chunk
    connector.write_data(key, overlapping_tick_df)
    second_read = connector.read_data('forex', 'eurusd', 'tick',
                                      datetime(2026, 10, 1),
                                      datetime(2026, 10, 31)).collect()
    # 3 initial + 2 new (2 overlapping deduplicated) = 5 total
    assert len(second_read) == 5
    assert second_read[COLUMN_NAME.TIMESTAMP].is_sorted()

    # Verify updated last timestamp
    connector._last_timestamp_cache = {}
    updated_ts = connector.read_last_timestamp('forex', 'EURUSD', 'tick', year=2026)
    assert updated_ts == datetime(2026, 10, 22, 21, 0, 0)


def test_dukascopy_download_month_raw_empty_on_invalid_bounds():
    """Test download_month_raw returns empty LazyFrame when start >= end."""
    connector = DukascopyConnector()
    # If start_date >= end_date, no download should occur
    result = connector.download_month_raw(
        ticker='EURUSD',
        year=2026,
        month_num=10,
        start_date=datetime(2026, 10, 20, 12, 0, 0),
        end_date=datetime(2026, 10, 20, 10, 0, 0),
        engine='polars_lazy',
    )
    assert isinstance(result, pl.LazyFrame)
    collected = result.collect()
    assert collected.height == 0


def test_download_year_overlap_boundary_calculation(tmp_path):
    """
    Test _download_year calculates boundary start from last_db_ts
    minus overlap_margin.
    """
    mock_last_ts = datetime(2026, 10, 19, 21, 59, 35)
    captured = {}

    def mock_download_month(
        t, y, m, engine='polars_lazy', start_date=None, end_date=None, **kwargs
    ):
        captured['start_date'] = start_date
        captured['end_date'] = end_date
        return pl.DataFrame(schema=POLARS_DTYPE_DICT.TIME_TICK_DTYPE).lazy()

    manager = HistoricalManagerDB(download_overlap_margin='2h', volume_data=True)
    with patch.object(
        LocalDBYearConnector, 'read_last_timestamp', return_value=mock_last_ts
    ):
        with patch.object(
            DukascopyConnector, 'download_month_raw', side_effect=mock_download_month
        ):
            with patch.object(
                DukascopyConnector, 'get_available_tickers', return_value=['EURUSD']
            ):
                manager._download_year('EURUSD', 2026, start_month=10, end_month=10)

    assert 'start_date' in captured
    # 2026-10-19 21:59:35 - 2h = 19:59:35, rounded down to hour = 19:00:00
    assert captured['start_date'] == datetime(2026, 10, 19, 19, 0, 0)
    assert captured['end_date'] is not None
    assert captured['end_date'] <= datetime.now() + timedelta(minutes=1)
