# -*- coding: utf-8 -*-
"""
Tests for normalize_to_naive_utc utility adhering to the UTC Datetime Constraint
and pytest framework conventions.
"""

from datetime import date, datetime, timedelta, timezone

import pytest
from pandas import Timestamp

from forex_data import normalize_to_naive_utc


@pytest.fixture
def sample_aware_utc_plus_2():
    return timezone(timedelta(hours=2))


@pytest.fixture
def sample_aware_utc_minus_5():
    return timezone(timedelta(hours=-5))


def test_normalize_to_naive_utc_none():
    """Verify that None input returns None regardless of to_pydatetime flag."""
    assert normalize_to_naive_utc(None) is None
    assert normalize_to_naive_utc(None, to_pydatetime=True) is None


@pytest.mark.parametrize("now_token", ["now", "NOW", "  now  ", "Now"])
@pytest.mark.parametrize("to_pydatetime", [False, True])
def test_normalize_to_naive_utc_now(now_token, to_pydatetime):
    """Verify 'now' resolves to current naive UTC timestamp or datetime."""
    res = normalize_to_naive_utc(now_token, to_pydatetime=to_pydatetime)
    if to_pydatetime:
        assert isinstance(res, datetime)
        assert res.tzinfo is None
    else:
        assert isinstance(res, Timestamp)
        assert res.tz is None
        assert res.tzinfo is None


@pytest.mark.parametrize(
    "input_val,expected_dt",
    [
        # Naive ISO strings
        ("2020-01-15 14:30:00", datetime(2020, 1, 15, 14, 30, 0)),
        ("2021-12-31T23:59:59", datetime(2021, 12, 31, 23, 59, 59)),
        # Aware ISO strings with timezone offsets
        ("2020-01-15 14:30:00+02:00", datetime(2020, 1, 15, 12, 30, 0)),
        ("2020-01-15 14:30:00-05:00", datetime(2020, 1, 15, 19, 30, 0)),
        ("2022-07-01T08:00:00Z", datetime(2022, 7, 1, 8, 0, 0)),
        # Pure date object (converted to midnight)
        (date(2023, 5, 20), datetime(2023, 5, 20, 0, 0, 0)),
        # Naive datetime
        (datetime(2021, 3, 10, 8, 15, 0), datetime(2021, 3, 10, 8, 15, 0)),
        # Naive pandas Timestamp
        (Timestamp("2020-01-15 12:00:00"), datetime(2020, 1, 15, 12, 0, 0)),
        # Aware pandas Timestamp
        (
            Timestamp("2020-01-15 12:00:00", tz="UTC"),
            datetime(2020, 1, 15, 12, 0, 0),
        ),
        # Unix timestamp in seconds (integer & float)
        (1577836800, datetime(2020, 1, 1, 0, 0, 0)),
        (1577836800.0, datetime(2020, 1, 1, 0, 0, 0)),
        # Unix timestamp in milliseconds (> 1e11)
        (1577836800000, datetime(2020, 1, 1, 0, 0, 0)),
        (1577836800000.0, datetime(2020, 1, 1, 0, 0, 0)),
    ],
)
def test_normalize_to_naive_utc_types(input_val, expected_dt):
    """Verify various input formats are converted to naive Timestamp and datetime."""
    # Test Timestamp return (default)
    res_ts = normalize_to_naive_utc(input_val, to_pydatetime=False)
    assert isinstance(res_ts, Timestamp)
    assert res_ts.tz is None
    assert res_ts.tzinfo is None
    assert res_ts == Timestamp(expected_dt)

    # Test Python datetime return
    res_py = normalize_to_naive_utc(input_val, to_pydatetime=True)
    assert isinstance(res_py, datetime)
    assert res_py.tzinfo is None
    assert res_py == expected_dt


def test_normalize_to_naive_utc_aware_datetime_fixtures(
    sample_aware_utc_plus_2, sample_aware_utc_minus_5
):
    """Verify timezone-aware datetimes with custom offsets are normalized to UTC."""
    dt_plus = datetime(2022, 6, 1, 10, 0, 0, tzinfo=sample_aware_utc_plus_2)
    res_plus = normalize_to_naive_utc(dt_plus)
    assert isinstance(res_plus, Timestamp)
    assert res_plus.tz is None
    assert res_plus == Timestamp(2022, 6, 1, 8, 0, 0)

    dt_minus = datetime(2022, 6, 1, 10, 0, 0, tzinfo=sample_aware_utc_minus_5)
    res_minus = normalize_to_naive_utc(dt_minus, to_pydatetime=True)
    assert isinstance(res_minus, datetime)
    assert res_minus.tzinfo is None
    assert res_minus == datetime(2022, 6, 1, 15, 0, 0)


@pytest.mark.parametrize(
    "aware_str,naive_dt",
    [
        ("2020-01-15 14:30:00+02:00", datetime(2020, 1, 15, 14, 0, 0)),
        ("2022-08-01 10:00:00Z", datetime(2022, 8, 1, 12, 0, 0)),
    ],
)
def test_comparison_between_normalized_dates_never_raises(aware_str, naive_dt):
    """
    Verify comparisons and arithmetic between normalized dates never trigger
    TypeError: can't compare offset-naive and offset-aware datetimes.
    """
    norm1 = normalize_to_naive_utc(aware_str)
    norm2 = normalize_to_naive_utc(naive_dt)

    # Both must be naive and comparable without TypeError
    assert norm1.tz is None
    assert norm2.tz is None
    assert norm1 < norm2
    assert norm2 > norm1
    assert norm2 - norm1 > timedelta(0)


@pytest.mark.parametrize(
    "invalid_input",
    [
        "not-a-valid-date",
        "2020-99-99",
        "invalid_text_input",
    ],
)
def test_normalize_to_naive_utc_invalid_input_raises(invalid_input):
    """Verify invalid date strings raise an appropriate exception."""
    with pytest.raises(Exception):
        normalize_to_naive_utc(invalid_input)
