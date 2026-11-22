# -*- coding: utf-8 -*-
"""Tests for normalize_to_naive_utc utility adhering to UTC Datetime Constraint."""

from datetime import date, datetime, timedelta, timezone
import unittest

from pandas import Timestamp

from forex_data import normalize_to_naive_utc


class TestTzNaiveUtc(unittest.TestCase):
    """Verify naive UTC timestamp normalization across input types."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.tz_plus_2 = timezone(timedelta(hours=2))
        cls.tz_minus_5 = timezone(timedelta(hours=-5))

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def test_normalize_to_naive_utc_none(self) -> None:
        """Verify None input returns None regardless of to_pydatetime flag."""
        self.assertIsNone(normalize_to_naive_utc(None))
        self.assertIsNone(normalize_to_naive_utc(None, to_pydatetime=True))

    def test_normalize_to_naive_utc_now(self) -> None:
        """Verify 'now' resolves to current naive UTC timestamp or datetime."""
        for now_token in ("now", "NOW", "  now  ", "Now"):
            for to_pydatetime in (False, True):
                with self.subTest(now_token=now_token, to_pydatetime=to_pydatetime):
                    res = normalize_to_naive_utc(
                        now_token, to_pydatetime=to_pydatetime
                    )
                    if to_pydatetime:
                        self.assertIsInstance(res, datetime)
                        self.assertIsNone(res.tzinfo)
                    else:
                        self.assertIsInstance(res, Timestamp)
                        self.assertIsNone(res.tz)
                        self.assertIsNone(res.tzinfo)

    def test_normalize_to_naive_utc_types(self) -> None:
        """Verify various input formats convert to naive Timestamp & datetime."""
        cases = (
            ("2020-01-15 14:30:00", datetime(2020, 1, 15, 14, 30, 0)),
            ("2021-12-31T23:59:59", datetime(2021, 12, 31, 23, 59, 59)),
            ("2020-01-15 14:30:00+02:00", datetime(2020, 1, 15, 12, 30, 0)),
            ("2020-01-15 14:30:00-05:00", datetime(2020, 1, 15, 19, 30, 0)),
            ("2022-07-01T08:00:00Z", datetime(2022, 7, 1, 8, 0, 0)),
            (date(2023, 5, 20), datetime(2023, 5, 20, 0, 0, 0)),
            (datetime(2021, 3, 10, 8, 15, 0), datetime(2021, 3, 10, 8, 15, 0)),
            (Timestamp("2020-01-15 12:00:00"), datetime(2020, 1, 15, 12, 0, 0)),
            (
                Timestamp("2020-01-15 12:00:00", tz="UTC"),
                datetime(2020, 1, 15, 12, 0, 0),
            ),
            (1577836800, datetime(2020, 1, 1, 0, 0, 0)),
            (1577836800.0, datetime(2020, 1, 1, 0, 0, 0)),
            (1577836800000, datetime(2020, 1, 1, 0, 0, 0)),
            (1577836800000.0, datetime(2020, 1, 1, 0, 0, 0)),
        )
        for input_val, expected_dt in cases:
            with self.subTest(input_val=input_val):
                res_ts = normalize_to_naive_utc(input_val, to_pydatetime=False)
                self.assertIsInstance(res_ts, Timestamp)
                self.assertIsNone(res_ts.tz)
                self.assertIsNone(res_ts.tzinfo)
                self.assertEqual(res_ts, Timestamp(expected_dt))

                res_py = normalize_to_naive_utc(input_val, to_pydatetime=True)
                self.assertIsInstance(res_py, datetime)
                self.assertIsNone(res_py.tzinfo)
                self.assertEqual(res_py, expected_dt)

    def test_normalize_to_naive_utc_aware_datetime_fixtures(self) -> None:
        """Verify timezone-aware datetimes with custom offsets normalize to UTC."""
        dt_plus = datetime(2022, 6, 1, 10, 0, 0, tzinfo=self.tz_plus_2)
        res_plus = normalize_to_naive_utc(dt_plus)
        self.assertIsInstance(res_plus, Timestamp)
        self.assertIsNone(res_plus.tz)
        self.assertEqual(res_plus, Timestamp(2022, 6, 1, 8, 0, 0))

        dt_minus = datetime(2022, 6, 1, 10, 0, 0, tzinfo=self.tz_minus_5)
        res_minus = normalize_to_naive_utc(dt_minus, to_pydatetime=True)
        self.assertIsInstance(res_minus, datetime)
        self.assertIsNone(res_minus.tzinfo)
        self.assertEqual(res_minus, datetime(2022, 6, 1, 15, 0, 0))

    def test_comparison_between_normalized_dates_never_raises(self) -> None:
        """Verify normalized dates compare without offset-naive/aware TypeError."""
        cases = (
            ("2020-01-15 14:30:00+02:00", datetime(2020, 1, 15, 14, 0, 0)),
            ("2022-08-01 10:00:00Z", datetime(2022, 8, 1, 12, 0, 0)),
        )
        for aware_str, naive_dt in cases:
            with self.subTest(aware_str=aware_str, naive_dt=naive_dt):
                norm1 = normalize_to_naive_utc(aware_str)
                norm2 = normalize_to_naive_utc(naive_dt)
                self.assertIsNone(norm1.tz)
                self.assertIsNone(norm2.tz)
                self.assertLess(norm1, norm2)
                self.assertGreater(norm2, norm1)
                self.assertGreater(norm2 - norm1, timedelta(0))

    def test_normalize_to_naive_utc_invalid_input_raises(self) -> None:
        """Verify invalid date strings raise an appropriate exception."""
        for invalid_input in ("not-a-valid-date", "2020-99-99", "invalid_text_input"):
            with self.subTest(invalid_input=invalid_input):
                with self.assertRaises(Exception):
                    normalize_to_naive_utc(invalid_input)
