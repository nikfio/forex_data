# -*- coding: utf-8 -*-
"""
Utility script to estimate physical timezone difference between HistData & Dukascopy.
Downloads the same time interval of data, shifts HistData by various offsets
(-7 to +7 hours), and matches them with Dukascopy to find the offset that
minimizes the price delta.
"""

import argparse
from pathlib import Path
import polars as pl
from forex_data.data_management import HistDataConnector, DukascopyConnector


def estimate_timezone(ticker: str, year: int, month: int, days_to_check: int = 5):
    BASE_DIR = Path.home() / ".test_database"
    HISTDATA_DATA_PATH = BASE_DIR / "historical_test" / "histdata"
    DUKASCOPY_DATA_PATH = BASE_DIR / "historical_test" / "dukascopy"

    print(f"--- Running Timezone Estimation for {ticker} {year}-{month:02d} ---")
    print(f"Checking first {days_to_check} days...")

    # Load HistData
    print("Downloading / loading HistData...")
    hist_conn = HistDataConnector(data_path=HISTDATA_DATA_PATH, ssl_verify=False)
    try:
        raw_hist = hist_conn.download_month_raw(
            ticker=ticker,
            year=year,
            month_num=month,
            engine="polars_lazy"
        )
        df_hist = raw_hist.collect().sort("timestamp")
    finally:
        hist_conn.clear_temporary_folder()

    # Load Dukascopy
    print("Downloading / loading Dukascopy...")
    duka_conn = DukascopyConnector(data_path=DUKASCOPY_DATA_PATH, ssl_verify=True)
    try:
        raw_duka = duka_conn.download_month_raw(
            ticker=ticker,
            year=year,
            month_num=month,
            engine="polars_lazy"
        )
        df_duka = raw_duka.collect().sort("timestamp")
    finally:
        duka_conn.clear_temporary_folder()

    ts_start = max(df_hist["timestamp"].min(), df_duka["timestamp"].min())
    ts_end = ts_start + pl.duration(days=days_to_check)

    df_hist_sub = df_hist.filter(
        (pl.col("timestamp") >= ts_start)
        & (pl.col("timestamp") <= ts_end)
    )
    df_duka_sub = df_duka.filter(
        (pl.col("timestamp") >= ts_start - pl.duration(hours=12))
        & (pl.col("timestamp") <= ts_end + pl.duration(hours=12))
    )

    print(f"Sub-sampled HistData  : {df_hist_sub.height:,} ticks")
    print(f"Sub-sampled Dukascopy : {df_duka_sub.height:,} ticks")

    results = []
    for h in range(-7, 8):
        df_hist_shifted = df_hist_sub.with_columns(
            (pl.col("timestamp") + pl.duration(hours=h)).alias("timestamp_shifted")
        ).sort("timestamp_shifted")

        joined = df_hist_shifted.join_asof(
            df_duka_sub.select(["timestamp", "p"]).rename({
                "timestamp": "ts_duka",
                "p": "p_duka"
            }),
            left_on="timestamp_shifted",
            right_on="ts_duka",
            strategy="nearest",
            tolerance="5s"
        ).filter(pl.col("p_duka").is_not_null())

        if joined.height > 100:
            price_diff = (joined["p_duka"] - joined["p"]).abs()
            median_diff = price_diff.median()
            std_diff = price_diff.std()
            results.append({
                "offset_hours": h,
                "matched_ticks": joined.height,
                "median_delta": median_diff,
                "std_delta": std_diff
            })
            print(
                f"Offset {h:+d}h: matched {joined.height:,} ticks, "
                f"median price delta: {median_diff:.6f}, std: {std_diff:.6f}"
            )
        else:
            print(f"Offset {h:+d}h: too few matches ({joined.height})")

    print("\nSummary of results (sorted by closest price fit):")
    sorted_results = sorted(results, key=lambda x: x["median_delta"])
    for r in sorted_results:
        best_marker = " <-- BEST FIT" if r == sorted_results[0] else ""
        print(
            f"Offset {r['offset_hours']:+d}h: "
            f"Median Price Delta = {r['median_delta']:.6f}, "
            f"Std = {r['std_delta']:.6f} "
            f"({r['matched_ticks']:,} matches){best_marker}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Estimate timezone difference between HistData and Dukascopy"
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default="EURUSD",
        help="Forex pair ticker (default: EURUSD)"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Year to download (default: 2024)"
    )
    parser.add_argument(
        "--month",
        type=int,
        default=7,
        help="Month to download (default: 7)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=5,
        help="Number of days to check (default: 5)"
    )
    args = parser.parse_args()

    estimate_timezone(
        ticker=args.ticker,
        year=args.year,
        month=args.month,
        days_to_check=args.days
    )


if __name__ == "__main__":
    main()
