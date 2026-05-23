# -*- coding: utf-8 -*-
"""
Created on Fri May 23 00:00:00 2026

@author: fiora

Compare EURUSD tick data for July downloaded from two connectors:
  - HistDataConnector  (histdata.com)
  - DukascopyConnector (dukascopy via tick_vault)

Downloads the same month, collects both results into Polars DataFrames,
and prints a side-by-side statistical comparison plus row-level diff
on the common timestamp range.
"""

import time
from pathlib import Path
from sys import stdout

import polars as pl
from loguru import logger

from forex_data.data_management import HistDataConnector, DukascopyConnector
from forex_data.data_management.common import DEFAULT_PATHS

# ── Configuration ─────────────────────────────────────────────────────────────
SYMBOL = "EURUSD"
YEAR   = 2024
MONTH  = 7          # July

# Shared temp / data directory so both connectors write into different sub-dirs
BASE_DIR = Path.home() / ".test_database"
HISTDATA_DATA_PATH  = BASE_DIR / DEFAULT_PATHS.HIST_DATA_FOLDER / "histdata"
DUKASCOPY_DATA_PATH = BASE_DIR / DEFAULT_PATHS.HIST_DATA_FOLDER / "dukascopy"

# Engine used for both downloads
ENGINE = "polars_lazy"

# ── Helpers ───────────────────────────────────────────────────────────────────


def _collect(frame) -> pl.DataFrame:
    """Collect a LazyFrame or pass through a DataFrame."""
    return frame.collect() if isinstance(frame, pl.LazyFrame) else frame


def _describe_df(df: pl.DataFrame, label: str) -> None:
    """Print row count, schema, and descriptive statistics."""
    print(f"\n{'─' * 60}")
    print(f"  {label}")
    print(f"{'─' * 60}")
    print(f"  Rows : {df.height:,}")
    print(f"  Cols : {df.columns}")
    print(f"  Range: {df['timestamp'].min()}  →  {df['timestamp'].max()}")
    print()
    print(df.describe())


def _compare_stats(df_hist: pl.DataFrame, df_duka: pl.DataFrame) -> None:
    """Print a numeric column-by-column comparison (mean / std / min / max)."""
    numeric_cols = ["ask", "bid", "p"]
    stats_rows = []

    for col in numeric_cols:
        if col not in df_hist.columns or col not in df_duka.columns:
            continue
        for stat_name, hist_val, duka_val in [
            ("mean", df_hist[col].mean(),   df_duka[col].mean()),
            ("std",  df_hist[col].std(),    df_duka[col].std()),
            ("min",  df_hist[col].min(),    df_duka[col].min()),
            ("max",  df_hist[col].max(),    df_duka[col].max()),
        ]:
            delta = (duka_val - hist_val) if (hist_val is not None and duka_val is not None) else None
            stats_rows.append({
                "column": col,
                "stat":   stat_name,
                "histdata":   round(hist_val, 6) if hist_val is not None else None,
                "dukascopy":  round(duka_val, 6) if duka_val is not None else None,
                "delta":      round(delta, 6)    if delta is not None else None,
            })

    cmp = pl.DataFrame(stats_rows)
    print(f"\n{'═' * 60}")
    print("  Statistical comparison on common timestamp range")
    print(f"{'═' * 60}")
    print(cmp)


def _timestamp_comparison(df_hist: pl.DataFrame, df_duka: pl.DataFrame, joined: pl.DataFrame) -> None:
    """
    1. Hour-of-day tick distribution for each connector.
    2. Average / median / max absolute timestamp delta on exact-matched ticks.
    """
    print(f"\n{'═' * 60}")
    print("  Timestamp analysis")
    print(f"{'═' * 60}")

    # ── Hour-of-day distribution ──────────────────────────────────────────────
    def hour_counts(df: pl.DataFrame, label: str) -> pl.DataFrame:
        return (
            df.with_columns(pl.col("timestamp").dt.hour().alias("hour"))
            .group_by("hour")
            .agg(pl.len().alias(label))
            .sort("hour")
        )

    hist_hours = hour_counts(df_hist, "histdata_ticks")
    duka_hours = hour_counts(df_duka, "dukascopy_ticks")

    hour_cmp = hist_hours.join(duka_hours, on="hour", how="full", coalesce=True).sort("hour")
    print("\n  Hour-of-day tick distribution (UTC):")
    print(hour_cmp)

    # ── Timestamp delta on exact-matched ticks ────────────────────────────────
    if joined.height == 0:
        print("\n  No exact-timestamp matches — cannot compute timestamp delta.")
        return

    # Both sides share the same timestamp value by definition of an inner join,
    # so delta is always 0 ms on exact matches.  Instead, we measure the
    # nearest-neighbour offset: for every HistData tick, find the closest
    # Dukascopy tick and report the time gap.
    hist_ts = df_hist.select("timestamp").rename({"timestamp": "ts_hist"})
    duka_ts = df_duka.select("timestamp").rename({"timestamp": "ts_duka"})

    # join_asof requires sorted inputs
    nearest = hist_ts.sort("ts_hist").join_asof(
        duka_ts.sort("ts_duka"),
        left_on="ts_hist",
        right_on="ts_duka",
        strategy="nearest",
    ).with_columns(
        ((pl.col("ts_duka").cast(pl.Int64) - pl.col("ts_hist").cast(pl.Int64))
         .abs()
         .alias("abs_delta_ms"))  # milliseconds (timestamp unit is ms)
    )

    mean_ms   = nearest["abs_delta_ms"].mean()
    median_ms = nearest["abs_delta_ms"].median()
    max_ms    = nearest["abs_delta_ms"].max()

    print("\n  Nearest-neighbour timestamp delta (HistData → Dukascopy):")
    print(f"    Mean   : {mean_ms:>10.3f} ms  ({mean_ms / 1000:>8.4f} s)")
    print(f"    Median : {median_ms:>10.3f} ms  ({median_ms / 1000:>8.4f} s)")
    print(f"    Max    : {max_ms:>10.3f} ms  ({max_ms / 1000:>8.4f} s)")

    # Quantile breakdown
    quantiles = [0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
    print("\n  Delta quantiles (ms):")
    for q in quantiles:
        print(f"    p{int(q*100):>2d}  : {nearest['abs_delta_ms'].quantile(q):>10.3f} ms")


def _check_timezone_offset(
    df_hist: pl.DataFrame,
    df_duka: pl.DataFrame,
) -> None:
    """
    Detect a systematic timezone offset between two tick datasets.

    Computes the signed nearest-neighbour delta (hist → duka) per hour of day.
    If the median delta is consistently large (≥ 30 min) across the majority
    of hours, it flags a probable timezone mismatch and estimates the shift.
    """
    hist_ts = df_hist.select("timestamp").rename({"timestamp": "ts_hist"})
    duka_ts = df_duka.select("timestamp").rename({"timestamp": "ts_duka"})

    # Signed nearest-neighbour delta: positive means duka is ahead of hist
    nearest = (
        hist_ts.sort("ts_hist")
        .join_asof(
            duka_ts.sort("ts_duka"),
            left_on="ts_hist",
            right_on="ts_duka",
            strategy="nearest",
        )
        .with_columns([
            (
                (pl.col("ts_duka").cast(pl.Int64) - pl.col("ts_hist").cast(pl.Int64))
                .alias("delta_ms")
            ),
            pl.col("ts_hist").dt.hour().alias("hour"),
        ])
    )

    # Aggregate per hour: median signed delta
    hourly = (
        nearest.group_by("hour")
        .agg(pl.col("delta_ms").median().alias("median_delta_ms"))
        .sort("hour")
    )

    median_deltas = hourly["median_delta_ms"]
    threshold_ms = 30 * 60 * 1000  # 30 minutes in ms

    # Check if most hours show a consistently large offset in the same direction
    large_offset_hours = median_deltas.filter(median_deltas.abs() >= threshold_ms)
    total_hours = median_deltas.len()
    affected_ratio = large_offset_hours.len() / total_hours if total_hours > 0 else 0.0

    if affected_ratio >= 0.7 and large_offset_hours.len() > 0:
        # Estimate the shift as the overall median, rounded to the nearest hour
        overall_median_ms = median_deltas.median()
        shift_hours = round(overall_median_ms / (3_600_000))
        sign = "+" if shift_hours >= 0 else ""

        print(f"\n  {'⚠' * 3}  TIMEZONE OFFSET DETECTED  {'⚠' * 3}")
        print(f"  {large_offset_hours.len()}/{total_hours} hours show a consistent"
              f" median delta ≥ 30 min")
        print(f"  Estimated shift: {sign}{shift_hours}h"
              f" (median delta = {overall_median_ms / 1000:.1f}s)")
        print(f"  → One dataset is likely in a different timezone.\n")
    else:
        print(f"\n  ✓ No timezone offset detected"
              f" ({large_offset_hours.len()}/{total_hours} hours"
              f" with median delta ≥ 30 min)")

    print("\n  Per-hour median signed delta (hist → duka):")
    print(
        hourly.with_columns(
            (pl.col("median_delta_ms") / 1000).round(2).alias("median_delta_s")
        ).select(["hour", "median_delta_s"])
    )


def _overlap_comparison(df_hist: pl.DataFrame, df_duka: pl.DataFrame) -> None:
    """
    Filter both frames to their shared timestamp range, then join on
    timestamp (nearest-match within 1 ms tolerance) and compute per-row
    ask/bid/p deltas.
    """
    ts_start = max(df_hist["timestamp"].min(), df_duka["timestamp"].min())
    ts_end   = min(df_hist["timestamp"].max(), df_duka["timestamp"].max())

    hist_overlap = df_hist.filter(
        (pl.col("timestamp") >= ts_start) & (pl.col("timestamp") <= ts_end)
    )
    duka_overlap = df_duka.filter(
        (pl.col("timestamp") >= ts_start) & (pl.col("timestamp") <= ts_end)
    )

    print(f"\n  Overlapping range: {ts_start}  →  {ts_end}")
    print(f"  HistData rows in overlap : {hist_overlap.height:,}")
    print(f"  Dukascopy rows in overlap: {duka_overlap.height:,}")

    # ── Timezone offset check ─────────────────────────────────────────────────
    _check_timezone_offset(hist_overlap, duka_overlap)

    # Exact-timestamp inner join to find matching ticks
    joined = hist_overlap.join(
        duka_overlap.select(["timestamp", "ask", "bid", "p"]),
        on="timestamp",
        how="inner",
        suffix="_duka"
    ).with_columns([
        (pl.col("ask_duka") - pl.col("ask")).alias("Δask"),
        (pl.col("bid_duka") - pl.col("bid")).alias("Δbid"),
        (pl.col("p_duka")   - pl.col("p")).alias("Δp"),
    ])

    print(f"\n  Exact-timestamp matches : {joined.height:,}")
    if joined.height > 0:
        print("\n  Sample of matched rows (first 10):")
        print(joined.select(["timestamp", "ask", "ask_duka", "Δask",
                              "bid", "bid_duka", "Δbid",
                              "p",   "p_duka",   "Δp"]).head(10))

        print("\n  Delta statistics across matched ticks:")
        print(joined.select(["Δask", "Δbid", "Δp"]).describe())

    _compare_stats(hist_overlap, duka_overlap)
    _timestamp_comparison(hist_overlap, duka_overlap, joined)



# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.remove()
    logger.add(
        stdout,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}"
    )

    print(f"\n{'═' * 60}")
    print(f"  Connector comparison — {SYMBOL}  {YEAR}-{MONTH:02d}")
    print(f"{'═' * 60}\n")

    # ── HistDataConnector ─────────────────────────────────────────────────────
    logger.info("Initialising HistDataConnector …")
    hist_connector = HistDataConnector(
        data_path=HISTDATA_DATA_PATH,
        ssl_verify=False,
    )

    if not hist_connector.check_connection():
        logger.error("HistDataConnector: no network connection. Skipping.")
        df_hist = None
    else:
        logger.info(f"Downloading {SYMBOL} {YEAR}-{MONTH:02d} via HistDataConnector …")
        t0 = time.perf_counter()
        raw_hist = hist_connector.download_month_raw(
            ticker=SYMBOL,
            year=YEAR,
            month_num=MONTH,
            engine=ENGINE,
        )
        elapsed_hist = time.perf_counter() - t0
        df_hist = _collect(raw_hist).sort("timestamp")
        logger.info(f"HistDataConnector finished in {elapsed_hist:.2f}s — {df_hist.height:,} ticks")
        hist_connector.clear_temporary_folder()

    # ── DukascopyConnector ────────────────────────────────────────────────────
    logger.info("Initialising DukascopyConnector …")
    duka_connector = DukascopyConnector(
        data_path=DUKASCOPY_DATA_PATH,
        ssl_verify=True,
    )

    if not duka_connector.check_connection():
        logger.error("DukascopyConnector: no network connection. Skipping.")
        df_duka = None
    else:
        logger.info(f"Downloading {SYMBOL} {YEAR}-{MONTH:02d} via DukascopyConnector …")
        t0 = time.perf_counter()
        raw_duka = duka_connector.download_month_raw(
            ticker=SYMBOL,
            year=YEAR,
            month_num=MONTH,
            engine=ENGINE,
        )
        elapsed_duka = time.perf_counter() - t0
        df_duka = _collect(raw_duka).sort("timestamp")
        logger.info(f"DukascopyConnector finished in {elapsed_duka:.2f}s — {df_duka.height:,} ticks")
        duka_connector.clear_temporary_folder()

    # ── Report ────────────────────────────────────────────────────────────────
    if df_hist is not None:
        _describe_df(df_hist, "HistDataConnector  (histdata.com)")

    if df_duka is not None:
        _describe_df(df_duka, "DukascopyConnector (dukascopy via tick_vault)")

    if df_hist is not None and df_duka is not None:
        print(f"\n{'═' * 60}")
        print("  Download timing")
        print(f"{'═' * 60}")
        print(f"  HistDataConnector  : {elapsed_hist:.2f} s  ({df_hist.height:,} ticks)")
        print(f"  DukascopyConnector : {elapsed_duka:.2f} s  ({df_duka.height:,} ticks)")

        _overlap_comparison(df_hist, df_duka)

    print(f"\n{'═' * 60}")
    print("  Done.")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()
