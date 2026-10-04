# -*- coding: utf-8 -*-
"""
Compare price and volume values between Dukascopy (LocalDB Historical) and a Realtime Connector.

Supports selecting the remote connector via `--connector` (e.g. tiingo, twelvedata).
Splits single-source volume into ask/bid components using configurable split formulas,
and evaluates full volume correlation tables and multi-panel diagnostic plots.

Usage:
    uv run python examples/profiling/compare_dukascopy_vs_realtime_connector.py --help
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import typer
from loguru import logger

from forex_data import (
    COLUMN_NAME,
    HistoricalManagerDB,
    CTraderConnector,
    TiingoConnector,
    TwelveDataConnector,
    is_empty_dataframe,
)

app = typer.Typer(
    name="compare_dukascopy_vs_realtime_connector",
    help="Compare raw prices and volumes between Dukascopy local database and a realtime connector.",
    add_completion=False,
)

DEFAULT_OUTPUT_DIR = (
    Path(__file__).parent.parent / "profiling-logs" / "data_compare"
)


def pip_multiplier(ticker: str) -> float:
    """Return pip multiplier for ticker (100 for JPY pairs, 10000 for standard forex)."""
    return 100.0 if "JPY" in ticker.upper() else 10000.0


def split_volume_series(
    total_vol: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, str]:
    """
    Split a single total volume series equally into ask_volume and bid_volume.
    Returns (ask_vol, bid_vol, formula_description).
    """
    ask_vol = (0.5 * total_vol).astype(np.float32)
    bid_vol = (0.5 * total_vol).astype(np.float32)
    formula_desc = (
        "Equal Neutral Split: ask_vol = 0.5 * V, bid_vol = 0.5 * V (50% / 50%)"
    )
    return ask_vol, bid_vol, formula_desc


def align_datasets(
    df_dukascopy: pl.DataFrame,
    df_connector: pl.DataFrame,
) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """Align Dukascopy and Connector DataFrames on timestamp using an inner join."""
    df_duk = df_dukascopy.with_columns(
        pl.col("timestamp").cast(pl.Datetime("ms"))
    ).sort("timestamp")
    df_conn = df_connector.with_columns(
        pl.col("timestamp").cast(pl.Datetime("ms"))
    ).sort("timestamp")

    duk_total = len(df_duk)
    conn_total = len(df_conn)

    df_aligned = df_duk.join(
        df_conn,
        on="timestamp",
        how="inner",
        suffix="_connector",
    ).sort("timestamp")

    aligned_total = len(df_aligned)
    duk_only = duk_total - aligned_total
    conn_only = conn_total - aligned_total

    meta = {
        "dukascopy_bars": duk_total,
        "connector_bars": conn_total,
        "aligned_bars": aligned_total,
        "dukascopy_only_bars": duk_only,
        "connector_only_bars": conn_only,
    }
    return df_aligned, meta


def compute_comparison_metrics(
    df_aligned: pl.DataFrame,
    ticker: str,
) -> Dict[str, Any]:
    """Compute detailed price and volume comparison metrics between Dukascopy and Connector."""
    mult = pip_multiplier(ticker)
    price_cols = [
        "close",
        "open",
        "high",
        "low",
        "ask",
        "bid",
        "vwmp",
        "vwmp_avg",
    ]
    price_stats = {}

    for col_name in price_cols:
        col_duk = col_name
        col_conn = f"{col_name}_connector"
        if col_duk in df_aligned.columns and col_conn in df_aligned.columns:
            s_duk = df_aligned[col_duk].to_numpy()
            s_conn = df_aligned[col_conn].to_numpy()

            diff = s_conn - s_duk
            diff_pips = diff * mult
            abs_diff_pips = np.abs(diff_pips)

            std_prod = np.std(s_duk) * np.std(s_conn)
            corr = (
                float(np.corrcoef(s_duk, s_conn)[0, 1])
                if std_prod > 1e-12
                else 1.0
            )

            price_stats[col_name] = {
                "duk_mean": float(np.mean(s_duk)),
                "conn_mean": float(np.mean(s_conn)),
                "mean_diff_pips": float(np.mean(diff_pips)),
                "mae_pips": float(np.mean(abs_diff_pips)),
                "max_abs_diff_pips": float(np.max(abs_diff_pips)),
                "rmse_pips": float(np.sqrt(np.mean(diff_pips**2))),
                "correlation": corr,
            }

    # Bid-Ask Spreads
    duk_spread = (
        (df_aligned["ask"] - df_aligned["bid"]).to_numpy() * mult
        if "ask" in df_aligned.columns and "bid" in df_aligned.columns
        else np.array([])
    )
    conn_spread = (
        (df_aligned["ask_connector"] - df_aligned["bid_connector"]).to_numpy() * mult
        if "ask_connector" in df_aligned.columns
        and "bid_connector" in df_aligned.columns
        else np.array([])
    )

    spread_stats = {
        "duk_spread_mean_pips": float(np.mean(duk_spread))
        if len(duk_spread)
        else 0.0,
        "duk_spread_std_pips": float(np.std(duk_spread))
        if len(duk_spread)
        else 0.0,
        "conn_spread_mean_pips": float(np.mean(conn_spread))
        if len(conn_spread)
        else 0.0,
        "conn_spread_std_pips": float(np.std(conn_spread))
        if len(conn_spread)
        else 0.0,
    }

    # Volume Evaluation with split
    duk_ask = df_aligned["ask_volume"].to_numpy().astype(np.float64)
    duk_bid = df_aligned["bid_volume"].to_numpy().astype(np.float64)
    duk_tot = duk_ask + duk_bid

    # Connector Volume
    if "ask_volume_connector" in df_aligned.columns and "bid_volume_connector" in df_aligned.columns:
        conn_raw_tot = (
            df_aligned["ask_volume_connector"] + df_aligned["bid_volume_connector"]
        ).to_numpy().astype(np.float64)
    elif "volume_connector" in df_aligned.columns:
        conn_raw_tot = df_aligned["volume_connector"].to_numpy().astype(np.float64)
    else:
        conn_raw_tot = np.zeros(len(df_aligned), dtype=np.float64)

    # Perform Volume Split on connector total volume
    conn_ask_split, conn_bid_split, formula_desc = split_volume_series(conn_raw_tot)

    # Dukascopy Internal Correlations
    std_ask_bid_duk = np.std(duk_ask) * np.std(duk_bid)
    corr_duk_ask_bid = (
        float(np.corrcoef(duk_ask, duk_bid)[0, 1])
        if std_ask_bid_duk > 1e-12
        else 0.0
    )
    std_ask_tot_duk = np.std(duk_ask) * np.std(duk_tot)
    corr_duk_ask_tot = (
        float(np.corrcoef(duk_ask, duk_tot)[0, 1])
        if std_ask_tot_duk > 1e-12
        else 0.0
    )
    std_bid_tot_duk = np.std(duk_bid) * np.std(duk_tot)
    corr_duk_bid_tot = (
        float(np.corrcoef(duk_bid, duk_tot)[0, 1])
        if std_bid_tot_duk > 1e-12
        else 0.0
    )
    mean_ratio_duk_ask_tot = float(
        np.mean(duk_ask / np.where(duk_tot > 0, duk_tot, 1.0))
    )

    # Cross Provider Correlations
    std_ask_cross = np.std(conn_ask_split) * np.std(duk_ask)
    corr_conn_duk_ask = (
        float(np.corrcoef(conn_ask_split, duk_ask)[0, 1])
        if std_ask_cross > 1e-12
        else 0.0
    )

    std_bid_cross = np.std(conn_bid_split) * np.std(duk_bid)
    corr_conn_duk_bid = (
        float(np.corrcoef(conn_bid_split, duk_bid)[0, 1])
        if std_bid_cross > 1e-12
        else 0.0
    )

    std_tot_cross = np.std(conn_raw_tot) * np.std(duk_tot)
    corr_conn_duk_tot = (
        float(np.corrcoef(conn_raw_tot, duk_tot)[0, 1])
        if std_tot_cross > 1e-12
        else 0.0
    )

    # Relative volume correlation
    sma_duk = max(float(np.mean(duk_tot)), 1e-10)
    norm_duk_vol = (duk_tot / sma_duk) - 1.0

    sma_conn = max(float(np.mean(conn_raw_tot)), 1e-10)
    norm_conn_vol = (conn_raw_tot / sma_conn) - 1.0

    std_norm_cross = np.std(norm_conn_vol) * np.std(norm_duk_vol)
    corr_norm_vol = (
        float(np.corrcoef(norm_conn_vol, norm_duk_vol)[0, 1])
        if std_norm_cross > 1e-12
        else 0.0
    )

    vol_stats = {
        "formula_description": formula_desc,
        "duk_ask_mean": float(np.mean(duk_ask)),
        "duk_bid_mean": float(np.mean(duk_bid)),
        "duk_tot_mean": float(np.mean(duk_tot)),
        "conn_tot_mean": float(np.mean(conn_raw_tot)),
        "conn_ask_split_mean": float(np.mean(conn_ask_split)),
        "conn_bid_split_mean": float(np.mean(conn_bid_split)),
        "scale_ratio": float(np.mean(conn_raw_tot) / max(np.mean(duk_tot), 1e-6)),
        # Correlation Table
        "corr_duk_ask_bid": corr_duk_ask_bid,
        "corr_duk_ask_tot": corr_duk_ask_tot,
        "corr_duk_bid_tot": corr_duk_bid_tot,
        "mean_ratio_duk_ask_tot": mean_ratio_duk_ask_tot,
        "corr_conn_duk_ask": corr_conn_duk_ask,
        "corr_conn_duk_bid": corr_conn_duk_bid,
        "corr_conn_duk_tot": corr_conn_duk_tot,
        "corr_norm_vol": corr_norm_vol,
    }

    return {
        "prices": price_stats,
        "spreads": spread_stats,
        "volumes": vol_stats,
        "_conn_ask_split": conn_ask_split,
        "_conn_bid_split": conn_bid_split,
        "_conn_raw_tot": conn_raw_tot,
        "_duk_ask": duk_ask,
        "_duk_bid": duk_bid,
        "_duk_tot": duk_tot,
    }


def plot_comparison(
    df_aligned: pl.DataFrame,
    metrics: Dict[str, Any],
    connector_name: str,
    ticker: str,
    timeframe: str,
    output_path: Path,
) -> None:
    """Generate and save multi-panel comparison plots including split volumes."""
    mult = pip_multiplier(ticker)
    raw_ts = df_aligned["timestamp"].to_list()
    timestamps = [
        t.to_pydatetime() if hasattr(t, "to_pydatetime") else t
        for t in raw_ts
    ]

    fig, axes = plt.subplots(
        3, 1, figsize=(15, 12), sharex=True, gridspec_kw={"height_ratios": [2.0, 1.1, 1.8]}
    )
    plt.subplots_adjust(hspace=0.18)

    # ── Panel 1: Price Overlay ────────────────────────────────────────────────
    ax_price = axes[0]
    ax_price.plot(
        timestamps,
        df_aligned["close"].to_numpy(),
        label="Dukascopy Close",
        color="#1f77b4",
        linewidth=1.6,
        alpha=0.9,
    )
    ax_price.plot(
        timestamps,
        df_aligned["close_connector"].to_numpy(),
        label=f"{connector_name.capitalize()} Close",
        color="#d62728",
        linestyle="--",
        linewidth=1.3,
        alpha=0.9,
    )

    close_corr = metrics["prices"].get("close", {}).get("correlation", 1.0)
    close_mae = metrics["prices"].get("close", {}).get("mae_pips", 0.0)
    ax_price.set_title(
        f"Raw Price Comparison — Dukascopy vs {connector_name.capitalize()} | {ticker} ({timeframe}) | Corr: {close_corr:.5f} | MAE: {close_mae:.2f} pips",
        fontsize=12,
        fontweight="bold",
    )
    ax_price.set_ylabel("Price", fontsize=11)
    ax_price.legend(loc="upper left", frameon=True, framealpha=0.9)
    ax_price.grid(True, linestyle=":", alpha=0.6)

    # ── Panel 2: Price Residuals in Pips ───────────────────────────────────────
    ax_diff = axes[1]
    diff_pips = (
        (df_aligned["close_connector"] - df_aligned["close"]).to_numpy() * mult
    )
    ax_diff.plot(
        timestamps,
        diff_pips,
        color="#9467bd",
        linewidth=1.2,
        label=rf"$\Delta$ Close ({connector_name.capitalize()} - Dukascopy)",
    )
    ax_diff.axhline(0.0, color="black", linestyle="-", linewidth=0.8, alpha=0.7)
    ax_diff.set_ylabel("Diff (Pips)", fontsize=11)
    ax_diff.legend(loc="upper left", frameon=True, framealpha=0.9)
    ax_diff.grid(True, linestyle=":", alpha=0.6)

    # ── Panel 3: Volume Dynamics with Split ───────────────────────────────────
    ax_vol_duk = axes[2]
    duk_tot = metrics["_duk_tot"]
    conn_tot = metrics["_conn_raw_tot"]
    conn_ask_split = metrics["_conn_ask_split"]
    conn_bid_split = metrics["_conn_bid_split"]

    vol_corr = metrics["volumes"]["corr_conn_duk_tot"]
    vol_scale = metrics["volumes"]["scale_ratio"]

    # Check if scales are comparable (within 10x) or need dual y-axis
    if 0.1 <= vol_scale <= 10.0:
        # Same magnitude scale (e.g. cTrader broker ticks vs Dukascopy ticks)
        ax_vol_duk.plot(
            timestamps,
            duk_tot,
            label="Dukascopy Total Volume",
            color="#1f77b4",
            lw=1.5,
        )
        ax_vol_duk.plot(
            timestamps,
            conn_tot,
            label=f"{connector_name.capitalize()} Total Volume",
            color="#2ca02c",
            ls="--",
            lw=1.4,
        )
        ax_vol_duk.plot(
            timestamps,
            conn_ask_split,
            label=f"{connector_name.capitalize()} Ask Volume (Split)",
            color="#ff7f0e",
            ls=":",
            lw=1.2,
        )
        ax_vol_duk.plot(
            timestamps,
            conn_bid_split,
            label=f"{connector_name.capitalize()} Bid Volume (Split)",
            color="#d62728",
            ls=":",
            lw=1.2,
        )
        ax_vol_duk.set_ylabel("Ticks / Volume", fontsize=11)
        ax_vol_duk.legend(loc="upper left", frameon=True, framealpha=0.9)
    else:
        # Disparate units (e.g. Dukascopy ticks vs Tiingo base currency units)
        ax_vol_conn = ax_vol_duk.twinx()
        line1 = ax_vol_duk.plot(
            timestamps,
            duk_tot,
            label="Dukascopy Total Volume (Ticks)",
            color="#1f77b4",
            lw=1.5,
        )
        line2 = ax_vol_conn.plot(
            timestamps,
            conn_tot,
            label=f"{connector_name.capitalize()} Total Volume (Units)",
            color="#2ca02c",
            ls="--",
            lw=1.4,
        )
        line3 = ax_vol_conn.plot(
            timestamps,
            conn_ask_split,
            label=f"{connector_name.capitalize()} Split Ask Volume",
            color="#ff7f0e",
            ls=":",
            lw=1.2,
        )
        lines = line1 + line2 + line3
        labels = [l.get_label() for l in lines]
        ax_vol_duk.legend(lines, labels, loc="upper left", frameon=True, framealpha=0.9)
        ax_vol_duk.set_ylabel("Dukascopy (Ticks)", color="#1f77b4", fontsize=11)
        ax_vol_conn.set_ylabel(f"{connector_name.capitalize()} (Units)", color="#2ca02c", fontsize=11)
        ax_vol_duk.tick_params(axis="y", labelcolor="#1f77b4")
        ax_vol_conn.tick_params(axis="y", labelcolor="#2ca02c")

    ax_vol_duk.set_title(
        f"Volume Dynamics | Formula: {metrics['volumes']['formula_description']} | Corr: {vol_corr:.4f}",
        fontsize=11,
        fontweight="bold",
    )
    ax_vol_duk.grid(True, linestyle=":", alpha=0.6)
    ax_vol_duk.set_xlabel("Timestamp", fontsize=11)
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    fig.autofmt_xdate()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Comparison plot saved to: {output_path}")


def generate_mock_connector_data(
    df_duk: pl.DataFrame,
    connector_name: str,
) -> pl.DataFrame:
    """Generate realistic synthetic connector data for offline testing."""
    logger.warning(f"Generating synthetic mock {connector_name} data for demonstration...")
    n = len(df_duk)
    np.random.seed(42)

    noise = np.random.normal(0.0, 0.00002, size=n).astype(np.float32)
    mock_df = df_duk.with_columns([
        (pl.col("open") + pl.Series(noise)).alias("open"),
        (pl.col("high") + pl.Series(np.maximum(noise, 0.0))).alias("high"),
        (pl.col("low") + pl.Series(np.minimum(noise, 0.0))).alias("low"),
        (pl.col("close") + pl.Series(noise)).alias("close"),
        (pl.col("close") + 0.00010).alias("ask"),
        (pl.col("close") - 0.00010).alias("bid"),
        pl.lit(1500000.0, dtype=pl.Float32).alias("ask_volume"),
        pl.lit(1500000.0, dtype=pl.Float32).alias("bid_volume"),
        (pl.col("close") + pl.Series(noise)).alias("vwmp"),
        (pl.col("close") + pl.Series(noise)).alias("vwmp_avg"),
    ])
    return mock_df


@app.command()
def main(
    connector: str = typer.Option(
        "tiingo",
        "--connector",
        "-c",
        help="Realtime connector to compare (tiingo, twelvedata).",
    ),
    ticker: str = typer.Option(
        "EURUSD",
        "--ticker",
        "-t",
        help="Currency pair ticker (e.g. EURUSD).",
    ),
    timeframe: str = typer.Option(
        "1h",
        "--timeframe",
        "-tf",
        help="Bar timeframe (e.g. 5m, 10m, 1h, 4h).",
    ),
    start_date: str = typer.Option(
        "2024-01-15 00:00:00",
        "--start-date",
        "-s",
        help="Start date and time (YYYY-MM-DD HH:MM:SS).",
    ),
    end_date: str = typer.Option(
        "2024-01-19 23:59:59",
        "--end-date",
        "-e",
        help="End date and time (YYYY-MM-DD HH:MM:SS).",
    ),
    data_path: Path = typer.Option(
        Path.home() / ".test_vol_database",
        "--data-path",
        "-d",
        help="Path to local database containing Dukascopy historical parquets.",
    ),
    api_key: Optional[str] = typer.Option(
        None,
        "--api-key",
        "-k",
        help="API key for the selected connector (falls back to provider env var).",
    ),
    output_dir: Path = typer.Option(
        DEFAULT_OUTPUT_DIR,
        "--output-dir",
        "-o",
        help="Directory to save comparison reports and plots.",
    ),
    plot: bool = typer.Option(
        True,
        "--plot/--no-plot",
        help="Whether to generate and save comparison plots.",
    ),
    mock: bool = typer.Option(
        False,
        "--mock/--no-mock",
        help="Use mock data for offline dry-run testing.",
    ),
) -> None:
    """Compare raw prices and volumes between Dukascopy local database and a realtime connector."""
    output_dir.mkdir(parents=True, exist_ok=True)
    connector_clean = connector.lower().strip()

    logger.info("=" * 70)
    logger.info(f"   Forex Data Profiling: Dukascopy (LocalDB) vs {connector_clean.capitalize()}")
    logger.info("=" * 70)
    logger.info(f"Connector       : {connector_clean}")
    logger.info(f"Ticker          : {ticker}")
    logger.info(f"Timeframe       : {timeframe}")
    logger.info(f"Interval        : {start_date} -> {end_date}")
    logger.info(f"Data Path       : {data_path}")
    logger.info(f"Output Dir      : {output_dir}")

    # 1. Fetch Dukascopy Data
    logger.info("Fetching Dukascopy historical data from LocalDB...")
    hist_mgr = HistoricalManagerDB(
        data_path=str(data_path),
        engine="polars_lazy",
        data_type="parquet",
        db_files_year_partitioning=True,
    )
    lf_duk = hist_mgr.get_data(
        ticker=ticker,
        timeframe=timeframe,
        start=start_date,
        end=end_date,
    )

    if is_empty_dataframe(lf_duk):
        logger.error(f"No Dukascopy data found in {data_path} for {ticker} ({timeframe}).")
        raise typer.Exit(code=1)

    df_duk = lf_duk.collect() if hasattr(lf_duk, "collect") else lf_duk
    logger.info(f"Retrieved {len(df_duk)} Dukascopy bars.")

    # 2. Fetch Connector Data
    df_conn: pl.DataFrame
    if mock:
        df_conn = generate_mock_connector_data(df_duk, connector_clean)
    elif connector_clean == "tiingo":
        effective_key = api_key or os.environ.get("TIINGO_API_KEY", "")
        if not effective_key:
            logger.warning("TIINGO_API_KEY not found. Falling back to --mock data.")
            df_conn = generate_mock_connector_data(df_duk, connector_clean)
        else:
            logger.info("Fetching Tiingo intraday candles...")
            conn = TiingoConnector(
                api_key=effective_key,
                plan="free",
                data_path=data_path / "Realtime_Tiingo",
            )
            lf_conn = conn.get_data(
                symbol=ticker,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
            )
            df_conn = lf_conn.collect() if hasattr(lf_conn, "collect") else lf_conn
    elif connector_clean == "ctrader":
        c_id = os.environ.get("CTRADER_CLIENT_ID", "")
        c_secret = os.environ.get("CTRADER_CLIENT_SECRET", "")
        token = api_key or os.environ.get("CTRADER_ACCESS_TOKEN", "")
        acc_id_str = os.environ.get("CTRADER_ACCOUNT_ID", "49024232")

        if not (c_id and token):
            logger.warning("cTrader credentials not found. Falling back to --mock data.")
            df_conn = generate_mock_connector_data(df_duk, connector_clean)
        else:
            logger.info("Fetching cTrader trendbars via CTraderConnector...")
            conn = CTraderConnector(
                client_id=c_id,
                client_secret=c_secret,
                access_token=token,
                broker_account_id=acc_id_str,
                data_path=data_path / "Realtime_cTrader",
            )
            lf_conn = conn.get_data(
                symbol=ticker,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
            )
            df_conn = lf_conn.collect() if hasattr(lf_conn, "collect") else lf_conn
    elif connector_clean == "twelvedata":
        effective_key = api_key or os.environ.get("TWELVE_DATA_API_KEY", "")
        if not effective_key:
            logger.warning("TWELVE_DATA_API_KEY not found. Falling back to --mock data.")
            df_conn = generate_mock_connector_data(df_duk, connector_clean)
        else:
            logger.info("Fetching TwelveData intraday candles...")
            conn = TwelveDataConnector(
                api_key=effective_key,
                plan="free",
                data_path=data_path / "Realtime_TwelveData",
            )
            lf_conn = conn.get_data(
                symbol=ticker,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
            )
            df_conn = lf_conn.collect() if hasattr(lf_conn, "collect") else lf_conn
    else:
        logger.error(f"Unsupported connector: {connector}. Supported: ctrader, tiingo, twelvedata.")
        raise typer.Exit(code=1)

    if is_empty_dataframe(df_conn):
        logger.error(f"No data returned for connector: {connector_clean}.")
        raise typer.Exit(code=1)

    logger.info(f"Retrieved {len(df_conn)} bars from {connector_clean}.")

    # 3. Align on timestamp
    df_aligned, align_meta = align_datasets(df_duk, df_conn)
    if is_empty_dataframe(df_aligned):
        logger.error("No overlapping timestamps found between Dukascopy and connector.")
        raise typer.Exit(code=1)

    logger.info(f"Alignment complete: {align_meta['aligned_bars']} overlapping bars found.")

    # 4. Compute Metrics
    metrics = compute_comparison_metrics(df_aligned, ticker)

    # 5. Format & Save Text Report
    v_stats = metrics["volumes"]
    report_lines = [
        "=" * 82,
        f"   COMPARISON REPORT: DUKASCOPY vs {connector_clean.upper()} — {ticker} ({timeframe})",
        "=" * 82,
        f"Interval            : {start_date} -> {end_date}",
        f"Aligned Bars        : {align_meta['aligned_bars']}",
        f"Connector           : {connector_clean}",
        f"Volume Split Formula: {v_stats['formula_description']}",
        "-" * 82,
        "PRICE COMPARISONS (Difference in Pips = Connector - Dukascopy):",
        f"{'Feature':<12} | {'Duk Mean':<10} | {'Conn Mean':<10} | {'Mean Diff':<10} | {'MAE (pips)':<10} | {'Corr':<8}",
        "-" * 82,
    ]

    for p_col, p_data in metrics["prices"].items():
        report_lines.append(
            f"{p_col:<12} | {p_data['duk_mean']:<10.5f} | {p_data['conn_mean']:<10.5f} | "
            f"{p_data['mean_diff_pips']:<+10.2f} | {p_data['mae_pips']:<10.2f} | {p_data['correlation']:<8.5f}"
        )

    report_lines.extend([
        "-" * 82,
        "BID-ASK SPREAD COMPARISON (Pips):",
        f"  Dukascopy Mean Spread: {metrics['spreads']['duk_spread_mean_pips']:.2f} pips (std: {metrics['spreads']['duk_spread_std_pips']:.2f})",
        f"  Connector Mean Spread: {metrics['spreads']['conn_spread_mean_pips']:.2f} pips (std: {metrics['spreads']['conn_spread_std_pips']:.2f})",
        "-" * 82,
        "VOLUME ANALYSIS & CORRELATION TABLE:",
        f"  Volume Scale Ratio (Connector/Dukascopy) : {v_stats['scale_ratio']:.2f}x",
        f"  [Dukascopy] Corr(V_ask, V_bid)             : {v_stats['corr_duk_ask_bid']:.4f}",
        f"  [Dukascopy] Corr(V_ask, V_total)           : {v_stats['corr_duk_ask_tot']:.4f}",
        f"  [Dukascopy] Corr(V_bid, V_total)           : {v_stats['corr_duk_bid_tot']:.4f}",
        f"  [Dukascopy] Mean Ratio (V_ask / V_total)   : {v_stats['mean_ratio_duk_ask_tot']:.4f}",
        f"  -------------------------------------------------------------",
        f"  Corr(V_connector_ask, V_dukascopy_ask)     : {v_stats['corr_conn_duk_ask']:.4f}",
        f"  Corr(V_connector_bid, V_dukascopy_bid)     : {v_stats['corr_conn_duk_bid']:.4f}",
        f"  Corr(V_connector_total, V_dukascopy_total) : {v_stats['corr_conn_duk_tot']:.4f}",
        f"  Corr(norm_V_connector, norm_V_dukascopy)   : {v_stats['corr_norm_vol']:.4f} (Relative Volume)",
        "=" * 82,
    ])

    report_text = "\n".join(report_lines)
    print(report_text)

    summary_file = (
        output_dir / f"compare_dukascopy_vs_{connector_clean}_{ticker}_{timeframe}_summary.txt"
    )
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(report_text)
    logger.info(f"Summary report written to: {summary_file}")

    # 6. Generate Plot
    if plot:
        plot_file = (
            output_dir / f"compare_dukascopy_vs_{connector_clean}_{ticker}_{timeframe}.png"
        )
        plot_comparison(
            df_aligned,
            metrics,
            connector_clean,
            ticker,
            timeframe,
            plot_file,
        )


if __name__ == "__main__":
    app()
