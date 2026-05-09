# -*- coding: utf-8 -*-
"""
Created on Sun May 09 23:08:00 2026

@author: fiora
"""
import cProfile
import io
import pstats
import time
from contextlib import contextmanager
from pathlib import Path
from sys import stdout

from forex_data import (
    HistoricalManagerDB
)

from loguru import logger

# ── Configuration ────────────────────────────────────────────────────────────
N_STEPS = 5           # number of steps to time in the repeated loop
TOP_N_FUNCS = 30      # how many functions to print in the cProfile report
SORT_BY = 'cumulative'

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
PROFILE_DIR = BASE_DIR / 'profiling'
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

CPROFILE_BIN = PROFILE_DIR / 'comparison_profiling_LocalDB_cprofile.prof'
CPROFILE_TXT = PROFILE_DIR / 'comparison_profiling_LocalDB_cprofile_stats.txt'
TIMING_TXT = PROFILE_DIR / 'comparison_profiling_LocalDB_timing.txt'

config_standard = '''
ENGINE: 'polars_lazy'
DATA_TYPE: 'parquet'
DATA_PATH: '~/.test_database_files'
SSL_VERIFY: False
'''

# ── Helpers ───────────────────────────────────────────────────────────────────


@contextmanager
def phase_timer(label: str, results: dict):
    t0 = time.perf_counter()
    yield
    results[label] = time.perf_counter() - t0


def run_benchmark(config_str: str, label: str, ticker: str):
    logger.info(f"Starting benchmark for: {label}")
    phase_times: dict[str, float] = {}

    with phase_timer("init", phase_times):
        manager = HistoricalManagerDB(config=config_str)

    ex_timeframe_1 = '1D'
    # Use a multi-year span to test the DB's ability to fetch and merge data
    ex_start_date_1 = '2021-01-03 10:00:00'
    ex_end_date_1 = '2023-12-03 10:00:00'

    with phase_timer("get_data_first_run_1D", phase_times):
        manager.get_data(
            ticker=ticker,
            timeframe=ex_timeframe_1,
            start=ex_start_date_1,
            end=ex_end_date_1
        )

    manager.add_timeframe('1W')

    ex_timeframe_2 = '3D'
    ex_start_date_2 = '2019-10-03 10:00:00'
    ex_end_date_2 = '2021-12-03 10:00:00'

    with phase_timer("get_data_first_run_3D", phase_times):
        manager.get_data(
            ticker=ticker,
            timeframe=ex_timeframe_2,
            start=ex_start_date_2,
            end=ex_end_date_2
        )

    logger.info(f"Running {N_STEPS} iterations for stable timing on {label}…")
    fetch_times_1: list[float] = []
    fetch_times_2: list[float] = []

    for _ in range(N_STEPS):
        t0 = time.perf_counter()
        manager.get_data(
            ticker=ticker,
            timeframe=ex_timeframe_1,
            start=ex_start_date_1,
            end=ex_end_date_1
        )
        fetch_times_1.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        manager.get_data(
            ticker=ticker,
            timeframe=ex_timeframe_2,
            start=ex_start_date_2,
            end=ex_end_date_2
        )
        fetch_times_2.append(time.perf_counter() - t0)

    manager.close()

    return {
        'label': label,
        'phase_times': phase_times,
        'fetch_1D_avg': sum(fetch_times_1) / len(fetch_times_1),
        'fetch_3D_avg': sum(fetch_times_2) / len(fetch_times_2),
        'fetch_1D_total': sum(fetch_times_1),
        'fetch_3D_total': sum(fetch_times_2),
        'fetch_1D_min': min(fetch_times_1),
        'fetch_3D_min': min(fetch_times_2),
        'fetch_1D_max': max(fetch_times_1),
        'fetch_3D_max': max(fetch_times_2),
    }


def print_report(res_standard):
    lines = []
    lines.append("=" * 64)
    lines.append("  HistoricalManagerDB — LocalDB Connector Comparison Report")
    lines.append("=" * 64)
    lines.append("")

    for res in [res_standard]:
        lines.append(f"Results for: {res['label']}")
        lines.append("-" * 40)
        lines.append("Phase Breakdown (First Runs, includes download if missing):")
        for k, v in res['phase_times'].items():
            lines.append(f"  {k:<30s} {v * 1000:10.3f} ms")
        lines.append("")
        lines.append(f"Repeated Fetches ({N_STEPS} iterations):")
        lines.append(
            f"  get_data 1D (avg)              {res['fetch_1D_avg'] * 1000:10.3f} ms "
            f"(min: {res['fetch_1D_min'] * 1000:.3f}, "
            f"max: {res['fetch_1D_max'] * 1000:.3f})"
        )
        lines.append(
            f"  get_data 3D (avg)              {res['fetch_3D_avg'] * 1000:10.3f} ms "
            f"(min: {res['fetch_3D_min'] * 1000:.3f}, "
            f"max: {res['fetch_3D_max'] * 1000:.3f})"
        )
        lines.append("")
        lines.append("-" * 40)
        lines.append("")

    report = "\n".join(lines)
    print("\n" + report)
    TIMING_TXT.write_text(report)
    logger.bind(target='profiler').info(f"Comparison report saved → {TIMING_TXT}")


def _write_cprofile_report(profiler: cProfile.Profile) -> None:
    profiler.dump_stats(str(CPROFILE_BIN))
    logger.bind(target='profiler').info(f"cProfile binary saved → {CPROFILE_BIN}")

    buf = io.StringIO()
    stats = pstats.Stats(profiler, stream=buf)
    stats.strip_dirs()
    stats.sort_stats(SORT_BY)
    stats.print_stats(TOP_N_FUNCS)

    stats_text = buf.getvalue()
    CPROFILE_TXT.write_text(stats_text)
    logger.bind(target='profiler').info(f"cProfile stats saved  → {CPROFILE_TXT}")


def main():
    logger.remove()
    # Replace existing logger with styled layout
    logger.add(
        stdout,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(PROFILE_DIR / 'comparison_profiling_LocalDB.log',
               level="TRACE",
               filter=lambda r: True)

    logger.bind(target='profiler').info("Starting comparison profiling run…")

    # Pick one ticker to use for both tests so it's a fair comparison
    # ticker = choice(get_histdata_tickers(verify=False))
    ticker = 'EURNOK'
    logger.bind(target='profiler').info(f"Using ticker: {ticker} for benchmarks")

    profiler = cProfile.Profile()
    profiler.enable()

    res_standard = run_benchmark(config_standard, "Standard (Non-Partitioned)", ticker)

    profiler.disable()
    _write_cprofile_report(profiler)

    print_report(res_standard)

    logger.info("Profiling complete.")
    logger.info(f"All output files are in: {PROFILE_DIR}")


if __name__ == '__main__':
    main()
