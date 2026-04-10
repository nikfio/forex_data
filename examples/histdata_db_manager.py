# -*- coding: utf-8 -*-
"""
Created on Sun Jun 15 11:48:53 2025

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
    HistoricalManagerDB,
    BASE_DATA_COLUMN_NAME,
    is_empty_dataframe,
    shape_dataframe,
    get_dataframe_element,
    get_histdata_tickers
)

from loguru import logger
from random import choice

# ── Configuration ────────────────────────────────────────────────────────────
N_STEPS = 5           # number of steps to time in the repeated loop
TOP_N_FUNCS = 40          # how many functions to print in the cProfile report
SORT_BY = 'cumulative'  # pstats sort key: 'cumulative', 'tottime', 'calls'

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
PROFILE_DIR = BASE_DIR / 'profiling'
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

CPROFILE_BIN = PROFILE_DIR / 'histdata_db_manager_cprofile.prof'
CPROFILE_TXT = PROFILE_DIR / 'histdata_db_manager_cprofile_stats.txt'
TIMING_TXT = PROFILE_DIR / 'histdata_db_manager_timing.txt'


# ── Helpers ───────────────────────────────────────────────────────────────────

@contextmanager
def phase_timer(label: str, results: dict):
    t0 = time.perf_counter()
    yield
    results[label] = time.perf_counter() - t0


def _write_timing_report(
        phase_times: dict,
        fetch_times_1: list[float],
        fetch_times_2: list[float]) -> None:
    lines = []
    lines.append("=" * 64)
    lines.append("  HistoricalManagerDB — histdata_db_manager Timing Report")
    lines.append("=" * 64)
    lines.append("")
    lines.append("Phase Breakdown")
    lines.append("-" * 40)
    for label, elapsed in phase_times.items():
        lines.append(f"  {label:<30s} {elapsed * 1000:10.3f} ms")

    lines.append("")
    lines.append(f"get_data (1D) Timing  ({len(fetch_times_1)} iterations)")
    lines.append("-" * 40)
    if fetch_times_1:
        mean_ms = (sum(fetch_times_1) / len(fetch_times_1)) * 1000
        min_ms = min(fetch_times_1) * 1000
        max_ms = max(fetch_times_1) * 1000
        total_ms = sum(fetch_times_1) * 1000
        lines.append(f"  Total                          {total_ms:10.3f} ms")
        lines.append(f"  Mean per call                  {mean_ms:10.3f} ms")
        lines.append(f"  Min                            {min_ms:10.3f} ms")
        lines.append(f"  Max                            {max_ms:10.3f} ms")

    lines.append("")
    lines.append(f"get_data (3D) Timing  ({len(fetch_times_2)} iterations)")
    lines.append("-" * 40)
    if fetch_times_2:
        mean_ms = (sum(fetch_times_2) / len(fetch_times_2)) * 1000
        min_ms = min(fetch_times_2) * 1000
        max_ms = max(fetch_times_2) * 1000
        total_ms = sum(fetch_times_2) * 1000
        lines.append(f"  Total                          {total_ms:10.3f} ms")
        lines.append(f"  Mean per call                  {mean_ms:10.3f} ms")
        lines.append(f"  Min                            {min_ms:10.3f} ms")
        lines.append(f"  Max                            {max_ms:10.3f} ms")

    lines.append("")
    lines.append("=" * 64)

    report = "\n".join(lines)
    print("\n" + report)
    TIMING_TXT.write_text(report)
    logger.bind(target='profiler').info(f"Timing report saved → {TIMING_TXT}")


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

    print("\n── cProfile top functions ──────────────────────────────────────")
    buf2 = io.StringIO()
    stats2 = pstats.Stats(profiler, stream=buf2)
    stats2.strip_dirs()
    stats2.sort_stats(SORT_BY)
    stats2.print_stats(15)
    print(buf2.getvalue())
    print(f"  Full report ({TOP_N_FUNCS} functions) saved to: {CPROFILE_TXT}")
    print(f"  Binary profile saved to:                        {CPROFILE_BIN}")
    print(f"  Tip: visualise with  →  python -m snakeviz {CPROFILE_BIN}\n")


def _try_line_profiler(manager: HistoricalManagerDB, ex_ticker: str) -> None:
    try:
        from line_profiler import LineProfiler  # type: ignore
    except ImportError:
        logger.bind(target='profiler').warning(
            "line_profiler not installed — skipping per-line timing.\n"
            "Install with: pip install line-profiler"
        )
        return

    lp = LineProfiler()
    lp.add_function(manager.get_data)

    wrapped_get_data = lp(manager.get_data)

    for _ in range(3):
        wrapped_get_data(
            ticker=ex_ticker,
            timeframe='1D',
            start='2018-10-03 10:00:00',
            end='2018-12-03 10:00:00'
        )

    lp_output_path = PROFILE_DIR / 'histdata_db_manager_line_profiler.txt'
    with open(lp_output_path, 'w') as f:
        lp.print_stats(stream=f, output_unit=1e-3)

    logger.bind(
        target='profiler').info(
        f"line_profiler report saved → {lp_output_path}")
    print("── line_profiler (get_data) ────────────────────")
    lp.print_stats(stream=stdout, output_unit=1e-3)


# Use a runtime defined config yaml file
test_config_yaml = '''
ENGINE: 'polars_lazy'
DATA_TYPE: 'parquet'
'''


def main():
    logger.remove()
    # Replace existing logger with styled layout
    logger.add(
        stdout,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(PROFILE_DIR / 'profiling_run_histdata_db.log',
               level="TRACE",
               filter=lambda r: True)

    logger.bind(target='profiler').info("Starting profiling run…")

    phase_times: dict[str, float] = {}
    profiler = cProfile.Profile()
    profiler.enable()

    with phase_timer("init", phase_times):
        histmanager = HistoricalManagerDB(config=test_config_yaml)

    ex_ticker = choice(get_histdata_tickers())
    ex_timeframe_1 = '1D'
    ex_start_date_1 = '2026-01-03 10:00:00'
    ex_end_date_1 = '2026-12-03 10:00:00'

    with phase_timer("get_data (1D)", phase_times):
        yeardata1 = histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe_1,
            start=ex_start_date_1,
            end=ex_end_date_1
        )

    histmanager.add_timeframe('1W')

    ex_timeframe_2 = '3D'
    ex_start_date_2 = '2018-10-03 10:00:00'
    ex_end_date_2 = '2020-12-03 10:00:00'

    with phase_timer("get_data (3D)", phase_times):
        yeardata2 = histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe_2,
            start=ex_start_date_2,
            end=ex_end_date_2
        )

    profiler.disable()
    _write_cprofile_report(profiler)

    logger.info(f"Running {N_STEPS} iterations for stable timing…")
    fetch_times_1: list[float] = []
    fetch_times_2: list[float] = []

    for _ in range(N_STEPS):
        t0 = time.perf_counter()
        histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe_1,
            start=ex_start_date_1,
            end=ex_end_date_1
        )
        fetch_times_1.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe_2,
            start=ex_start_date_2,
            end=ex_end_date_2
        )
        fetch_times_2.append(time.perf_counter() - t0)

    _write_timing_report(phase_times, fetch_times_1, fetch_times_2)
    _try_line_profiler(histmanager, ex_ticker)

    if not is_empty_dataframe(yeardata1):
        logger.bind(
            target='profiler').debug(
            f"get_data (1D): ticker {ex_ticker}, " f"rows {
                shape_dataframe(yeardata1)[0]}, " f"start {
                get_dataframe_element(
                    yeardata1,
                    BASE_DATA_COLUMN_NAME.TIMESTAMP,
                    0)}, " f"end {
                        get_dataframe_element(
                            yeardata1,
                            BASE_DATA_COLUMN_NAME.TIMESTAMP,
                            shape_dataframe(yeardata1)[0] - 1)}")
    else:
        logger.bind(target='profiler').warning("get_data (1D): no data found")

    if not is_empty_dataframe(yeardata2):
        logger.bind(
            target='profiler').debug(
            f"get_data (3D): ticker {ex_ticker}, " f"rows {
                shape_dataframe(yeardata2)[0]}, " f"start {
                get_dataframe_element(
                    yeardata2,
                    BASE_DATA_COLUMN_NAME.TIMESTAMP,
                    0)}, " f"end {
                        get_dataframe_element(
                            yeardata2,
                            BASE_DATA_COLUMN_NAME.TIMESTAMP,
                            shape_dataframe(yeardata2)[0] - 1)}")
    else:
        logger.bind(target='profiler').warning("get_data (3D): no data found")

    logger.info("Profiling complete.")
    logger.info(f"All output files are in: {PROFILE_DIR}")

    histmanager.close()


if __name__ == '__main__':
    main()
