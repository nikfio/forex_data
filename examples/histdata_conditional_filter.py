# -*- coding: utf-8 -*-
"""
Created on Sun Dec 29 19:56:00 2025

@author: fiora

Example demonstrating conditional filtering with get_data method.
This shows how to use column_name, check_level, and condition parameters
to filter data based on specific column values.
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
    SQL_COMPARISON_OPERATORS,
    SQL_CONDITION_AGGREGATION_MODES,
    is_empty_dataframe,
    shape_dataframe,
)

from loguru import logger

# ── Configuration ────────────────────────────────────────────────────────────
N_STEPS = 5           # number of steps to time in the repeated loop
TOP_N_FUNCS = 40          # how many functions to print in the cProfile report
SORT_BY = 'cumulative'  # pstats sort key: 'cumulative', 'tottime', 'calls'

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
PROFILE_DIR = BASE_DIR / 'profiling'
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

CPROFILE_BIN = PROFILE_DIR / 'histdata_conditional_filter_cprofile.prof'
CPROFILE_TXT = PROFILE_DIR / 'histdata_conditional_filter_cprofile_stats.txt'
TIMING_TXT = PROFILE_DIR / 'histdata_conditional_filter_timing.txt'


# ── Helpers ───────────────────────────────────────────────────────────────────

@contextmanager
def phase_timer(label: str, results: dict):
    t0 = time.perf_counter()
    yield
    results[label] = time.perf_counter() - t0


def _write_timing_report(phase_times: dict, timings: dict) -> None:
    lines = []
    lines.append("=" * 64)
    lines.append("  HistoricalManagerDB — conditional_filter Timing Report")
    lines.append("=" * 64)
    lines.append("")
    lines.append("Phase Breakdown")
    lines.append("-" * 40)
    for label, elapsed in phase_times.items():
        lines.append(f"  {label:<30s} {elapsed * 1000:10.3f} ms")

    for section, t_list in timings.items():
        lines.append("")
        lines.append(f"{section} Timing  ({len(t_list)} iterations)")
        lines.append("-" * 40)
        if t_list:
            mean_ms = (sum(t_list) / len(t_list)) * 1000
            min_ms = min(t_list) * 1000
            max_ms = max(t_list) * 1000
            total_ms = sum(t_list) * 1000
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

    for _ in range(2):
        wrapped_get_data(
            ticker=ex_ticker, timeframe='1D', start='2018-01-01', end='2018-12-31',
            comparison_column_name=BASE_DATA_COLUMN_NAME.OPEN,
            check_level=1.13, comparison_operator=SQL_COMPARISON_OPERATORS.LESS_THAN
        )

    lp_output_path = PROFILE_DIR / 'histdata_conditional_filter_line_profiler.txt'
    with open(lp_output_path, 'w') as f:
        lp.print_stats(stream=f, output_unit=1e-3)

    logger.bind(
        target='profiler').info(
        f"line_profiler report saved → {lp_output_path}")
    print("── line_profiler (get_data) ────────────────────")
    lp.print_stats(stream=stdout, output_unit=1e-3)


# Use a runtime defined config yaml file
test_config_yaml = '''
DATA_FILETYPE: 'parquet'
ENGINE: 'polars_lazy'
'''


def main():
    logger.remove()
    logger.add(
        stdout,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(PROFILE_DIR / 'profiling_run_histdata_cond_filter.log',
               level="TRACE",
               filter=lambda r: True)

    logger.bind(target='profiler').info("Starting profiling run…")

    phase_times: dict[str, float] = {}
    profiler = cProfile.Profile()
    profiler.enable()

    with phase_timer("init", phase_times):
        histmanager = HistoricalManagerDB(config=test_config_yaml)

    # Example 1: Get data with a single condition (open price < threshold)
    ex_ticker = 'EURUSD'
    ex_timeframe = '1D'
    ex_start_date_1 = '2018-01-01'
    ex_end_date_1 = '2018-12-31'
    min_open_value = 1.13

    with phase_timer("Example 1 (Single Condition)", phase_times):
        filtered_data_1 = histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe,
            start=ex_start_date_1,
            end=ex_end_date_1,
            comparison_column_name=BASE_DATA_COLUMN_NAME.OPEN,
            check_level=min_open_value,
            comparison_operator=SQL_COMPARISON_OPERATORS.LESS_THAN
        )

    # Example 2: Get data with multiple conditions
    ex_start_date_2 = '2019-01-01'
    ex_end_date_2 = '2019-12-31'
    high_threshold = 1.145
    low_threshold = 1.12
    condition_mode = SQL_CONDITION_AGGREGATION_MODES.OR

    with phase_timer("Example 2 (Multiple Conditions)", phase_times):
        filtered_data_2 = histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe,
            start=ex_start_date_2,
            end=ex_end_date_2,
            comparison_column_name=[
                BASE_DATA_COLUMN_NAME.HIGH,
                BASE_DATA_COLUMN_NAME.LOW],
            check_level=[
                high_threshold,
                low_threshold],
            comparison_operator=[
                SQL_COMPARISON_OPERATORS.GREATER_THAN,
                SQL_COMPARISON_OPERATORS.LESS_THAN],
            aggregation_mode=condition_mode)

    # Example 3: Compare with non-filtered data
    ex_start_date_3 = '2020-06-01'
    ex_end_date_3 = '2020-06-30'
    close_threshold = 1.12

    with phase_timer("Example 3 (No Filter)", phase_times):
        all_data_3 = histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe,
            start=ex_start_date_3,
            end=ex_end_date_3
        )

    with phase_timer("Example 3 (Close Filter)", phase_times):
        filtered_data_3 = histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe,
            start=ex_start_date_3,
            end=ex_end_date_3,
            comparison_column_name=BASE_DATA_COLUMN_NAME.CLOSE,
            check_level=close_threshold,
            comparison_operator=SQL_COMPARISON_OPERATORS.GREATER_THAN_OR_EQUAL
        )

    profiler.disable()
    _write_cprofile_report(profiler)

    logger.info(f"Running {N_STEPS} iterations for stable timing…")
    timings = {
        'Example 1': [],
        'Example 2': [],
        'Example 3 No Filter': [],
        'Example 3 Close Filter': []
    }

    for _ in range(N_STEPS):
        t0 = time.perf_counter()
        histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe,
            start=ex_start_date_1,
            end=ex_end_date_1,
            comparison_column_name=BASE_DATA_COLUMN_NAME.OPEN,
            check_level=min_open_value,
            comparison_operator=SQL_COMPARISON_OPERATORS.LESS_THAN)
        timings['Example 1'].append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe,
            start=ex_start_date_2,
            end=ex_end_date_2,
            comparison_column_name=[
                BASE_DATA_COLUMN_NAME.HIGH,
                BASE_DATA_COLUMN_NAME.LOW],
            check_level=[
                high_threshold,
                low_threshold],
            comparison_operator=[
                SQL_COMPARISON_OPERATORS.GREATER_THAN,
                SQL_COMPARISON_OPERATORS.LESS_THAN],
            aggregation_mode=condition_mode)
        timings['Example 2'].append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe,
            start=ex_start_date_3,
            end=ex_end_date_3)
        timings['Example 3 No Filter'].append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        histmanager.get_data(
            ticker=ex_ticker,
            timeframe=ex_timeframe,
            start=ex_start_date_3,
            end=ex_end_date_3,
            comparison_column_name=BASE_DATA_COLUMN_NAME.CLOSE,
            check_level=close_threshold,
            comparison_operator=SQL_COMPARISON_OPERATORS.GREATER_THAN_OR_EQUAL)
        timings['Example 3 Close Filter'].append(time.perf_counter() - t0)

    _write_timing_report(phase_times, timings)
    _try_line_profiler(histmanager, ex_ticker)

    if not is_empty_dataframe(filtered_data_1):
        logger.bind(
            target='profiler').success(
            f"Example 1: {ex_ticker}-{ex_timeframe} Found {
                shape_dataframe(filtered_data_1)[0]} entries OPEN < {min_open_value}")

    if not is_empty_dataframe(filtered_data_2):
        logger.bind(
            target='profiler').success(
            f"Example 2: {ex_ticker}-{ex_timeframe} Found "
            f"{shape_dataframe(filtered_data_2)[0]} entries "
            f"HIGH > {high_threshold} OR LOW < {low_threshold}")

    if not is_empty_dataframe(all_data_3) and not is_empty_dataframe(filtered_data_3):
        t_candles = shape_dataframe(all_data_3)[0]
        f_candles = shape_dataframe(filtered_data_3)[0]
        logger.bind(target='profiler').success(
            f"Example 3: {ex_ticker}-{ex_timeframe} Total: {t_candles}, "
            f"Filtered >= {close_threshold}: {f_candles} "
            f"({(f_candles / t_candles * 100):.2f}%)"
        )

    logger.info("Profiling complete.")
    logger.info(f"All output files are in: {PROFILE_DIR}")

    histmanager.close()


if __name__ == '__main__':
    main()
