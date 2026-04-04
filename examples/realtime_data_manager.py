# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 22:13:04 2022

@author: fiora

test data_manager object realtime feature:

    1) test real time data providers

    2) test timeframe flexibility as in historical data manager

    3) download indicators values

"""
import cProfile
import io
import pstats
import time
from contextlib import contextmanager
from pathlib import Path
from sys import stdout

from loguru import logger

from pandas import (
    Timestamp,
    Timedelta
)
# custom lib
from forex_data import (
    BASE_DATA_COLUMN_NAME,
    RealtimeManager,
    get_dataframe_element,
    is_empty_dataframe,
    shape_dataframe
)

from os import getenv

# Use a runtime defined config yaml file
alpha_vantage_key = getenv('ALPHA_VANTAGE_API_KEY')
polygon_io_key = getenv('POLYGON_IO_API_KEY')
if not alpha_vantage_key:
    raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required")
if not polygon_io_key:
    raise ValueError("POLYGON_IO_API_KEY environment variable is required")

test_config_yaml = f'''
DATA_FILETYPE: 'parquet'
ENGINE: 'polars_lazy'

PROVIDERS_KEY:
    ALPHA_VANTAGE_API_KEY : {alpha_vantage_key},
    POLYGON_IO_API_KEY    : {polygon_io_key}

'''

# ── Configuration ────────────────────────────────────────────────────────────
N_STEPS = 5           # number of steps to time in the repeated loop
TOP_N_FUNCS = 40          # how many functions to print in the cProfile report
SORT_BY = 'cumulative'  # pstats sort key: 'cumulative', 'tottime', 'calls'

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
PROFILE_DIR = BASE_DIR / 'profiling'
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

CPROFILE_BIN = PROFILE_DIR / 'realtime_data_manager_cprofile.prof'
CPROFILE_TXT = PROFILE_DIR / 'realtime_data_manager_cprofile_stats.txt'
TIMING_TXT = PROFILE_DIR / 'realtime_data_manager_timing.txt'


# ── Helpers ───────────────────────────────────────────────────────────────────

@contextmanager
def phase_timer(label: str, results: dict):
    t0 = time.perf_counter()
    yield
    results[label] = time.perf_counter() - t0


def _write_timing_report(phase_times: dict, timings: dict) -> None:
    lines = []
    lines.append("=" * 64)
    lines.append("  RealtimeManager — realtime_data_manager Timing Report")
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


def _try_line_profiler(manager: RealtimeManager, ex_ticker: str) -> None:
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
    lp.add_function(manager.get_daily_close)

    w_get_data = lp(manager.get_data)
    w_get_daily_close = lp(manager.get_daily_close)

    for _ in range(2):
        w_get_daily_close(ticker=ex_ticker, last_close=True)
        w_get_data(
            ticker='EURUSD',
            start=Timestamp.now() - Timedelta('10D'),
            end=Timestamp.now() - Timedelta('8D'),
            timeframe='5m'
        )

    lp_output_path = PROFILE_DIR / 'realtime_data_manager_line_profiler.txt'
    with open(lp_output_path, 'w') as f:
        lp.print_stats(stream=f, output_unit=1e-3)

    logger.bind(
        target='profiler').info(
        f"line_profiler report saved → {lp_output_path}")
    print("── line_profiler ────────────────────")
    lp.print_stats(stream=stdout, output_unit=1e-3)


def main():
    logger.remove()
    logger.add(
        stdout,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(PROFILE_DIR / 'profiling_run_realtime_data.log',
               level="TRACE",
               filter=lambda r: True)

    logger.bind(target='profiler').info("Starting profiling run…")

    phase_times: dict[str, float] = {}
    profiler = cProfile.Profile()
    profiler.enable()

    with phase_timer("init", phase_times):
        realtimedata_manager = RealtimeManager(config=test_config_yaml)

    ex_ticker = 'EURCAD'

    with phase_timer("get_daily_close (last close)", phase_times):
        dayclose_quote = realtimedata_manager.get_daily_close(
            ticker=ex_ticker, last_close=True
        )

    ex_n_days = 13
    with phase_timer("get_daily_close (recent days)", phase_times):
        window_daily_ohlc = realtimedata_manager.get_daily_close(
            ticker=ex_ticker, recent_days_window=ex_n_days
        )

    ex_start_date_limits = '2025-01-15'
    ex_end_date_limits = '2025-01-23'
    with phase_timer("get_daily_close (start/end)", phase_times):
        window_limits_daily_ohlc = realtimedata_manager.get_daily_close(
            ticker=ex_ticker, day_start=ex_start_date_limits, day_end=ex_end_date_limits
        )

    ex_start_date_tf = '2024-04-10'
    ex_end_date_tf = '2024-04-15'
    ex_timeframe_tf = '1h'
    with phase_timer("get_data (timeframe)", phase_times):
        window_data_ohlc = realtimedata_manager.get_data(
            ticker=ex_ticker,
            start=ex_start_date_tf,
            end=ex_end_date_tf,
            timeframe=ex_timeframe_tf)

    ex_start_date_intra = Timestamp.now() - Timedelta('10D')
    ex_end_date_intra = Timestamp.now() - Timedelta('8D')
    ex_timeframe_intra = '5m'
    with phase_timer("get_data (intraday)", phase_times):
        window_data_ohlc_intraday = realtimedata_manager.get_data(
            ticker='EURUSD',
            start=ex_start_date_intra,
            end=ex_end_date_intra,
            timeframe=ex_timeframe_intra)

    profiler.disable()
    _write_cprofile_report(profiler)

    logger.info(f"Running {N_STEPS} iterations for stable timing…")
    timings = {
        'last_close': [],
        'recent_days': [],
        'start_end_daily': [],
        'get_data_tf': [],
        'get_data_intraday': []
    }

    for _ in range(N_STEPS):
        t0 = time.perf_counter()
        realtimedata_manager.get_daily_close(ticker=ex_ticker, last_close=True)
        timings['last_close'].append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        realtimedata_manager.get_daily_close(
            ticker=ex_ticker, recent_days_window=ex_n_days)
        timings['recent_days'].append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        realtimedata_manager.get_daily_close(
            ticker=ex_ticker,
            day_start=ex_start_date_limits,
            day_end=ex_end_date_limits)
        timings['start_end_daily'].append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        realtimedata_manager.get_data(
            ticker=ex_ticker,
            start=ex_start_date_tf,
            end=ex_end_date_tf,
            timeframe=ex_timeframe_tf)
        timings['get_data_tf'].append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        realtimedata_manager.get_data(
            ticker='EURUSD',
            start=ex_start_date_intra,
            end=ex_end_date_intra,
            timeframe=ex_timeframe_intra)
        timings['get_data_intraday'].append(time.perf_counter() - t0)

    _write_timing_report(phase_times, timings)
    _try_line_profiler(realtimedata_manager, ex_ticker)

    # Logging outputs
    if not is_empty_dataframe(dayclose_quote):
        logger.bind(
            target='profiler').debug(
            f"get_daily_close: ticker {ex_ticker} rows {
                shape_dataframe(dayclose_quote)[0]} " f"date {
                get_dataframe_element(
                    dayclose_quote,
                    BASE_DATA_COLUMN_NAME.TIMESTAMP,
                    0)}")
    if not is_empty_dataframe(window_daily_ohlc):
        logger.bind(
            target='profiler').debug(
            f"get_daily_close (recent): ticker {ex_ticker} rows {
                shape_dataframe(window_daily_ohlc)[0]} " f"date {
                get_dataframe_element(
                    window_daily_ohlc,
                    BASE_DATA_COLUMN_NAME.TIMESTAMP,
                    0)}")
    if not is_empty_dataframe(window_limits_daily_ohlc):
        logger.bind(
            target='profiler').debug(
            f"get_daily_close (limits): ticker {ex_ticker} rows {
                shape_dataframe(window_limits_daily_ohlc)[0]} " f"date {
                get_dataframe_element(
                    window_limits_daily_ohlc,
                    BASE_DATA_COLUMN_NAME.TIMESTAMP,
                    0)}")
    if not is_empty_dataframe(window_data_ohlc):
        logger.bind(
            target='profiler').debug(
            f"get_data (tf): ticker {ex_ticker} timeframe {ex_timeframe_tf} rows {
                shape_dataframe(window_data_ohlc)[0]} " f"start {
                get_dataframe_element(
                    window_data_ohlc,
                    BASE_DATA_COLUMN_NAME.TIMESTAMP,
                    0)}")
    if not is_empty_dataframe(window_data_ohlc_intraday):
        logger.bind(
            target='profiler').debug(
            f"get_data (intraday): ticker EURUSD timeframe {ex_timeframe_intra} rows {
                shape_dataframe(window_data_ohlc_intraday)[0]} " f"start {
                get_dataframe_element(
                    window_data_ohlc_intraday,
                    BASE_DATA_COLUMN_NAME.TIMESTAMP,
                    0)}")

    logger.info("Profiling complete.")
    logger.info(f"All output files are in: {PROFILE_DIR}")


if __name__ == '__main__':
    main()
