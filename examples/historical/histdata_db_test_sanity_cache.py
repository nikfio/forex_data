# -*- coding: utf-8 -*-
"""
Created on Thu Apr 03 00:10:00 2026

@author: fiora

Sanity test for HistoricalManagerDB with multiple random queries on a single ticker.
Purpose: Verify that repeated random calls to the database (and cache) do not
produce errors and return valid data where expected.
"""
import random
from datetime import datetime, timedelta
from sys import stdout

from forex_data import (
    HistoricalManagerDB,
    BASE_DATA_COLUMN_NAME,
    SQL_COMPARISON_OPERATORS,
    is_empty_dataframe,
    shape_dataframe,
)

from loguru import logger

# ── Configuration ────────────────────────────────────────────────────────────
N_ITERATIONS = 50       # Number of random queries to perform
TICKER = 'EURUSD'       # Predefined ticker for sanity test

# Runtime defined config yaml file
test_config_yaml = '''
DATA_FILETYPE: 'parquet'
ENGINE: 'polars_lazy'
DATA_PATH: '~/.test_database'
'''


def get_random_date_range(start_limit, end_limit, max_days=365):
    """Generate a random start and end date within limits."""
    delta = end_limit - start_limit
    random_days_start = random.randint(0, delta.days - 10)
    start_date = start_limit + timedelta(days=random_days_start)

    # Random duration between 1 and max_days
    duration_days = random.randint(1, min(max_days, (end_limit - start_date).days))
    end_date = start_date + timedelta(days=duration_days)

    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def main():
    # Setup styling for terminal output
    logger.remove()
    logger.add(
        stdout,
        level="INFO",
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")

    logger.info(f"Starting sanity test for {TICKER} with {N_ITERATIONS} iterations...")

    try:
        # Initialize the manager
        histmanager = HistoricalManagerDB(config=test_config_yaml)

        # Verify TICKER is in database
        available_tickers = histmanager._get_ticker_list()
        if TICKER.lower() not in available_tickers:
            msg = f"Ticker {TICKER} not found. Available: {available_tickers}"
            logger.error(msg)
            histmanager.close()
            return

        ticker = TICKER.lower()
        # Get available timeframes for TICKER
        timeframes = histmanager._get_ticker_timeframes_list(ticker)
        if not timeframes:
            logger.error(f"No timeframes found for {TICKER}. Exiting.")
            histmanager.close()
            return

        logger.info(f"Available timeframes for {TICKER}: {timeframes}")

        # Get start/end dates programmatically
        years = histmanager._get_ticker_years_list(ticker)
        if not years:
            logger.error(f"No data years found for {TICKER}. Exiting.")
            histmanager.close()
            return

        start_year = min(years)
        end_year = max(years)
        start_limit = datetime(start_year, 1, 1)
        end_limit = datetime(end_year, 12, 31)

        msg = f"HistoricalManagerDB initialized. Testing range: {start_year}-{end_year}"
        logger.success(msg)

        success_count = 0
        empty_count = 0
        error_count = 0

        for i in range(1, N_ITERATIONS + 1):
            tf = random.choice(timeframes)
            start, end = get_random_date_range(start_limit, end_limit)

            # Randomly decide whether to add a conditional filter
            use_filter = random.random() > 0.6

            try:
                if use_filter:
                    # Example filter: Open < random price (roughly centered around 1.1)
                    threshold = round(random.uniform(1.0, 1.3), 4)
                    df = histmanager.get_data(
                        ticker=ticker,
                        timeframe=tf,
                        start=start,
                        end=end,
                        comparison_column_name=BASE_DATA_COLUMN_NAME.OPEN,
                        check_level=threshold,
                        comparison_operator=SQL_COMPARISON_OPERATORS.LESS_THAN
                    )
                    filter_str = f" | Filter: OPEN < {threshold}"
                else:
                    df = histmanager.get_data(
                        ticker=ticker,
                        timeframe=tf,
                        start=start,
                        end=end
                    )
                    filter_str = ""

                if is_empty_dataframe(df):
                    empty_count += 1
                    status = "<yellow>EMPTY</yellow>"
                else:
                    success_count += 1
                    rows, cols = shape_dataframe(df)
                    status = f"<cyan>{rows} rows</cyan>"

                if i % 5 == 0 or i == 1:
                    log_msg = (
                        f"Iter {i:2d}/{N_ITERATIONS}: {tf} | {start} to "
                        f"{end}{filter_str} -> {status}"
                    )
                    logger.opt(colors=True).info(log_msg)

            except Exception as e:
                error_count += 1
                logger.error(f"Iter {i:2d} FAILED: {tf} | {start} to {end} -> {str(e)}")

        # Final summary
        logger.info("-" * 50)
        logger.info("Sanity Test Summary:")
        logger.info(f"  Total Iterations: {N_ITERATIONS}")
        logger.info(f"  Successful (with data): {success_count}")
        logger.info(f"  Successful (empty results): {empty_count}")
        logger.info(f"  Errors: {error_count}")

        if error_count == 0:
            logger.success("Sanity test COMPLETED with NO ERRORS.")
        else:
            logger.warning(f"Sanity test COMPLETED with {error_count} errors.")

        histmanager.close()
        logger.info("Manager closed.")

    except Exception as e:
        logger.critical(f"Critical failure during initialization OR execution: {e}")


if __name__ == '__main__':
    main()
