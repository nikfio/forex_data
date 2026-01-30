# -*- coding: utf-8 -*-
"""
Created on Sun Dec 29 19:56:00 2025

@author: fiora

Example demonstrating conditional filtering with get_data method.
This shows how to use column_name, check_level, and condition parameters
to filter data based on specific column values.
"""

from forex_data import (
    HistoricalManagerDB,
    BASE_DATA_COLUMN_NAME,
    SQL_COMPARISON_OPERATORS,
    SQL_CONDITION_AGGREGATION_MODES,
    is_empty_dataframe,
    shape_dataframe,
)

from loguru import logger
from time import time


# Use a runtime defined config yaml file
test_config_yaml = '''
DATA_FILETYPE: 'parquet'

ENGINE: 'polars_lazy'
'''


def main():

    # Instance data manager
    histmanager = HistoricalManagerDB(
        config=test_config_yaml
    )

    # Example 1: Get data with a single condition (open price < threshold)
    ex_ticker = 'EURUSD'
    ex_timeframe = '1D'
    ex_start_date = '2018-01-01'
    ex_end_date = '2018-12-31'
    min_open_value = 1.13

    start_time = time()

    # Get data with condition: open < 1.05
    filtered_data = histmanager.get_data(
        ticker=ex_ticker,
        timeframe=ex_timeframe,
        start=ex_start_date,
        end=ex_end_date,
        comparison_column_name=BASE_DATA_COLUMN_NAME.OPEN,
        check_level=min_open_value,
        comparison_operator=SQL_COMPARISON_OPERATORS.LESS_THAN
    )

    if not is_empty_dataframe(filtered_data):
        logger.success(
            f"{ex_ticker}-{ex_timeframe} from {ex_start_date} to {ex_end_date}: "
            f"Found {shape_dataframe(filtered_data)[0]} entries where "
            f"OPEN < {min_open_value}"
        )
    else:
        logger.warning(
            f"{ex_ticker}-{ex_timeframe} from {ex_start_date} to {ex_end_date}: "
            f"No data found matching condition: OPEN < {min_open_value}"
        )

    logger.info(f"Elapsed time: {time() - start_time:.4f}s\n")

    # Example 2: Get data with multiple conditions
    ex_start_date = '2019-01-01'
    ex_end_date = '2019-12-31'
    high_threshold = 1.145
    low_threshold = 1.12
    condition_aggregation_mode = SQL_CONDITION_AGGREGATION_MODES.OR

    start_time = time()

    # Get data with multiple conditions
    filtered_data = histmanager.get_data(
        ticker=ex_ticker,
        timeframe=ex_timeframe,
        start=ex_start_date,
        end=ex_end_date,
        comparison_column_name=[BASE_DATA_COLUMN_NAME.HIGH, BASE_DATA_COLUMN_NAME.LOW],
        check_level=[high_threshold, low_threshold],
        comparison_operator=[
            SQL_COMPARISON_OPERATORS.GREATER_THAN,
            SQL_COMPARISON_OPERATORS.LESS_THAN
        ],
        aggregation_mode=condition_aggregation_mode
    )

    if not is_empty_dataframe(filtered_data):
        logger.success(
            f"{ex_ticker}-{ex_timeframe} from {ex_start_date} to {ex_end_date}: "
            f"Found {shape_dataframe(filtered_data)[0]} candles where "
            f"HIGH > {high_threshold} {condition_aggregation_mode} "
            f"LOW < {low_threshold}"
        )
    else:
        logger.warning(
            f"{ex_ticker}-{ex_timeframe} from {ex_start_date} to {ex_end_date}: "
            f"No data found matching conditions: "
            f"HIGH > {high_threshold} {condition_aggregation_mode} "
            f"LOW < {low_threshold}"
        )

    logger.info(f"Elapsed time: {time() - start_time:.4f}s\n")

    # Example 3: Compare with non-filtered data
    ex_start_date = '2020-06-01'
    ex_end_date = '2020-06-30'
    close_threshold = 1.12

    start_time = time()

    # Get all data (no filter)
    all_data = histmanager.get_data(
        ticker=ex_ticker,
        timeframe=ex_timeframe,
        start=ex_start_date,
        end=ex_end_date
    )

    # Get filtered data (close >= threshold)
    filtered_data = histmanager.get_data(
        ticker=ex_ticker,
        timeframe=ex_timeframe,
        start=ex_start_date,
        end=ex_end_date,
        comparison_column_name=BASE_DATA_COLUMN_NAME.CLOSE,
        check_level=close_threshold,
        comparison_operator=SQL_COMPARISON_OPERATORS.GREATER_THAN_OR_EQUAL
    )

    if not is_empty_dataframe(all_data):
        total_candles = shape_dataframe(all_data)[0]
        filtered_candles = shape_dataframe(filtered_data)[0]

        logger.success(
            f"{ex_ticker}-{ex_timeframe} from {ex_start_date} to {ex_end_date}: "
            f"Total candles: {total_candles}"
        )
        logger.success(
            f"{ex_ticker}-{ex_timeframe} from {ex_start_date} to {ex_end_date}: "
            f"Candles with CLOSE >= {close_threshold}: {filtered_candles}"
        )
        logger.success(
            f"{ex_ticker}-{ex_timeframe} from {ex_start_date} to {ex_end_date}: "
            f"Percentage: {(filtered_candles / total_candles * 100):.2f}%"
        )
    else:
        logger.warning(
            f"{ex_ticker}-{ex_timeframe} from {ex_start_date} to {ex_end_date}: "
            f"No data found for the specified period"
        )

    logger.info(f"Elapsed time: {time() - start_time:.4f}s\n")

    # close data manager
    histmanager.close()


if __name__ == '__main__':
    main()
