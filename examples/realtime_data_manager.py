# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 22:13:04 2022

@author: fiora

test data_manager object realtime feature:

    1) test real time data providers

    2) test timeframe flexibility as in historical data manager

    3) download indicators values

"""

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


def main():

    # instance data manager
    realtimedata_manager = RealtimeManager(
        config=test_config_yaml
    )

    # example parameters
    ex_ticker = 'EURCAD'

    # get last close on daily basis
    dayclose_quote = \
        realtimedata_manager.get_daily_close(
            ticker=ex_ticker,
            last_close=True
        )

    if not is_empty_dataframe(dayclose_quote):

        logger.trace(f"""
                     get_daily_close:
                     ticker {ex_ticker}
                     rows {shape_dataframe(dayclose_quote)[0]}
                     date {get_dataframe_element(
            dayclose_quote,
            BASE_DATA_COLUMN_NAME.TIMESTAMP, 0)}"""
        )

    else:

        logger.trace(f"""
                     get_daily_close: no data found
                     requested pair {ex_ticker}
                     last_close = True"""
                     )

    # example parameters
    ex_n_days = 13

    # test time window data function with daily resolution
    window_daily_ohlc = \
        realtimedata_manager.get_daily_close(
            ticker=ex_ticker,
            recent_days_window=ex_n_days
        )

    if not is_empty_dataframe(window_daily_ohlc):

        logger.trace(f"""
                     get_daily_close:
                     ticker {ex_ticker}
                     rows {shape_dataframe(window_daily_ohlc)[0]}
                     date {get_dataframe_element(
            window_daily_ohlc,
            BASE_DATA_COLUMN_NAME.TIMESTAMP, 0)}"""
        )

    else:

        logger.trace(f"""
                     get_daily_close: no data found
                     requested pair {ex_ticker}
                     n_days = {ex_n_days}"""
                     )

    # test start-end window data function with daily resolution

    # example parameters
    ex_start_date = '2025-01-15'
    ex_end_date = '2025-01-23'

    window_limits_daily_ohlc = \
        realtimedata_manager.get_daily_close(
            ticker=ex_ticker,
            day_start=ex_start_date,
            day_end=ex_end_date
        )

    if not is_empty_dataframe(window_limits_daily_ohlc):

        logger.trace(f"""
                     get_daily_close:
                     ticker {ex_ticker}
                     rows {shape_dataframe(window_limits_daily_ohlc)[0]}
                     date {get_dataframe_element(
            window_limits_daily_ohlc,
            BASE_DATA_COLUMN_NAME.TIMESTAMP, 0)}"""
        )

    else:

        logger.trace(f"""
                     get_daily_close: no data found
                     requested pair {ex_ticker}
                     start {ex_start_date}
                     end {ex_start_date}"""
                     )

    # test time window data function with timeframe resolution

    # test parameters
    ex_start_date = '2024-04-10'
    ex_end_date = '2024-04-15'
    ex_timeframe = '1h'

    window_data_ohlc = \
        realtimedata_manager.get_data(
            ticker=ex_ticker,
            start=ex_start_date,
            end=ex_end_date,
            timeframe=ex_timeframe
        )

    if not is_empty_dataframe(window_data_ohlc):

        logger.trace(f"""
                     get_data:
                     ticker {ex_ticker}
                     timeframe {ex_timeframe}
                     rows {shape_dataframe(window_data_ohlc)[0]}
                     start {get_dataframe_element(
            window_data_ohlc,
            BASE_DATA_COLUMN_NAME.TIMESTAMP, 0)},
                     end {get_dataframe_element(
                window_data_ohlc,
                BASE_DATA_COLUMN_NAME.TIMESTAMP,
                shape_dataframe(window_data_ohlc)[0] - 1)}"""
        )

    else:

        logger.trace(f"""
                     get_data: no data found, "
                     requested pair {ex_ticker}
                     start {ex_start_date}
                     end {ex_start_date}"""
                     )

    # test time window data function with timeframe resolution: intraday case

    # example parameters
    ex_start_date = Timestamp.now() - Timedelta('10D')
    ex_end_date = Timestamp.now() - Timedelta('8D')
    ex_timeframe = '5m'

    window_data_ohlc = \
        realtimedata_manager.get_data(
            ticker='EURUSD',
            start=ex_start_date,
            end=ex_end_date,
            timeframe=ex_timeframe
        )

    if not is_empty_dataframe(window_data_ohlc):

        logger.trace(f"""
                     get_daily_close:
                     ticker {ex_ticker}
                     timeframe {ex_timeframe}
                     rows {shape_dataframe(window_data_ohlc)[0]}
                     start {get_dataframe_element(
            window_data_ohlc,
            BASE_DATA_COLUMN_NAME.TIMESTAMP, 0)},
                     end {get_dataframe_element(
                window_data_ohlc,
                BASE_DATA_COLUMN_NAME.TIMESTAMP,
                shape_dataframe(window_data_ohlc)[0] - 1)}"""
        )

    else:

        logger.trace(f"""
                     get_daily_close: no data found
                     requested pair {ex_ticker}
                     start {ex_start_date}
                     end {ex_start_date}"""
                     )


if __name__ == '__main__':
    main()
