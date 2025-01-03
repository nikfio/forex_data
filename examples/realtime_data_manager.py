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
                    realtime_manager,
                    APPCONFIG_FILE_YAML
                )

from sys import stderr


def main():
    
    # instance data manager
    realtimedata_manager = realtime_manager(
                            config_file = APPCONFIG_FILE_YAML
    )
    
    # add logging to stderr 
    logger.add(stderr, level="TRACE")
        
    # input test request definition
    test_day_start   = '2024-03-10'
    test_day_end     = '2024-03-26'
    test_n_days      = 10
    
    # get last close on daily basis
    dayclose_quote = \
        realtimedata_manager.get_daily_close(
                            ticker = 'EURUSD',
                            last_close=True
        )
    
    logger.trace(f'Real time daily close quote {dayclose_quote}')
    
    # test time window data function with daily resolution
    window_daily_ohlc = \
        realtimedata_manager.get_daily_close(
            ticker = 'EURUSD',
            recent_days_window=test_n_days
        )
    
    logger.trace(f'Last {test_n_days} window data: {window_daily_ohlc}')
                                                                       
    # test start-end window data function with daily resolution
    window_limits_daily_ohlc = \
        realtimedata_manager.get_daily_close(
            ticker = 'EURUSD',
            day_start=test_day_start,
            day_end=test_day_end
        )
     
    logger.trace(f'From {test_day_start} to {test_day_end} ' 
                 f'window data: {window_limits_daily_ohlc}')
    
    # test time window data function with timeframe resolution
    
    # input test request definition
    test_day_start   = '2024-04-10'
    test_day_end     = '2024-04-15'
    test_timeframe   = '1h'
    
    window_data_ohlc = \
        realtimedata_manager.get_data(  
            ticker    = 'EURUSD',    
            start     = test_day_start,
            end       = test_day_end,
            timeframe = test_timeframe
        )
    
    logger.trace(f'Real time {test_timeframe} window data: {window_data_ohlc}')
    
    # test time window data function with timeframe resolution: intraday case
    test_day_start = Timestamp.now() - Timedelta('10D')
    test_day_end   = Timestamp.now() - Timedelta('8D')
    test_timeframe = '5m'
    
    window_data_ohlc = \
        realtimedata_manager.get_data(  
            ticker    = 'EURUSD',
            start     = test_day_start,
            end       = test_day_end,
            timeframe = test_timeframe
        )
    
    logger.trace(f'Real time {test_timeframe} window data: {window_data_ohlc}')
    
    
if __name__ == '__main__':
    main()
