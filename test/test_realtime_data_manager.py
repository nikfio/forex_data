# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 22:13:04 2022

@author: fiora

test data_manager object realtime feature:
    
    1) test real time data providers
    
    2) test timeframe flexibility as in historical data manager
    
    3) download indicators values

"""

from pandas import (
                    Timestamp,
                    Timedelta
                    )
# custom lib
from forex_prediction import (
                              realtime_manager,
                              read_config_file
                          )


def main():
    
    realtimedata_manager = realtime_manager(
                            ticker = 'NZDUSD',
                            config_file = r'C:/Projects/forex_prediction_project/appconfig/config.yaml'
    )
    
    # input test request definition
    test_day_end     = '2022-03-26'
    test_day_start   = '2022-03-10'
    test_timeframe   = '1H'
    test_n_days      = 10
    
    ## test list tickers - listing function no expected results
    #av_tickers_list    = test_rt_data_manager.get_realtime_tickers_list(source='ALPHA_VANTAGE')
    #poly_tickers_list  = test_rt_data_manager.get_realtime_tickers_list(source='POLYGON-IO')
    
    # test current quote value function
    # required subscription
    # current_quote = test_rt_data_manager.get_realtime_quote() 
    # print(f'Realtime current quote: {current_quote}')
    '''
    # get last close on daily basis
    dayclose_quote = \
        realtimedata_manager.get_realtime_daily_close(last_close=True)
    
    print(f'Real time daily close quote {dayclose_quote}')
    
    # test time window data function with daily resolution
    window_daily_ohlc = \
        realtimedata_manager.get_realtime_daily_close(recent_days_window=test_n_days)
    
    print(f'Last {test_n_days} window data: {window_daily_ohlc}')
                                                                       
    # test start-end window data function with daily resolution
    window_limits_daily_ohlc = \
        realtimedata_manager.get_realtime_daily_close(day_start=test_day_start,
                                                      day_end=test_day_end)
     
    print(f'From {test_day_start} to {test_day_end} ' 
          f'window data: {window_limits_daily_ohlc}')
    '''
    # test time window data function with timeframe resolution
    window_data_ohlc = \
        realtimedata_manager.get_data(  start     = test_day_start,
                                        end       = test_day_end,
                                        timeframe = test_timeframe)
    
    print(f'Real time {test_timeframe} window data: {window_data_ohlc}')
    
    # test time window data function with timeframe resolution: intraday case
    test_day_start = Timestamp.now() - Timedelta('10h')
    test_day_end   = Timestamp.now() - Timedelta('1h')
    
    window_data_ohlc = \
        realtimedata_manager.get_data(  start     = test_day_start,
                                        end       = test_day_end,
                                        timeframe = test_timeframe)
    
    print(f'Real time {test_timeframe} window data: {window_data_ohlc}')
    
    
if __name__ == '__main__':
    main()
