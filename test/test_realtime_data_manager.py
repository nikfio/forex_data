# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 22:13:04 2022

@author: fiora

test data_manager object realtime feature:
    
    1) test real time data providers
    
    2) test timeframe flexibility as in historical data manager
    
    3) download indicators values

"""

import pyaml

from absl import app


# custom lib
from forex_prediction import (
                              DB_MODE,
                              db_parameters,
                              db_manager
                              )


def main(argv):
    
    # realtime manager instantiation
    rt_param = db_parameters(mode            = DB_MODE.REALTIME_MODE,
                             pair            = 'EUR/USD',
                             timeframe       = '30min',
                             av_api_key      = av_key, 
                             poly_api_key    = poly_key )
    
    test_rt_data_manager = db_manager(rt_param)
    
    # input test request definition
    test_day_end     = '2022-03-26'
    test_day_start   = '2022-03-10'
    
    # test list tickers - listing function no expected results
    #av_tickers_list    = test_rt_data_manager.get_realtime_tickers_list(source='ALPHA_VANTAGE')
    #poly_tickers_list  = test_rt_data_manager.get_realtime_tickers_list(source='POLYGON-IO')
    
    # test current quote value function
    current_daily_ohlc = test_rt_data_manager.get_realtime_quote() 
    
    # test time window data function with daily resolution
    window_daily_ohlc = test_rt_data_manager.get_realtime_daily_close(recent_days_window=10)
                                                                       
    # test start-end window data function with daily resolution
    window_limits_daily_ohlc = test_rt_data_manager.get_realtime_daily_close(day_start=test_day_start,
                                                                             day_end=test_day_end)
     
    # test time window data function with timeframe resolution
    window_data_ohlc = test_rt_data_manager.get_realtime_window_data(time_window='1W',
                                                                     reframe=True)
    
    print(current_daily_ohlc)
    
    print(window_daily_ohlc)
    
    print(window_limits_daily_ohlc)
    
    print(window_data_ohlc)
    
if __name__ == '__main__':
    app.run(main)
