# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 22:13:04 2022

@author: fiora
"""
# python base
import os
from absl import app

# custom lib
from src import (
                ALPHA_VANTAGE_KEY_ENV,
                POLY_IO_KEY_ENV,
                DB_MODE,
                REALTIME_DATA_SOURCE)

from data_manager import db_manager, db_parameters


test_day_end     = '2022-03-26'
test_day_start   = '2022-03-10'

def main(argv):
    
    
    av_key = os.getenv(ALPHA_VANTAGE_KEY_ENV)
    poly_key = os.getenv(POLY_IO_KEY_ENV)
    

    rt_param = db_parameters(mode            = DB_MODE.REALTIME_MODE,
                             pair            = 'EUR/USD',
                             timeframe       = '30min',
                             rt_data_source  = REALTIME_DATA_SOURCE.ALPHA_VANTAGE,
                             av_api_key      = av_key, 
                             poly_api_key    = poly_key )
    
    test_rt_data_manager = db_manager(rt_param)
    
    # test list tickers
    #av_tickers_list    = test_rt_data_manager.get_realtime_tickers_list(source='ALPHA_VANTAGE')
    #poly_tickers_list  = test_rt_data_manager.get_realtime_tickers_list(source='POLYGON-IO')
    
    # test current quote value function
    current_daily_ohlc      = test_rt_data_manager.get_realtime_quote() 
    
    # test time window data function with daily resolution
    window_daily_ohlc = test_rt_data_manager.get_realtime_daily_close(recent_days_window=10)
                                                                       
    # test start-end window data function with daily resolution
    window_limits_daily_ohlc = test_rt_data_manager.get_realtime_daily_close(day_start=test_day_start,
                                                                             day_end=test_day_end)
     
    # test time window data function with timeframe resolution
    window_data_ohlc   = test_rt_data_manager.get_realtime_window_data(recent_time_window='1W',
                                                                       reframe=True)
    
    
    print(current_daily_ohlc)
    
    print(window_daily_ohlc)
    
    print(window_limits_daily_ohlc)
    

    
if __name__ == '__main__':
    app.run(main)
