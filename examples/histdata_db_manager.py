# -*- coding: utf-8 -*-
"""
Created on Sun Jun 15 11:48:53 2025

@author: fiora
"""

from taos.sqlalchemy import TaosDialect
TaosDialect.supports_statement_cache = False 

from forex_data import (
        historical_manager_db,
        BASE_DATA_COLUMN_NAME,
        TICK_TIMEFRAME,
        is_empty_dataframe,
        shape_dataframe,
        get_dataframe_element
    )

from loguru import logger

from sys import stderr

def main():
    
    #TODO: look for config file and get reference
    
    # instance data manager                          
    histmanager = historical_manager_db(
        config = 'C:/Projects/forex-data/appconfig'
    )
     
    # add logging to stderr 
    logger.add(stderr, level="TRACE")
    
    # test auxiliary
    tickers = histmanager._get_ticker_list()
    
    if tickers:
        
        keys = histmanager._get_ticker_keys(ticker = tickers[0])
        
        TICK_keys = histmanager._get_ticker_keys(ticker = tickers[0],
                                                 timeframe = TICK_TIMEFRAME
        )
        
        years = histmanager._get_ticker_years_list(tickers[0],
                                                   timeframe = TICK_TIMEFRAME)
        
    # example parameters
    ex_ticker     = 'EURJPY'
    ex_timeframe  = '1d'
    ex_start_date = '2018-10-03 10:00:00'
    ex_end_date   = '2018-12-03 10:00:00'
    
    # get data
    yeardata = histmanager.get_data(
        ticker    = ex_ticker,
        timeframe = ex_timeframe,
        start     = ex_start_date,
        end       = ex_end_date
    )
    
    if not is_empty_dataframe(yeardata):
        
        logger.trace(f"""
                     get_data:
                     ticker {ex_ticker}
                     timeframe {ex_timeframe}
                     rows {shape_dataframe(yeardata)[0]}
                     start {get_dataframe_element(yeardata,BASE_DATA_COLUMN_NAME.TIMESTAMP,0)}, 
                     end {get_dataframe_element(yeardata,BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                                shape_dataframe(yeardata)[0]-1)}"""
        )
        
    else:
        
        logger.trace("""
                     get_data: no data found
                     requested pair {ex_ticker}
                     start {ex_start_date}, "
                     end {ex_start_date}"""
        )
        
        
    # add new timeframe
    histmanager.add_timeframe('1W', update_data=True)
    
    # plot data 
    histmanager.plot( ticker      = ex_ticker,
                      timeframe   = '1D',
                      start_date  = '2016-02-02 18:00:00',
                      end_date    = '2016-06-23 23:00:00'
    )
    
    
    ## get data from another ticker
    
    # example parameters
    ex_ticker     = 'EURUSD'
    ex_timeframe  = '3D'
    ex_start_date = '2018-10-03 10:00:00'
    ex_end_date   = '2020-12-03 10:00:00'
    
    # get data
    yeardata = histmanager.get_data(
        ticker    = ex_ticker,
        timeframe = ex_timeframe,
        start     = ex_start_date,
        end       = ex_end_date
    )
    
    if not is_empty_dataframe(yeardata):
        
        logger.trace(f"""
                     get_data:
                     ticker {ex_ticker}
                     timeframe {ex_timeframe}
                     rows {shape_dataframe(yeardata)[0]}
                     start {get_dataframe_element(yeardata,BASE_DATA_COLUMN_NAME.TIMESTAMP,0)}, 
                     end {get_dataframe_element(yeardata,BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                                shape_dataframe(yeardata)[0]-1)}"""
        )
        
    else:
        
        logger.trace("""
                     get_data: no data found
                     requested pair {ex_ticker}
                     start {ex_start_date}, "
                     end {ex_start_date}"""
        )
        
    
if __name__ == '__main__':
    main()
    