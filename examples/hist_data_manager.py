# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 22:03:27 2022

@author: fiora
"""

"""
Description: 
    test data_manager object historical data feature:
        
        1) download historical data and save to file in minimal timeframe
           available (TICK) to avoid further downloads at each run
           
        2) resample TICK data to to have any larger timeframe specified
            2.1) manage read/write of .csv files and .parquet files 
    
        3) plot ticker between date interval specified
    
"""

from forex_data import (
        historical_manager,
        APPCONFIG_FILE_YAML,
        BASE_DATA_COLUMN_NAME,
        is_empty_dataframe,
        shape_dataframe,
        get_dataframe_element
    )

from loguru import logger

from sys import stderr


def main():
    
    #TODO: look for config file and get reference
    
    # instance data manager                          
    histmanager = historical_manager(
                    config = 'C:/Projects/forex-data/appconfig'
    )
     
    # add logging to stderr 
    logger.add(stderr, level="TRACE")
    
    # example parameters
    ex_ticker     = 'EURJPY'
    ex_timeframe  = '1D'
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
                      start_date  = '2017-02-02 18:00:00',
                      end_date    = '2017-06-23 23:00:00'
    )
    
    
    ## get data from another ticker
    
    # example parameters
    ex_ticker     = 'EURCAD'
    ex_timeframe   = '3D'
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
        
        logger.trace(f"""
                     get_data:
                     ticker {ex_ticker}
                     rows {shape_dataframe(yeardata)[0]}
                     start {get_dataframe_element(yeardata,BASE_DATA_COLUMN_NAME.TIMESTAMP,0)}, 
                     end {get_dataframe_element(yeardata,BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                                shape_dataframe(yeardata)[0]-1)}"""
        )
    
    
if __name__ == '__main__':
    main()