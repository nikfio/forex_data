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
                        is_empty_dataframe
                )

from loguru import logger

from sys import stderr


def main():
    
    # instance data manager                          
    histmanager = historical_manager(
                    ticker='NZDUSD',
                    config_file=APPCONFIG_FILE_YAML
    )
    
    # add logging to stderr 
    logger.add(stderr, level="TRACE")
    
    # # example dates 
    ex_start_date = '2015-10-03 10:00:00'
    ex_end_date   = '2015-12-03 10:00:00'
    
    # get data
    yeardata = histmanager.get_data(timeframe = '1h',
                                    start     = ex_start_date,
                                    end       = ex_end_date
    )
    
    if not is_empty_dataframe(yeardata):
        
        logger.trace(f"""
                     get_data: 
                     rows {yeardata.shape[0]}
                     start {yeardata[BASE_DATA_COLUMN_NAME.TIMESTAMP][0]}, 
                     end {yeardata[BASE_DATA_COLUMN_NAME.TIMESTAMP][yeardata.shape[0]-1]}"""
        )
        
    else:
        
        logger.trace("""
                     get_data: no data found, "
                     requested pair {histmanager.ticker}
                     start {ex_start_date}, "
                     end {ex_start_date}"""
        )
                                        
    # add new timeframe
    histmanager.add_timeframe('1W', update_data=True)
    
    # plot data 
    histmanager.plot( timeframe   = '1D',
                      start_date  = '2013-02-02 18:00:00',
                      end_date    = '2013-06-23 23:00:00'
    )
    
    
if __name__ == '__main__':
    main()