# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 23:54:21 2024

@author: fiora
"""

import unittest

from pandas import (
                    DataFrame as pandas_dataframe,
                    infer_freq
    )

from polars.dataframe import ( 
                    DataFrame as polars_dataframe
    )

from forex_data import (
                    BASE_DATA_COLUMN_NAME,
                    historical_manager
                )

# build configuration file
config_file_yaml = \
"""
---
# yaml document for data management configuration

TICKER: 'EURUSD'

DATA_FILETYPE: 'parquet'

ENGINE: 'polars'

"""

class TestHistData(unittest.TestCase):
    
    def test_with_config_file(self):
        
        # historical manager instantiation                            
        histmanager = historical_manager(
            config_file=config_file_yaml
        )
        
        self.assertEqual('EURUSD', 
                         histmanager.ticker,
                         'Ticker parameter assignment from config file is invalid')
        
        
        self.assertTrue(
            isinstance(histmanager._dataframe_type(), 
                       polars_dataframe()),
            'dataframe engine assignment from config file is invalid'
        )
        
        # TODO: should do assignment checks for every parameter?
        
    
    def test_instance_call_mod_from_config(self):
        
        # historical manager instantiation                            
        histmanager = historical_manager(
            ticker='USDJPY',
            engine='pandas',
            config_file=config_file_yaml
        )
    
        self.assertEqual('USDJPY', histmanager.ticker,
                         'Ticker parameter assignment from object instantiaton '
                         'is invalid'
        )
        
        self.assertTrue(
            isinstance(histmanager._dataframe_type(), 
                       pandas_dataframe()),
            'dataframe engine assignment from object instantiation '
            'is invalid'
        )
        
        # TODO: should do assignment checks for every parameter?
        
    
    def test_getdata_timeframe(self):
        
        # historical manager instantiation                            
        histmanager = historical_manager(
            config_file=config_file_yaml
        )

        # example dates 
        ex_start_date = '2008-10-03 10:00:00'
        ex_end_date   = '2008-12-03 10:00:00'
        
        # get data
        data = histmanager.get_data(timeframe = '1h',
                                    start     = ex_start_date,
                                    end       = ex_end_date
        )        
        
        self.assertTrue(
            isinstance(data, polars_dataframe()),
            'data output type is not equal to dataframe engine assigned type'
        )
        
        timestamp_col = data[BASE_DATA_COLUMN_NAME.TIMESTAMP]
        
        inferred_freq = infer_freq(timestamp_col)
        
        self.assertEqual('1h', inferred_freq)
        
        
    def test_data_sump(self):
        
        pass
    
    
    def test_engines(self):
        
        pass
        
        
if __name__ == '__main__':
    
    histtest = TestHistData()
    
    histtest.test_with_config_file()
    
    histtest.test_instance_call_mod_from_config()
        
        