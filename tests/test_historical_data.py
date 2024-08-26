# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 23:54:21 2024

@author: fiora
"""

import unittest

from datetime import timedelta

from pandas import (
                    DataFrame as pandas_dataframe
    )

from polars.dataframe import ( 
                    DataFrame as polars_dataframe
    )

from forex_data import (
                    data_management as data_mng,
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
        
        self.assertEqual('parquet', 
                         histmanager.data_filetype,
                         'Ticker parameter assignment from config file is invalid')
        
        self.assertTrue(
            isinstance(histmanager._dataframe_type([]), 
                       polars_dataframe),
            'dataframe engine assignment from config file is invalid'
        )
        
        # TODO: should do assignment checks for every parameter?
        
    
    def test_instance_call_mod_from_config(self):
        
        # historical manager instantiation 
        # test that if a paraemter is assigned in
        # clas constructor it ovverides
        # value assigned from config file if present                           
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
                       pandas_dataframe),
            'dataframe engine assignment from object instantiation '
            'is invalid'
        )
        
    
    def test_getdata_with_timeframe(self):
        
        # historical manager instantiation                            
        histmanager = historical_manager(
            config_file=config_file_yaml
        )

        # example dates, take weekdays of a single week
        # to avoid false positive check on timeframe
        ex_start_date = '2008-11-17 09:00:00'
        ex_end_date   = '2008-11-21 18:00:00'
        
        # get data
        data = histmanager.get_data(timeframe = '1h',
                                    start     = ex_start_date,
                                    end       = ex_end_date
        )        
        
        self.assertTrue(
            isinstance(data, polars_dataframe),
            'data output type is not equal to dataframe engine assigned type'
        )
        
        # TODO: after data query, cancel checks between stock
        #       closing days like weekends and canonical festivities
        check = (
                    data[data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP] 
                    - data[data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP].shift(1)
                ).drop_nulls().value_counts().to_dict()

        self.assertEqual(len(check[data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP]),
                         1,
                         msg=('timeframe check failed, found mutiple: '  
                             + f'{check[data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP]}')
        )
    
        self.assertEqual(check[data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP][0],
                         timedelta(hours=1),
                         msg=('timeframe request 1h check failed, found mutiple: '  
                             + f'{check[data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP]}')
        )
        
        
    def test_data_filetypes(self):
        
        pass
    
    
    def test_engines(self):
        
        pass
        
    
    def test_add_timeframe(self):
        
        pass
    
        
        