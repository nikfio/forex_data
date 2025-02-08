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

from polars import ( 
                    DataFrame as polars_dataframe,
                    LazyFrame as polars_lazyframe
    )

from forex_data import (
                    data_management as data_mng,
                    historical_manager,
                    get_dataframe_column,
                    is_empty_dataframe,
                    DATA_FILE_COLUMN_INDEX
                )

__all__ = [
        'TestHistData'
    ]

# build configuration file
config_file_yaml = \
"""
---
# yaml document for data management configuration

DATA_FILETYPE: 'parquet'

ENGINE: 'polars_lazy'

"""

class TestHistData(unittest.TestCase):
    
    def test_with_config_file(self):
        
        # historical manager instantiation                            
        histmanager = historical_manager(
            config_file=config_file_yaml
        )
        
        self.assertEqual('parquet', 
                         histmanager.data_filetype,
                         'data_filetype parameter assignment from'
                         ' config file is invalid'
        )
        
        self.assertTrue(
            isinstance(histmanager._dataframe_type([]), 
                       polars_lazyframe),
            'dataframe engine assignment from config file is invalid'
        )
        
        # TODO: should do assignment checks for every parameter?
        
    
    def test_instance_call_mod_from_config(self):
        
        # historical manager instantiation 
        # test that if a paraemter is assigned in
        # clas constructor it ovverides
        # value assigned from config file if present                           
        histmanager = historical_manager(
            engine='pandas',
            config_file=config_file_yaml
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
        ex_ticker     = 'EURCAD'
        ex_timefame   = '3D'
        ex_start_date = '2008-11-17 09:00:00'
        ex_end_date   = '2008-11-30 18:00:00'
        
        # get data
        data = histmanager.get_data(
                    ticker    = ex_ticker,
                    timeframe = ex_timefame,
                    start     = ex_start_date,
                    end       = ex_end_date
        )        
        
        self.assertTrue(
            isinstance(data, polars_lazyframe),
            'data output type is not equal to dataframe engine assigned type'
        )
        
        self.assertFalse(
            is_empty_dataframe(data),
            'data output is empty'
        )
        
        # TODO: after data query, cancel checks between stock
        #       closing days like weekends and canonical festivities
        check =(
                    (
                        get_dataframe_column(data,
                                             data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP
                        )
                        - 
                        get_dataframe_column(data,
                                             data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP
                        ).shift(1)
                    ).drop_nulls().to_series(DATA_FILE_COLUMN_INDEX.TIMESTAMP)
                    .value_counts()
                    .to_dict()
                )

        self.assertEqual(len(check[data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP]),
                         1,
                         msg=('timeframe check failed, found mutiple: '  
                             + f'{check[data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP]}')
        )
    
        self.assertEqual(check[data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP][0],
                         timedelta(days=3),
                         msg=('timeframe request 1h check failed, found multiple: '  
                             + f'{check[data_mng.BASE_DATA_COLUMN_NAME.TIMESTAMP]}')
        )
        
        
    def test_data_filetypes(self):
        
        pass
    
    
    def test_engines(self):
        
        pass
        
    
    def test_add_timeframe(self):
        
        pass
    
        
        