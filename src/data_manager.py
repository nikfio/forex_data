# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 16:18:32 2022

@author: fiora
"""

"""
Description:
  
    
"""

from typing import NamedTuple
import pandas_datareader as pdr

import histdatalib as hd

import lmdb

class DEFAULT_PATHS:
    
    FOREX_LOCAL_HIST_DATA_PATH     = "C:/Database/Forex/Historical"
    FOREX_LOCAL_REALTIME_DATA_PATH = "C:/Database/Forex/RealTime"
    
class db_parameters(NamedTuple):
    """
        pair : exchange currency pair
    """
    
    pair                : str  = None
    timescale           : list = None 
    years               : list = None
    months              : list = None
    hist_data_path      : str  = DEFAULT_PATHS.FOREX_LOCAL_HIST_DATA_PATH
    rt_data_path        : str  = DEFAULT_PATHS.FOREX_LOCAL_REALTIME_DATA_PATH
    data_source         : list = None
    nrows_per_file      : int  = 100000
    add_real_time       : bool = False
    search_local_folder : bool = False
    
    
class db_manager:
    
    def __init__(self, parameters=None):
                 
        assert isinstance( parameters, db_parameters), \
                'Parameters must be a data_manager_parameters instance'
        
        
        self.parameters = parameters
        # historical data manager
        self._historical_mngr = hd.HistDataManager(parameters.pair, 
                                                   parameters.hist_data_path,
                                                   parameters.years,
                                                   timeframe = parameters.timescale)
                
        # realtime data manager
        
        
        
    def download_histdata(self, years):
             
        
        self._historical_mngr.download(years)
        

    def get_realtime_quote(self):
        
        pass
    
    
    def get_last_candle(self, retain_data=False):
        
        pass
    
    
    def data_to_file(self, file_type='csv', filepath=None):
        
        if self._historical_mngr:
            self._historical_mngr.data_to_file()
        
    
    def search_local_folder(self):
        
        pass


