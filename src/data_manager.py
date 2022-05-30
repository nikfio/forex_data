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



import lmdb

# custom lib
from histdatalib import HistDataManager
from realtimedatalib import RealTime_data_manager

# custom definitions
import data_common_def as dc_def

    
class db_parameters(NamedTuple):
    """
    
    pair   : forex currency exchange pair
    
    """
    
    mode                : dc_def.DB_MODE = dc_def.DB_MODE.FULL_MODE
    pair                : str  = None
    timeframe           : list = None 
    years               : list = None
    months              : list = None
    hist_data_path      : str  = dc_def.DEFAULT_PATHS.FOREX_LOCAL_HIST_DATA_PATH
    rt_data_path        : str  = dc_def.DEFAULT_PATHS.FOREX_LOCAL_REALTIME_DATA_PATH
    data_source         : list = None
    nrows_per_file      : int  = 100000
    add_real_time       : bool = False
    rt_data_access      : dc_def.DATA_ACCESS = dc_def.DATA_ACCESS.ALPHA_VANTAGE_WRAPPER
    rt_data_source      : dc_def.REALTIME_DATA_SOURCE = dc_def.REALTIME_DATA_SOURCE.ALPHA_VANTAGE
    api_key             : str  = ''
    
    
    
    
class db_manager:
    
    def __init__(self, parameters):
                 
        assert isinstance( parameters, db_parameters), \
                'Parameters must be a data_manager_parameters instance'
        
        # internal intialization
        self._parameters = parameters
        self._historical_data_enabled = False
        self._realtime_data_enabled   = False
        
        if parameters.mode == dc_def.DB_MODE.FULL_MODE \
           or parameters.mode == dc_def.DB_MODE.HISTORICAL_MODE:
               
            # historical data manager
            self._historical_mngr = HistDataManager(parameters.pair, 
                                                    parameters.hist_data_path,
                                                    parameters.years,
                                                    timeframe = parameters.timeframe)
                
            self._historical_data_enabled = True
            
        if parameters.mode == dc_def.DB_MODE.FULL_MODE \
           or parameters.mode == dc_def.DB_MODE.REALTIME_MODE:
               
            # realtime data manager
            self._realtime_mngr = RealTime_data_manager(pair        = parameters.pair,
                                                        timeframe   = parameters.timeframe,
                                                        data_access = parameters.rt_data_access,
                                                        data_source = parameters.rt_data_source,
                                                        api_key     = parameters.api_key)
        
            self._realtime_data_enabled = True
        
    def download_histdata(self, years):
             
        
        self._historical_mngr.download(years)
        

    def get_realtime_quote(self,):
        
        self._realtime_mngr
    
     
    
    def data_to_file(self, file_type='csv', filepath=None):
        
        if self._historical_mngr:
            
            pass
        
    
    def plot(self, data_source, timeframe=None, 
                   chart_mode=None, start_date=None, end_date=None):
        
        if data_source == 'Historical_data':
            
            self._historical_mngr.plot_data(tf         = timeframe,
                                            start_date = start_date,
                                            end_date   = end_date)


        elif data_source == 'Realtime_data':
            
            pass

