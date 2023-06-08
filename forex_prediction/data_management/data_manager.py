# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 16:18:32 2022

@author: fiora
"""

"""
Description:
  
    
"""

__all__ = ['db_parameters', 
           'db_manager']

from typing import NamedTuple

# custom lib
from .historicaldata import HistDataManager
from .realtimedata import RealTime_data_manager

# custom definitions
from .common import * 
    

class db_parameters(NamedTuple):
    """
    
    pair   : forex currency exchange pair
    
    """
    
    mode                : DB_MODE = None
    pair                : str  = None
    timeframe           : list = None 
    years               : list = None
    hist_data_path      : str  = DEFAULT_PATHS.FOREX_LOCAL_HIST_DATA_PATH
    rt_data_path        : str  = DEFAULT_PATHS.FOREX_LOCAL_REALTIME_DATA_PATH
    data_source         : list = None
    add_real_time       : bool = False
    av_api_key          : str  = ''
    poly_api_key        : str  = ''
    
    
    

class db_manager:
    
    def __init__(self, parameters):
                 
        assert isinstance( parameters, db_parameters), \
                'Parameters must be a data_manager_parameters instance'
        
        # internal initialization
        self._parameters = parameters
        self._historical_data_enabled = False
        self._realtime_data_enabled   = False
        
        if parameters.mode == DB_MODE.FULL_MODE \
           or parameters.mode == DB_MODE.HISTORICAL_MODE:
               
            # historical data manager
            self._historical_mngr = HistDataManager(parameters.pair, 
                                                    parameters.hist_data_path,
                                                    parameters.years,
                                                    timeframe = parameters.timeframe)
                
            self._historical_data_enabled = True
           
        if parameters.mode == DB_MODE.FULL_MODE \
           or parameters.mode == DB_MODE.REALTIME_MODE:
               
            # realtime data manager
            self._realtime_mngr = RealTime_data_manager(pair           = parameters.pair,
                                                        timeframe      = parameters.timeframe,
                                                        av_api_key     = parameters.av_api_key,
                                                        poly_api_key   = parameters.poly_api_key)
        
            self._realtime_data_enabled = True
        
        
    def add_historical_data(self, years):
        
        self._historical_mngr.download(years = years, 
                                       search_local = True)
        
    def add_timeframe(self, timeframe, update_data=True):
        
        if self._historical_data_enabled \
            and hasattr(self, '_historical_mngr'):
                
            self._historical_mngr.add_timeframe(timeframe,
                                                update_data=update_data)
            
        
    def get_realtime_tickers_list(self, source, asset_class=None):
        
        return self._realtime_mngr.tickers_list(source, asset_class = asset_class)
    
    
    def get_realtime_quote(self):
        
        return self._realtime_mngr.get_daily_close(last_close=True)
    
    
    def get_realtime_daily_close(self,
                                 recent_days_window=None, 
                                 day_start=None, 
                                 day_end=None):
        
        return self._realtime_mngr.get_daily_close(last_close=False,
                                                   recent_days_window=recent_days_window,
                                                   day_start=day_start,
                                                   day_end=day_end)
    

    def get_realtime_window_data(self,
                                 time_window=None,
                                 start=None,
                                 end=None,
                                 reframe=False,
                                 timeframe=None):
        
        if reframe:
            
            self._realtime_mngr.get_time_window_data(time_window        = time_window,
                                                     start              = start,
                                                     end                = end,
                                                     timeframe          = timeframe)
                                 
        else:
            
            self._realtime_mngr.get_time_window_data(time_window        = time_window,
                                                     start              = start,
                                                     end                = end)
        
        
    def plot(self, 
             data_source,
             timeframe  = None,
             start_date = None,
             end_date   = None):
        
        
        if data_source == DB_MODE.HISTORICAL_MODE:
            
            self._historical_mngr.plot_data(timeframe  = timeframe,
                                            start_date = start_date,
                                            end_date   = end_date)


        elif data_source == DB_MODE.REALTIME_MODE:
            
            pass

