# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 16:18:32 2022

@author: fiora
"""

"""
Description:
  
    
"""

__all__ = ['data_manager']

import logging

from attrs import ( 
                    define,
                    field,
                    validators
                )

from .historicaldata import historical_manager
from .realtimedata import realtime_manager

# custom definitions
from .common import * 
    
    
@define
class data_manager:
    
    pass
    # # def __attrs_post_init(self):
                 
    # #     pass
        
        
    # def add_historical_data(self, years):
        
    #     self._historical_mngr.download(years = years, 
    #                                    search_local = True)
        
        
    # # def add_timeframe(self, timeframe, update_data=True):
        
    # #     if self._historical_data_enabled \
    # #         and hasattr(self, '_historical_mngr'):
                
    # #         self._historical_mngr.add_timeframe(timeframe,
    # #                                             update_data=update_data)
            
        
    # def get_realtime_tickers_list(self, source, asset_class=None):
        
    #     return self._realtime_mngr.tickers_list(source, asset_class = asset_class)
    
    
    # def get_realtime_quote(self):
        
    #     return self._realtime_mngr.get_realtime_quote()
    
    
    # def get_realtime_daily_close(self,
    #                              recent_days_window=None, 
    #                              day_start=None, 
    #                              day_end=None,
    #                              last_close=False):
        
        
    #     return self._realtime_mngr.get_daily_close(last_close=last_close,
    #                                                recent_days_window=recent_days_window,
    #                                                day_start=day_start,
    #                                                day_end=day_end)
    

    # def get_realtime_window_data(self,
    #                              start=None,
    #                              end=None,
    #                              timeframe=None):
        
    #     return self._realtime_mngr.get_time_window_data( start     = start,
    #                                                      end       = end,
    #                                                      timeframe = timeframe)
        
        
    # def plot(self, 
    #          data_source,
    #          timeframe  = None,
    #          start_date = None,
    #          end_date   = None):
        
        
    #     if data_source == DB_MODE.HISTORICAL_MODE:
            
    #         self._historical_mngr.plot_data(timeframe  = timeframe,
    #                                         start_date = start_date,
    #                                         end_date   = end_date)


    #     elif data_source == DB_MODE.REALTIME_MODE:
            
    #         pass

