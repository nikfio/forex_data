# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 18:07:21 2022

@author: fiora
"""

# python base 
import time 
from requests import Session
import pandas as pd
from dotty_dict import dotty


# wrapper for multiple sources
import pandas_datareader as pd_r


# external 

# wrapper dedicated to alpha-vantage source
from alpha_vantage.foreignexchange import ForeignExchange as av_FX_reader


# internally defined  
import data_common_def as dc_def

# constants
READ_RETRY_COUNT = 2 
READ_PAUSE       = 1   
READ_CHUNKSIZE   = 96

PRIORITY_ACCESS_LIST = ['AV_WRAPPER', 'PD_READER']
PIORITY_SOURCE_LIST  = ['ALPHA_VANTAGE', 'YAHOO', 'POLYGON-IO']

MINIMAL_RECENT_TIME_WINDOW_DAYS = 3 

## Realtime data manager
#  source providers to test APIs: pandas-datareader, alpha-vantage
#  pandas-datareader offers higher level functions with possibility
#  to interact with a list of data providers


class RealTime_data_manager():
    
    def __init__(self, pair, data_access, data_source=None, api_key=None, 
                 timeframe=None, recent_time_window=MINIMAL_RECENT_TIME_WINDOW_DAYS):
        
        assert isinstance(pair, str), 'pair input must be str type'
        assert isinstance(api_key, str), 'api_key input must be str type'
        assert dc_def.check_AV_FX_symbol(pair), 'invalid pair symbol format'
        assert dc_def.check_timeframe_str(recent_time_window), 'invalid time window'
        
        if pd.to_offset(recent_time_window) < pd.DateOffset(days=MINIMAL_RECENT_TIME_WINDOW_DAYS):
            
            self._recent_time_window = pd.to_offset(recent_time_window)
            
        else:
            
            raise ValueError('time window value not accepted')
        
        self._pair          = pair.upper()
        
        self._from_symbol, self._to_symbol = dc_def.get_fxpair_components(pair)
        
        self._tf            = dc_def.check_timeframe_str(timeframe)
        
        self._data_access   = data_access 
        self._data_source   = data_source
        self._api_key       = api_key
        
        self._db_dotdict    = dotty()
        
        self._session       = Session()
        
        
        # pandas-datareader instances
        if self._data_access == dc_def.DATA_ACCESS.PANDAS_DATAREADER:
              
            
            if self._data_source == dc_def.REALTIME_DATA_SOURCE.ALPHA_VANTAGE:
                
                # real time forex data
                self._pdr_av_fx_reader = pd_r.av.forex.AVForexReader(symbols     = self._pair,
                                                                     retry_count = READ_RETRY_COUNT,
                                                                     pause       = READ_PAUSE,
                                                                     session     = self._session,
                                                                     api_key     = self._api_key)
                
                
                # generic real time quotes
                self._pdr_av_qt_reader = pd_r.av.quotes.AVQuotesReader(symbols     = self._pair,
                                                                       retry_count = READ_RETRY_COUNT,
                                                                       pause       = READ_PAUSE,
                                                                       session     = self._session,
                                                                       api_key     = self._api_key )
                
                
            elif self._data_source == dc_def.REALTIME_DATA_SOURCE.YAHOO_FINANCE:
                
                pass
                
            
        # alpha_vantage wrapper case
        elif self._data_access == dc_def.DATA_ACCESS.ALPHA_VANTAGE_WRAPPER:
            
            self._av_wrap_ts_reader = av_FX_reader(key   = self._api_key,
                                                   output_format= 'pandas',
                                                   indexing_type= 'date')
                                                   
                                                  
            
        
        
    
    def get_daily_data(self, timeframe=None):
        
        # pandas-datareader case
        if self._data_access == dc_def.DATA_ACCESS.PANDAS_DATAREADER:
            
            if self._data_source == dc_def.REALTIME_DATA_SOURCE.ALPHA_VANTAGE:
                
                pass
            
            
        # alpha_vantage wrapper case
        elif self._data_access == dc_def.DATA_ACCESS.ALPHA_VANTAGE_WRAPPER:
            
            pass
        
        return 
    
    def get_time_series_data(self, recent_time_window=None, start=None, end=None, timeframe=None):
        
        # start and end input have higher priority
        if start and end:
            
            start_date = start
            end_date   = end
            
        else:
            
            # read following recent time window input parameter
            assert dc_def.check_timeframe_str(recent_time_window), 'invalid time window'
            
            if pd.to_offset(recent_time_window) < pd.DateOffset(days=MINIMAL_RECENT_TIME_WINDOW_DAYS):
                
                self._recent_time_window = pd.to_offset(recent_time_window)
                
            else:
                
                raise ValueError('time window value not accepted')
            
            # create window start and end bounds
            end_date   = pd.Timestamp.now()
            start_date = end - self._recent_time_window
        
        # time series reader : to get complete ohlc data element
        
        # using PANDAS DATAREADER
        self._pdr_av_TS_reader = pd_r.av.time_series.AVTimeSeriesReader(symbols   = self._pair,
                                                                        function  = 'TIME_SERIES_INTRADAY_EXTENDED',
                                                                        start     = start_date,
                                                                        end       = end_date,
                                                                        pause     = READ_PAUSE,
                                                                        session   = self._session,
                                                                        chunksize = READ_CHUNKSIZE,
                                                                        api_key   = self._api_key )
                                
        
        window_raw_data = self._pdr_av_TS_reader.read()
        
        # using alpha vantage wrapper
        data, meta_data = self._av_wrap_ts_reader.get_currency_exchange_intraday(self._from_symbol,
                                                                                 self._to_symbol,
                                                                                 interval='1min',
                                                                                 outputsize='full')
        
        
        
        window_data = self._parse_time_series_data(window_raw_data)
        
        
        if dc_def.check_timeframe_str(timeframe):
            
            window_data_reframed = self._reframe_data(window_data, self._timeframe)
            
            return window_data_reframed
        
        else:
        
            return window_data
    
    def get_quote(self):
        
        # pandas-datareader case
        if self._data_access == dc_def.DATA_ACCESS.PANDAS_DATAREADER:
            
            if self._data_source == dc_def.REALTIME_DATA_SOURCE.ALPHA_VANTAGE:
                
                av_fx_data = self._pdr_av_fx_reader.read()
                
                av_qt_data = self._pdr_av_qt_reader.read()
        
        # alpha_vantage wrapper case
        elif self._data_access == dc_def.DATA_ACCESS.ALPHA_VANTAGE_WRAPPER:
    
            pass
        
        
        return av_fx_data, av_qt_data
    
    def pause(time):
        
        time.sleep(time)
        
        
    def _source_data_to_ohlc(raw_source_data):
            
        pass
    
    
    def _parse_time_series_data(raw_window_data):
        
        # parse raw data and format data as common defined 
        
        # timestamp column to datetime type and common format
        # dc_def.infer_raw_date_dt()
        
        # set timestamp column as df index
        
        # set columns names as 
        # dc_def.DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX
        
        # RETURN
        
        pass
    
    
    def _reframe_data(source_data, timeframe):
        
        # resample followinf timeframe input spec
        
        pass
    

    def dump_data_to_file(file_nrows):
        
        pass        
    
    