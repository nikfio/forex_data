# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 18:07:21 2022

@author: fiora
"""

# python base 
from time import sleep 
from requests import Session
import pandas as pd
from dotty_dict import dotty
from io import StringIO
from typing import Union, Optional



# external 

# polygon-io source
from polygon import RESTClient

# alpha-vantage source
from alpha_vantage.foreignexchange import ForeignExchange as av_FX_reader
 
from common import * 

# constants
READ_RETRY_COUNT = 2 
READ_PAUSE       = 1   
READ_CHUNKSIZE   = 96

MINIMAL_RECENT_TIME_WINDOW_DAYS = 3 

## Realtime data manager
#  source data providers to test APIs: polygon-IO, alpha-vantage

class RealTime_data_manager():
    
    def __init__(self, pair:str,   
                 av_api_key:str=None, poly_api_key:str=None, timeframe=None):
        
        assert check_AV_FX_symbol(pair), 'invalid pair symbol format'
        
        # check if pair is in market 'Forex' tickers available by polygon provider
        # and alpha vantage provider
        # if not return failure for invalid ticker requested
        
        self._pair          = pair.upper()
        
        self._from_symbol, self._to_symbol = get_fxpair_components(pair)
        
        self._tf            = check_timeframe_str(timeframe)
        
        self._db_dotdict    = dotty()
        
        self._session       = Session()
        
        # keys for data providers
        self._av_api_key       = av_api_key
        self._poly_api_key     = poly_api_key
        
        # alpha vantage client
        self._av_reader = av_FX_reader(key   = self._av_api_key,
                                               output_format= 'pandas',
                                               indexing_type= 'date')
        
        # Polygon-io client 
        self._poly_reader = RESTClient(api_key=self._poly_api_key)          
    

    def tickers_list(self, 
                     data_source, 
                     asset_class : Optional[ASSET_TYPE] = None):
         
        # return list of symbols for tickers actively treated by data providers
        
        tickers_list = list()
        
        if data_source == REALTIME_DATA_SOURCE.ALPHA_VANTAGE:
        
            # compose URL for tickers listing request
            # decode content
            with self._session as s:
                listing_downloaded = s.get( AV_LIST_URL.format(api_key=self._av_api_key))
                decoded_content = listing_downloaded.content.decode('utf-8')
                tickers_df = pd.read_csv(StringIO(decoded_content), sep=',', header=0)
                
            if asset_class:
                
                if asset_class == ASSET_TYPE.FOREX:
                    
                    raise ValueError('alpha vantage listing not including forex tickers') 
                    
                elif asset_class == ASSET_TYPE.ETF:
                    
                    assetType_req_index = tickers_df[:, 'assetType'] == 'ETF'
                    
                elif asset_class == ASSET_TYPE.STOCK:
                    
                    assetType_req_index = tickers_df[:, 'assetType'] == 'Stock'
            
            
                tickers_list = tickers_df.loc[assetType_req_index, 'symbol'].to_list()
                
            else:
                
                tickers_list = tickers_df.loc[:, 'symbol'].to_list()
                
        elif data_source == REALTIME_DATA_SOURCE.POLYGON_IO:
            
            if asset_class:
                
                if asset_class == ASSET_TYPE.FOREX:
                    
                    poly_asset_class = 'fx'
                    
            else:
                
                poly_asset_class = None
                    
            # call function for forex asset_class
            listing_downloaded = self._poly_reader.get_exchanges(asset_class = poly_asset_class)
                                                                 
            
            
                    
            tickers_list = [item.acronym for item in listing_downloaded]
            
        return tickers_list
     
        
    def pause(time):
        
        # pause execution
        sleep(time)
        
        
    def get_timestamp_window_bounds(freq, ):
        
        pass
        
        
    def _parse_time_series_data(raw_window_data):
        
        # parse raw data and format data as common defined 
        
        # timestamp column to datetime type and common format
        # infer_raw_date_dt()
        
        # set timestamp column as df index
        # df.set_index()
        
        # set columns names as 
        # DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX
        
        # RETURN
        
        pass
    
    
    def _reframe_data(source_data, timeframe):
        
        # resample following timeframe input spec
        
        pass
    
        
    def get_day_close(self,
                      last_close=False,
                      recent_days_window=None, 
                      day_start=None, 
                      day_end=None):
        
        if last_close:
        
            av_daily_data_resp = self._av_reader.get_currency_exchange_daily(self._from_symbol,
                                                                             self._to_symbol,
                                                                             outputsize='compact')
            
            # parse response and return
            return self._parse_av_daily_data(av_daily_data_resp,
                                             last_close=True,
                                             recent_days_window=recent_days_window, 
                                             day_start=day_start, 
                                             day_end=day_end)  
            
        else:
            
            av_daily_data_resp = self._av_reader.get_currency_exchange_daily(self._from_symbol,
                                                                             self._to_symbol,
                                                                             outputsize='full')
            
            # parse response and return
            return self._parse_av_daily_data(av_daily_data_resp,
                                             last_close=False,
                                             recent_days_window=recent_days_window, 
                                             day_start=day_start, 
                                             day_end=day_end) 
            
        
    def _parse_av_daily_data(self, 
                             daily_data,
                             last_close=False,
                             recent_days_window=10, 
                             day_start=None, 
                             day_end=None):
    
        # TODO: post download redundancy check using dict info
        resp_info_dict = daily_data[CANONICAL_INDEX.AV_DICT_INFO_INDEX]
        
        daily_df = daily_data[CANONICAL_INDEX.AV_DF_DATA_INDEX]
        # assign canonical column names
        daily_df.columns = DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX
        
        # set timestamp index as datetime64 type
        if not pd.api.types.is_datetime64_any_dtype(daily_df.index):
            daily_df.index = infer_date_dt(daily_df.index)
        
        daily_df.index.name = BASE_DATA_FEATURE_NAME.TIMESTAMP
        
        if last_close:
            
            # timestamp as column to include it in return data
            daily_df.reset_index(inplace=True)
            
            # get most recent line --> lowest num index
            return daily_df.iloc[CANONICAL_INDEX.LATEST_DATA_INDEX].to_dict()
        
        else:
            
            # start and end input have higher priority
            if not day_start or not day_end:
                
                assert isinstance(recent_days_window, int) \
                       and recent_days_window > 1, 'recent days input param must be integer type > 1'
                     
                # create window start and end bounds
                day_end   = pd.Timestamp.now(tz='UTC')
                day_start = day_end - pd.Timedelta(recent_days_window + 1, 'd')
                
            else:
                
                day_start = infer_date_dt(day_start)
                day_end = infer_date_dt(day_end)
                
            day_start = day_start.normalize()    
                
            # set datetime64 type
            if not pd.api.types.is_datetime64_any_dtype(day_start):
                day_start.to_pydatetime()
                
            if isinstance(day_start, pd.Timestamp):
                day_start = day_start.to_datetime64()
                    
            # set datetime64 type
            if not pd.api.types.is_datetime64_dtype(day_end):
                day_end = infer_date_dt(day_end)
                
            if isinstance(day_end, pd.Timestamp):
                day_end = day_end.to_datetime64()
        
            # return data based on filter output
            window_data = daily_df[(daily_df.index >= day_start) \
                                   & (daily_df.index <= day_end)]
            
            return window_data

        
    def get_time_window_data(self, recent_time_window=None, start=None, end=None, timeframe=None):
        """
         
        
        Parameters
        ----------
        recent_time_window : TYPE, string, template '<value><unit>'
        DESCRIPTION. The default is None. Follows pandas timedelta unit specification
        https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html
        start : TYPE, optional
        DESCRIPTION. The default is None.
        end : TYPE, optional
        DESCRIPTION. The default is None.
        timeframe : TYPE, optional
        DESCRIPTION. The default is None.
           
        Raises
        ------
        ValueError
        DESCRIPTION.
           
        Returns
        -------
        TYPE
        DESCRIPTION.
           
        """
         
        now_date = pd.Timestamp.now(tz='UTC')
        
        # start and end input have higher priority
        if start and end:
        
            start_date = start
            end_date   = end
        
        else:
        
            # get time window as a pandas timedelta object
            timedelta_window = timewindow_str_to_timedelta(recent_time_window)
            
            # create window start and end bounds
            end_date   = now_date
            start_date = end_date - timedelta_window
            
            # time series reader : to get complete ohlc data 
            
            # check if start date is older than last midnight
            curr_midnight_date = pd.Timestamp.normalize(now_date)
            
        data_df = pd.DataFrame()
        
        # try to get data with alpha_vantage if available
        if start_date > curr_midnight_date:
        
            # using alpha vantage wrapper
            # use alpha vantage intraday option if start date is later than last midnight
            data, meta_data = self._av_reader.get_currency_exchange_intraday(self._from_symbol,
                                                                             self._to_symbol,
                                                                             interval='1min',
                                                                             outputsize='full')
            
        # parse response
        
        data_df = pd.DataFrame(data)
        
        # if alpha vantage fails or it is not available 
        # --> polygon-ai is the backup provider
        if data_df.empty:
        
            # using Polygon-io client
            poly_resp = self._poly_reader.get_aggs(ticker      = self._pair, 
                                                   multiplier  = 1, 
                                                   timespan    = '1min', 
                                                   from_       = start_date,
                                                   to          = end_date)
        
        # parse response
        
        data_df = pd.DataFrame(poly_resp.results)
        
        
        window_data = self._parse_time_series_data(data)
        
        if check_timeframe_str(timeframe):
        
            window_data_reframed = self._reframe_data(window_data, self._timeframe)
            
            return window_data_reframed
        
        else:
        
            return window_data
        
           
    def dump_data_to_file(file_nrows):
         
        pass        
    
    