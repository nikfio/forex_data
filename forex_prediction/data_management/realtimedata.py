# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 18:07:21 2022

@author: fiora
"""

# python base 
from time import sleep 
from requests import Session
from dotty_dict import dotty
from io import StringIO
from typing import Union, Optional



# external 

# polygon-io source
from polygon import RESTClient

# alpha-vantage source
from alpha_vantage.foreignexchange import ForeignExchange as av_FX_reader
 
from .common import * 

# constants
READ_RETRY_COUNT = 2 
READ_PAUSE       = 1   
READ_CHUNKSIZE   = 96

MINIMAL_RECENT_TIME_WINDOW_DAYS = 3 

## Realtime data manager
#  source data providers to test APIs: polygon-IO, alpha-vantage

class RealTime_data_manager():
    
    def __init__(self, 
                 pair:str,   
                 av_api_key:str=None, 
                 poly_api_key:str=None, 
                 timeframe:list=None):
        
        self._to_symbol, self._from_symbol = get_pair_symbols(pair)
        
        # pair
        self._pair         = pair.upper()
        
        self._pair_polygon = to_source_symbol(self._pair, 
                                                  REALTIME_DATA_PROVIDER.ALPHA_VANTAGE)
        
        self._pair_alphavantage = to_source_symbol(self._pair, 
                                                  REALTIME_DATA_PROVIDER.POLYGON_IO)
        
        assert isinstance(timeframe, list)   \
                and all([check_timeframe_str(tf) 
                         for tf in timeframe]), \
                f'requested non compliant timeframe {timeframe}'
        
        self._tf_list  = timeframe
        
        # http session instance
        self._session       = Session()
        
        # keys for data providers
        self._av_api_key       = av_api_key
        self._poly_api_key     = poly_api_key
        
        # alpha vantage client
        self._av_reader = av_FX_reader( key   = self._av_api_key,
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
     
                 
    def get_daily_close(self,
                        last_close=False,
                        recent_days_window=None, 
                        day_start=None, 
                        day_end=None):
        
        
        if last_close:
        
            av_daily_data_resp = self._av_reader.get_currency_exchange_daily(self._to_symbol,
                                                                             self._from_symbol,
                                                                             outputsize='compact')
            
            # parse response and return
            return self._parse_av_daily_data(av_daily_data_resp,
                                             last_close=True,
                                             recent_days_window=recent_days_window, 
                                             day_start=day_start, 
                                             day_end=day_end)  
            
        else:
            
            if not day_start or not day_end:
                assert isinstance(recent_days_window, int), 'recent_days_window must be integer'
            
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
                             recent_days_window=None, 
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
            
            if isinstance(recent_days_window, int):
                # set window as DateOffset str with num and days
                days_window = '{days_num}d'.format(days_num=recent_days_window)
            else:
                days_window = None
                
            day_start, day_end = get_date_interval(interval_end_mode='now',
                                                   interval_timespan=days_window,
                                                   normalize=True,
                                                   bdays=True)
        
            # return data based on filter output
            window_data = daily_df[(daily_df.index >= day_start) \
                                   & (daily_df.index <= day_end)]
                
            window_data.as_type(DTYPE_DICT.TF_DTYPE)
            
            return window_data


    def _parse_time_window_data(raw_window_data,
                                data_provider):
        
        # parse raw data and format data as common defined 
        
        data_df = raw_window_data.copy()
        
        if data_provider == REALTIME_DATA_PROVIDER.POLYGON_IO:
            
            # keep base data columns
            extra_columns = DATA_COLUMN_NAMES.TF_DATA - data_df.columns
            data_df.drop(extra_columns, inplace=True)
            
            # set index as timestamp datetime64
            data_df.set_index(BASE_DATA_FEATURE_NAME.TIMESTAMP, \
                              inplace = True)
            
            data_df.index = any_date_to_datetime64(data_df.index)
            
            # conventional dtype
            data_df.astype(DTYPE_DICT.TF_DTYPE)
            
        elif data_provider == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:
            
            pass
        
        
        return data_df
    
    
    def get_time_window_data(self, 
                             time_window=None, 
                             start=None, 
                             end=None, 
                             timeframe=None):
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
         
        start_date, end_date = get_date_interval(start=start,
                                                 end=end,
                                                 interval_end_mode='now',
                                                 interval_timespan=time_window,
                                                 bdays=True,
                                                 normalize=False)
            
        data_df = pd.DataFrame()
        data_provider = ''
        
        # try to get data with alpha_vantage if available
        # alpha vantage provides intraday data with high resolution
        if pd.Timestamp(start_date).tz_localize('UTC')  \
            > pd.Timestamp.utcnow().normalize():
        
            # using alpha vantage wrapper
            # use alpha vantage intraday option if start date is later than last midnight
            data, meta_data = self._av_reader.get_currency_exchange_intraday(self._to_symbol,
                                                                             self._from_symbol,
                                                                             interval='1min',
                                                                             outputsize='full')
            
            # parse response
            data_df = pd.DataFrame(data)
            data_provider = REALTIME_DATA_PROVIDER.ALPHA_VANTAGE
        
        # if alpha vantage fails or it is not available 
        # --> polygon-ai is the backup provider
        if data_df.empty:
        
            # get dates as datetime dtype
            start_ts = pd.Timestamp(start_date).to_pydatetime()
            end_ts = pd.Timestamp(end_date).to_pydatetime()
                                          
            # using Polygon-io client
            poly_resp = self._poly_reader.get_aggs(ticker      = self._pair_poly_format, 
                                                   multiplier  = 1, 
                                                   timespan    = 'minute', 
                                                   from_       = start_ts,
                                                   to          = end_ts,
                                                   adjusted    = True,
                                                   sort        = 'asc' )
        
            # parse response
            data_df = pd.DataFrame(poly_resp)
            data_provider = REALTIME_DATA_PROVIDER.POLYGON_IO
        
        window_data = self._parse_time_window_data(data_df,
                                                   data_provider)
        
        if check_timeframe_str(timeframe):
        
            window_data_reframed = reframe_data(window_data, self._timeframe)
            
            return window_data_reframed
        
        else:
        
            return window_data
        
           
    def dump_data_to_file(file_nrows):
         
        pass        
    
    