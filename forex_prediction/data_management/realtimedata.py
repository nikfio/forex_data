# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 18:07:21 2022

@author: fiora
"""

import logging

from attrs import ( 
                    define,
                    field,
                    Factory,
                    validate,
                    validators
                )

# PANDAS
from pandas import (
                    DataFrame as pandas_dataframe,
                    read_csv as pandas_read_csv
    )

# PYARROW
from pyarrow import (
                    BufferReader
    )

# POLARS
from polars import (
                    String as polars_string,
                    read_csv as polars_read_csv,
                    col,
                    from_epoch 
    )

from polars.dataframe import ( 
                    DataFrame as polars_dataframe
    )

from datetime import datetime

from numpy import array

# python base 
from time import sleep 
from requests import Session
from dotty_dict import dotty
from io import StringIO
from typing import Union, Optional

from dotty_dict import Dotty

# external 

# polygon-io source
from polygon import RESTClient as polygonio_client

# alpha-vantage source
from alpha_vantage.foreignexchange import ForeignExchange as av_forex_client
 
from .common import *
from ..config import read_config_file 

# constants
READ_RETRY_COUNT = 2 
READ_PAUSE       = 1   
READ_CHUNKSIZE   = 96

MINIMAL_RECENT_TIME_WINDOW_DAYS = 3 


__all__ = ['realtime_manager']

## Realtime data manager
#  source data providers to test APIs: polygon-IO, alpha-vantage

@define(kw_only=True, slots=True)
class realtime_manager:
    
    # interface parameters
    providers_key   : dict = field(validator=validators.instance_of(dict))
    # interface parameters
    config_file     : str = field(default=None,
                              validator=validators.instance_of(str))
    ticker          : str = field(default=None,
                              validator=validators.instance_of(str))
    data_filetype   : str = field(default='parquet',
                                 validator=validators.in_(SUPPORTED_DATA_FILES))
    data_path       : str = field(default=Path(DEFAULT_PATHS.REALTIME_DATA_PATH),
                              validator=validator_dir_path)
    engine          : str = field(default='pandas',
                                  validator=validators.in_(SUPPORTED_DATA_ENGINES))
    
    # internal parameters
    _db_dict        = field(factory=dotty,
                            validator=validators.instance_of(Dotty))
    _dataframe_type = field(default=pandas_dataframe)
    _ticker_polygonio    = field(default='',
                                 validator=validators.instance_of(str))
    _ticker_alphavantage = field(default='',
                                 validator=validators.instance_of(str))
  
    # if a valid config file is passed
    # arguments contained are assigned here 
    # if instantiation passed values are present
    # they will override the related argument
    # value in the next initialization step
    
    # if neither by instantation or config file
    # an argument value is set, the argument
    # will be set by asociated defined default 
    # or factory
        
    def __init__(self, **kwargs):
            
        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)
        
        if kwargs['config_file']:
            
            self.config_file = kwargs['config_file']
            config_path = Path(kwargs['config_file'])
            if config_path.exists() \
                and  \
                config_path.is_file() \
                and  \
                config_path.suffix == '.yaml':
                
                # read parameters from config file 
                # and force keys to lower case
                config_args = {key.lower(): val for key, val in 
                               read_config_file(config_path.absolute()).items()
                               }
                
                # set args from config file
                attrs_keys_configfile = \
                        set(_class_attributes_name).intersection(config_args.keys())
                
                for attr_key in attrs_keys_configfile:
                    
                    self.__setattr__(attr_key, 
                                     config_args[attr_key])
                    
                    _not_assigned_attrs_index_mask[ 
                           _class_attributes_name.index(attr_key)  
                    ] = False
                    
                # set args from instantiation 
                # override if attr already has a value from config
                attrs_keys_input = \
                        set(_class_attributes_name).intersection(kwargs.keys())
                
                for attr_key in attrs_keys_input:
                    
                    self.__setattr__(attr_key, 
                                     kwargs[attr_key])
                    
                    _not_assigned_attrs_index_mask[ 
                           _class_attributes_name.index(attr_key)  
                    ] = False
                
                # attrs not present in config file or instance inputs
                # --> self.attr leads to KeyError
                # are manually assigned to default value derived
                # from __attrs_attrs__
                
                for attr_key in array(_class_attributes_name)[
                        _not_assigned_attrs_index_mask
                ]:
                    
                    try:
                        
                        attr = [attr 
                                for attr in self.__attrs_attrs__
                                if attr.name == attr_key][0]
                        
                    except KeyError:
                        
                        logging.warning('KeyError: initializing object has no '
                                        f'attribute {attr.name}')
                        
                    except IndexError:
                        
                        logging.warning('IndexError: initializing object has no '
                                        f'attribute {attr.name}')
                    
                    else:
                        
                        # assign default value
                        # try default and factory sabsequently
                        # if neither are present
                        # assign None
                        if hasattr(attr, 'default'):
                            
                            if hasattr(attr.default, 'factory'): 
                    
                                self.__setattr__(attr.name, 
                                                 attr.default.factory())
                                
                            else:
                                
                                self.__setattr__(attr.name, 
                                                 attr.default)
                            
                        else:
                                
                            self.__setattr__(attr.name, 
                                             None)
                
                
            else:
                
                raise ValueError('invalid config_file')
                        
        else:
            
            # no config file is defined
            # call generated init 
            self.__attrs_init__(**kwargs)
            
        validate(self)
        
        self.__attrs_post_init__()
        
            
    def __attrs_post_init__(self):
        
        # Fundamentals parameters initialization
        self.ticker = self.ticker.upper()
        
        # files details variable initialization
        self.data_path = Path(self.data_path)    
        
        if self.engine == 'pandas':
            
            self._dataframe_type = pandas_dataframe 

        elif self.engine == 'polars':
            
            self._dataframe_type = polars_dataframe

        # set ticker in polygon format
        self._ticker_polygonio = to_source_symbol(
                self.ticker,
                REALTIME_DATA_PROVIDER.POLYGON_IO
        )
    
    def _getClient(self, provider, **kwargs):
        
        if provider == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:
            
            pass
            
        elif provider == REALTIME_DATA_PROVIDER.POLYGON_IO:
            
            return polygonio_client(api_key =
                                    self.providers_key[POLY_IO_KEY_ENV])
        
        
    
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
     
        
    def get_realtime_quote(self):
        
        poly_resp = self._poly_reader.get_last_forex_quote(self._from_symbol,
                                                           self._to_symbol)
        
        return av_resp 
                 
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
                                             last_close=True)
            
        else:
            
            if not day_start or not day_end:
                assert isinstance(recent_days_window, int), 'recent_days_window must be integer'
            
            # careful that option "outputsize='full'" does not have constant day start
            # so it is not possible to guarantee a consistent meeting of the 
            # function input 'day_start' and 'recent_days_window' when
            # they imply a large interval outside of the 
            # "outputsize='full'" option
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
        if not pd.api.types.is_datetime64_any_dtype(daily_df.index) \
            or \
            not daily_df.index.tzinfo:
            daily_df.index = any_date_to_datetime64(daily_df.index)
        
        daily_df.index.name = BASE_DATA_COLUMN_NAME.TIMESTAMP
        
        if last_close:
            
            # timestamp as column to include it in return data
            daily_df.reset_index(inplace=True)
            
            # get most recent line --> lowest num index
            return daily_df.iloc[CANONICAL_INDEX.LATEST_DATA_INDEX].to_dict()
        
        else:
            
            if isinstance(recent_days_window, int):
                # set window as DateOffset str with num and days
                days_window = '{days_num}d'.format(days_num=recent_days_window)
                
                day_start, day_end = get_date_interval(interval_end_mode='now',
                                                       interval_timespan=days_window,
                                                       normalize=True,
                                                       bdays=True)
                
            else:
                
                day_start = any_date_to_datetime64(day_start)
                day_end   = any_date_to_datetime64(day_end)
            
            # TODO: implement try-except if req data is outside
            #       response data time interval
            
            # return data based on filter output
            return daily_df[(daily_df.index >= day_start) \
                            & (daily_df.index <= day_end)].astype(DTYPE_DICT.TF_DTYPE)


    def _parse_aggs_data(self, 
                         data,
                         data_provider,
                         engine='polars'):
        
        if engine == 'pandas':
        
            # parse data and format data as common defined 
            df = pandas_dataframe(data)
            
            # keep base data columns
            extra_columns = list(set(data_df.columns).difference(DATA_COLUMN_NAMES.TF_DATA)) 
            data_df.drop(extra_columns, axis=1, inplace=True)
            
            if data_provider == REALTIME_DATA_PROVIDER.POLYGON_IO:
                
                data_df.index = any_date_to_datetime64(
                    data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                    unit='ms'
                )
                
            elif data_provider == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:
                
                # TODO
                pass
        
        elif engine == 'polars':
            
            df = polars_dataframe(data)
            
            extra_columns = list(set(df.columns).difference(DATA_COLUMN_NAMES.TF_DATA))
            
            df = df.drop(extra_columns)
            
            # convert timestamp column to datetime data type
            df = df.with_columns(
                    from_epoch(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                               time_unit='ms').alias(BASE_DATA_COLUMN_NAME.TIMESTAMP)
                )
        
        
        # convert to conventional dtype
        df = astype(df, POLARS_DTYPE_DICT.TIME_TF_DTYPE)
            
        return df
    
    
    def get_data(self, 
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
         
        start = any_date_to_datetime64(start)
        end = any_date_to_datetime64(end)
            
        data_provider = ''
        
        # try to get data with alpha_vantage if available
        # alpha vantage provides intraday data with high resolution
        
        # TODO: add check if alpha vantage key relates to
        #       premium subscription
        # if pd.Timestamp(start).tz_convert('utc')  \
        #     > pd.Timestamp.utcnow().normalize():
        
        #     # using alpha vantage wrapper
        #     # use alpha vantage intraday option if start date is later than last midnight
        #     # intraday from alpha vantage needs premium subscription
        #     data, meta_data = self._av_reader.get_currency_exchange_intraday(self._to_symbol,
        #                                                                      self._from_symbol,
        #                                                                      interval='1min',
        #                                                                      outputsize='full')
            
        #     # parse response
        #     data_df = pd.DataFrame(data)
        #     data_provider = REALTIME_DATA_PROVIDER.ALPHA_VANTAGE
        
        # if alpha vantage fails or it is not available 
        # --> polygon-ai is the backup provider
        
        avdata_valid = False
        
        if not avdata_valid:
            
            client = self._getClient(REALTIME_DATA_PROVIDER.POLYGON_IO)
            
            poly_aggs = []
            
            # TODO: set up try-except with BadResponse to manage provider 
            # subcription limitation
            
            # using Polygon-io client
            for a in client.list_aggs(  ticker      = self._ticker_polygonio, 
                                        multiplier  = 1, 
                                        timespan    = 'minute', 
                                        from_       = start,
                                        to          = end,
                                        adjusted    = True,
                                        sort        = 'asc' ):
                
                poly_aggs.append(a)
        
            
        data_provider = REALTIME_DATA_PROVIDER.POLYGON_IO
        
        data_df = self._parse_aggs_data(poly_aggs,
                                        data_provider,
                                        engine=self.engine)
        
        if check_timeframe_str(timeframe):
        
            return reframe_data(data_df, timeframe)
        
        else:
        
            return data_df
    
    