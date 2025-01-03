# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 18:07:21 2022

@author: fiora
"""

from loguru import logger

from pathlib import Path

from attrs import ( 
                    define,
                    field,
                    validate,
                    validators
    )

# PANDAS
from pandas import (
                    DataFrame as pandas_dataframe
    )

# PYARROW
from pyarrow import (
                    Table as pyarrow_Table,
                    table as pyarrow_table,
                    compute as pc
    )

# POLARS
from polars import (
                    col,
                    from_epoch,
                    from_dict as polars_fromdict
    )

from polars.dataframe import ( 
                    DataFrame as polars_dataframe
    )


from numpy import array

# python base 
from dotty_dict import dotty
from io import StringIO
from typing import Optional

from dotty_dict import Dotty

# external 

# polygon-io source
import polygon
from polygon import (
        RESTClient as polygonio_client,
        BadResponse
    )

# alpha-vantage source
from alpha_vantage.foreignexchange import ForeignExchange as av_forex_client
 
from .common import *
from ..config import ( 
            read_config_file,
            read_config_string
    )

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
    data_filetype   : str = field(default='parquet',
                                 validator=validators.in_(SUPPORTED_DATA_FILES))
    engine          : str = field(default='pandas',
                                  validator=validators.in_(SUPPORTED_DATA_ENGINES))
    
    # internal parameters
    _db_dict        = field(factory=dotty,
                            validator=validators.instance_of(Dotty))
    _dataframe_type = field(default=pandas_dataframe)
    _data_path = field(default = Path(DEFAULT_PATHS.BASE_PATH), 
                       validator=validator_dir_path(create_if_missing=True))
    _realtimedata_path = field(
                        default = Path(DEFAULT_PATHS.BASE_PATH) / DEFAULT_PATHS.REALTIME_DATA_FOLDER, 
                        validator=validator_dir_path(create_if_missing=True)
                     )
  
    # if a valid config file or string
    # is passed
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
        
        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)
        
        if kwargs['config_file']:
            
            config_path = Path(kwargs['config_file'])
            config_args = {}
            if config_path.exists() \
                and  \
                config_path.is_file() \
                and  \
                config_path.suffix == '.yaml':
                
                # read parameters from config file 
                # and force keys to lower case
                config_args = {key.lower(): val for key, val in 
                               read_config_file(str(config_path)).items()}
            
            elif isinstance(kwargs['config_file'], str):
                
                # read parameters from config file 
                # and force keys to lower case
                config_args = {key.lower(): val for key, val in 
                               read_config_string(kwargs['config_file']).items()
                               }
                
            else:
            
                logger.error('invalid config_file type '
                             '{kwargs["config_file"]}')
                raise TypeError
                        
            # check consistency of config_args
            if  (
                    not isinstance(config_args, dict)
                    or
                    not bool(config_args)
                ):
                
                logger.error(f'config_file {kwargs["config_file"]} '
                             'has no valid yaml formatted data')
                raise ValueError
            
            self.config_file = kwargs['config_file']
            
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
                    
                    logger.warning('KeyError: initializing object has no '
                                    f'attribute {attr.name}')
                    
                except IndexError:
                    
                    logger.warning('IndexError: initializing object has no '
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
            
            # no config file is defined
            # call generated init 
            self.__attrs_init__(**kwargs)
            
        validate(self)
        
        self.__attrs_post_init__()
        
            
    def __attrs_post_init__(self):
        
        # reset logging handlers
        logger.remove()
    
        # checks on data folder path
        if ( 
            not self._realtimedata_path.is_dir() 
            or
            not self._realtimedata_path.exists()
            ):
                    
            self._realtimedata_path.mkdir(parents=True,
                                          exist_ok=True)
        
        # add logging file handle
        logger.add(self._data_path / 'forexdata.log',
                   level="TRACE",
                   rotation="5 MB"
        )
        
        if self.engine == 'pandas':
            
            self._dataframe_type = pandas_dataframe 

        elif self.engine == 'pyarrow':
            
            self._dataframe_type = pyarrow_table 
            
        elif self.engine == 'polars':
            
            self._dataframe_type = polars_dataframe

    
    def _getClient(self, provider):
        
        if provider == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:
            
            return av_forex_client(key = 
                                    self.providers_key[ALPHA_VANTAGE_KEY_ENV])
            
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
                    
                    logger.error('alpha vantage listing not including forex tickers')
                    raise ValueError 
                    
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
        
        
        with self._getClient(REALTIME_DATA_PROVIDER.POLYGON_IO) as client:
            
            to_symbol, from_symbol = get_pair_symbols(ticker.upper())
            
            poly_resp = client.get_last_forex_quote(from_symbol,
                                                    to_symbol)
        
        return poly_resp 
                 
    
    def get_daily_close(self,
                        ticker,
                        last_close=False,
                        recent_days_window=None, 
                        day_start=None, 
                        day_end=None):
        
        to_symbol, from_symbol = get_pair_symbols(ticker.upper())
        
        try:
            
            client = self._getClient(REALTIME_DATA_PROVIDER.ALPHA_VANTAGE)
            
            if last_close:
            
                
                res = client.get_currency_exchange_daily(
                        to_symbol,
                        from_symbol,
                        outputsize='compact'
                )
                
                # parse response and return
                return self._parse_data_daily_alphavantage(
                        res,
                        last_close=True
                )
                
            else:
                
                if not day_start or not day_end:
                    assert isinstance(recent_days_window, int), \
                            'recent_days_window must be integer'
                
                # careful that option "outputsize='full'" does not have constant day start
                # so it is not possible to guarantee a consistent meeting of the 
                # function input 'day_start' and 'recent_days_window' when
                # they imply a large interval outside of the 
                # "outputsize='full'" option
                res = client.get_currency_exchange_daily(
                                                        from_symbol,
                                                        to_symbol,
                                                        outputsize='full'
                )
                
                # parse response and return
                return self._parse_data_daily_alphavantage(res,
                    last_close=False,
                    recent_days_window=recent_days_window, 
                    day_start=day_start, 
                    day_end=day_end
                ) 
            
        except BadResponse as e:
            
            logger.warning(e)
            return self._dataframe_type([])
        
        except Exception as e:
            
            logger.warning(f'Raised Exception: {e}')
            return self._dataframe_type([])
            
            
    def _parse_aggs_data(self, data_provider, **kwargs):
        
        if data_provider == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:
            
            return self._parse_data_daily_alphavantage(**kwargs)
        
        elif data_provider == REALTIME_DATA_PROVIDER.POLYGON_IO:
        
            return self._parse_data_aggs_polygonio(**kwargs)
        
        else:
            
            logger.error(f'data provider {data_provider} is invalid '
                         '- supported providers: {REALTIME_DATA_PROVIDER_LIST}')
            
            return self._dataframe_type()
            
            
    def _parse_data_daily_alphavantage(
            self, 
            daily_data,
            last_close=False,
            recent_days_window=None, 
            day_start=None, 
            day_end=None
        ):
    
        if not last_close:
            
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
                
            # try to convert to datetime data type if not already is
            if self.engine == 'polars':
                 
                day_start =  day_start.to_pydatetime()
                day_end = day_end.to_pydatetime()
            
        # parse alpha vantage response from daily api request
        resp_data_dict = daily_data[CANONICAL_INDEX.AV_DF_DATA_INDEX]
        
        # raw response data to dictionary
        timestamp  = list(resp_data_dict.keys())
        data_values = resp_data_dict.values()
        open_data  = [item['1. open']  for item in data_values]
        high_data  = [item['2. high']  for item in data_values]
        low_data   = [item['3. low']   for item in data_values]
        close_data = [item['4. close'] for item in data_values]
        
        if self.engine == 'pandas':
        
            df = pandas_dataframe(
                    {
                        BASE_DATA_COLUMN_NAME.TIMESTAMP : timestamp,
                        BASE_DATA_COLUMN_NAME.OPEN      : open_data,
                        BASE_DATA_COLUMN_NAME.HIGH      : high_data,
                        BASE_DATA_COLUMN_NAME.LOW       : low_data,
                        BASE_DATA_COLUMN_NAME.CLOSE     : close_data
                    }
            )
            
            df = astype(df, DTYPE_DICT.TIME_TF_DTYPE)
            
            # timestamp as column to include it in return data
            df.reset_index(inplace=True)
            
            if last_close:
               
                # get most recent line --> lowest num index
                df = df.iloc[CANONICAL_INDEX.AV_LATEST_DATA_INDEX]
        
            else:
                
                # return data based on filter output
                df = df[
                        (df[BASE_DATA_COLUMN_NAME.TIMESTAMP] >= day_start) \
                        & 
                        (df[BASE_DATA_COLUMN_NAME.TIMESTAMP]  <= day_end)
                ] 
                            
        elif self.engine == 'pyarrow':
            
            df = pyarrow_table(
                    {
                        BASE_DATA_COLUMN_NAME.TIMESTAMP : timestamp,
                        BASE_DATA_COLUMN_NAME.OPEN      : open_data,
                        BASE_DATA_COLUMN_NAME.HIGH      : high_data,
                        BASE_DATA_COLUMN_NAME.LOW       : low_data,
                        BASE_DATA_COLUMN_NAME.CLOSE     : close_data
                    }
            )
            
            # final cast to standard dtypes
            df = astype(df, PYARROW_DTYPE_DICT.TIME_TF_DTYPE)
            
            if last_close:
                
                df = df[CANONICAL_INDEX.AV_LATEST_DATA_INDEX]
                
            else:
                
                mask = pc.and_(
                            pc.greater(df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                       day_start),
                            pc.less(df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                       day_end)
                            )
                
                data_df = pyarrow_Table.from_arrays(df.filter(mask).columns,
                                                    schema=df.schema)
        
        elif self.engine == 'polars':
        
            df = polars_fromdict(
                {
                    BASE_DATA_COLUMN_NAME.TIMESTAMP : timestamp,
                    BASE_DATA_COLUMN_NAME.OPEN      : open_data,
                    BASE_DATA_COLUMN_NAME.HIGH      : high_data,
                    BASE_DATA_COLUMN_NAME.LOW       : low_data,
                    BASE_DATA_COLUMN_NAME.CLOSE     : close_data
                }
            )
            
            # convert timestamp column to datetime data type
            df = \
                df.with_columns(
                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).str.strptime(
                        polars_datetime('ms'), 
                        format=DATE_NO_HOUR_FORMAT
                    )
                )
            
            # final cast to standard dtypes
            df = astype(df, POLARS_DTYPE_DICT.TIME_TF_DTYPE)
            
            if last_close:
                
                df = df[CANONICAL_INDEX.AV_LATEST_DATA_INDEX]
                
            else:
                
                # filter on date
                df = \
                (
                    df
                    .filter(
                        col(BASE_DATA_COLUMN_NAME.TIMESTAMP).is_between(day_start,
                                                                        day_end   
                        )
                    ).clone()
                )
            
        return df


    def _parse_data_aggs_polygonio(
            self, 
            data=None,
            engine='polars'
        ):
        
        
        if engine == 'pandas':
        
            # parse data and format data as common defined 
            df = pandas_dataframe(data)
            
            # keep base data columns
            extra_columns = list(set(df.columns).difference(DATA_COLUMN_NAMES.TF_DATA)) 
            df.drop(extra_columns, axis=1, inplace=True)
            
            df.index = any_date_to_datetime64(
                df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                unit='ms'
            )
                
            # convert to conventional dtype
            df = astype(df, DTYPE_DICT.TIME_TF_DTYPE)
        
        elif engine == 'pyarrow':
            
            # TODO: convert Agg items into dicts
            #       call Table.from_pylist and set also 
            #       schema appropriate
            
            data_dict_list = [polygon_agg_to_dict(agg) 
                              for agg in data]
            
            df = pyarrow_Table.from_pylist(data_dict_list)
                                        
            extra_columns = list(set(df.column_names).difference(DATA_COLUMN_NAMES.TF_DATA))
            
            df = df.drop_columns(extra_columns)
        
            # convert to conventional dtype
            df = astype(df, PYARROW_DTYPE_DICT.TIME_TF_DTYPE)
            
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
                 ticker,
                 start=None, 
                 end=None, 
                 timeframe=None):
        """
         
        
        Parameters
        ----------
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
            
        # forward request only to polygon-io
        # set ticker in polygon format
        
        ticker_polygonio = to_source_symbol(
                ticker.upper(),
                REALTIME_DATA_PROVIDER.POLYGON_IO
        )
        
        try:
                
            client = self._getClient(REALTIME_DATA_PROVIDER.POLYGON_IO)
            
            poly_aggs = []
            
            # TODO: set up try-except with BadResponse to manage provider 
            # subcription limitation
            
            # using Polygon-io client
            for a in client.list_aggs(  ticker      = ticker_polygonio, 
                                        multiplier  = 1, 
                                        timespan    = 'minute', 
                                        from_       = start,
                                        to          = end,
                                        adjusted    = True,
                                        sort        = 'asc' ):
                
                poly_aggs.append(a)
                    
        except BadResponse as e:
            
            # to log
            logger.warning(e)
            return self._dataframe_type([])
            
        
        data_df = self._parse_aggs_data(REALTIME_DATA_PROVIDER.POLYGON_IO,
                                        data=poly_aggs,
                                        engine=self.engine)
        
        return reframe_data(data_df, timeframe)
    