## -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 09:23:19 2022

@author: fiora
"""

# python base
from re import ( 
                fullmatch,
                findall,
                search
            )

from pandas import (
                    DataFrame,
                    Timestamp,
                    isnull,
                    bdate_range,
                    to_datetime,
                    Timedelta
                )

from pandas.api.types import is_datetime64_any_dtype
from pandas.tseries.frequencies import to_offset
from pandas.tseries.offsets import DateOffset

from dateutil.rrule import (
                            rrule,
                            DAILY,
                            MO,
                            TU,
                            WE,
                            TH,
                            FR 
                        )

from pathlib import Path

from pyarrow import (
                    Table
                )

# common functions, constants and templates

HISTDATA_URL_TICKDATA_TEMPLATE  = 'http://www.histdata.com/download-free-forex-historical-data/?/' \
                                  'ascii/tick-data-quotes/{ticker}/{year}/{month_num}'

HISTDATA_URL_ONEMINDATA_TEMPLATE= 'http://www.histdata.com/download-free-forex-data/?/' \
                                  'ascii/1-minute-bar-quotes/{pair}/{year}/{month_num}'

HISTDATA_BASE_DOWNLOAD_URL      = "http://www.histdata.com/get.php"
HISTDATA_BASE_DOWNLOAD_METHOD   = 'POST'

MONTHS                          = ['January', 'February', 'March', 'April', 'May', 'June',
                                   'July', 'August', 'September', 'October', 'November', 'December']
YEARS                           = list(range(2000, 2022, 1))

DATE_NO_HOUR_FORMAT             = '%Y-%m-%d'
DATE_FORMAT_ISO8601             = 'ISO8601'
DATE_FORMAT_HISTDATA_CSV        = '%Y%m%d %H%M%S%f'

FILENAME_STR                    = '{ticker}_Y{year}_{tf}.{file_ext}'
DEFAULT_TIMEZONE                = 'utc'
TICK_TIMEFRAME                  = 'TICK'

## ticker PAIR of forex market
PAIR_GENERIC_FORMAT             = '{TO}/{FROM}'
SINGLE_CURRENCY_PATTERN_STR     = '[A-Z]{3}'

## PAIR ALPHAVANTAGE
PAIR_ALPHAVANTAGE_FORMAT        = '{TO}/{FROM}'
PAIR_ALPHAVANTAGE_PATTERN       = '^' + SINGLE_CURRENCY_PATTERN_STR + '/' \
                                      + SINGLE_CURRENCY_PATTERN_STR + '$'
ALPHA_VANTAGE_KEY_ENV           = 'ALPHA_VANTAGE_KEY'
AV_LIST_URL                     = 'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={api_key}'

## PAIR POLYGON IO
PAIR_POLYGON_FORMAT             = 'C:{TO}{FROM}'
PAIR_POLYGON_PATTERN            = '^C:' + SINGLE_CURRENCY_PATTERN_STR + \
                                        SINGLE_CURRENCY_PATTERN_STR + '$'
POLY_IO_KEY_ENV                 = 'POLYGON_IO_KEY'

## TIME PATTERN
TIME_WINDOW_PATTERN_STR         = '^[-+]?[0-9]+[A-Za-z]{1,}$'
TIME_WINDOW_COMPONENTS_PATTERN_STR         = '^[-+]?[0-9]+|[A-Za-z]{1,}$'
TIME_WINDOW_UNIT_PATTERN_STR    = '[A-Za-z]{1,}$'
GET_YEAR_FROM_TICK_KEY_PATTERN_STR = '^[A-Za-z].Y[0-9].TICK'
YEAR_FIELD_PATTERN_STR          = '^Y([0-9]{4,})$'              


### auxiliary CONSTANT DEFINITIONS
     
# dotty key template: <PAIR>.Y<year>.<timeframe>.<data-type>
class DATA_KEY:
    
    PAIR_INDEX              = 0
    YEAR_INDEX              = 1 
    TF_INDEX                = 2 
    DATATYPE_INDEX          = 3 

# filename template : <PAIR>_Y<year>_<timeframe>.<filetype>
class FILENAME_TEMPLATE:
    
    PAIR_INDEX              = 0
    YEAR_INDEX              = 1
    YEAR_NUMERICAL_CHAR     = 1
    TF_INDEX                = 2
    FILETYPE_INDEX          = 3

# default path to store data in locally
class DEFAULT_PATHS:
    
    HIST_DATA_PATH     = "C:/Database/Historical"
    REALTIME_DATA_PATH = "C:/Database/RealTime"
    
class DATA_FILE_TYPE:
    
    CSV_FILETYPE            = 'csv'
    PARQUET_FILETYPE        = 'parquet'
    
class DATA_FILE_COLUMN_INDEX:
    
    TIMESTAMP               = 0
    
SUPPORTED_DATA_FILES = [
                        DATA_FILE_TYPE.CSV_FILETYPE,
                        DATA_FILE_TYPE.PARQUET_FILETYPE
                    ]

### SINGLE BASE DATA COMPOSIION TEMPLATE: ['open','close','high','low']
###                                       with datetime/timestamp as index     
# column names for dataframes TICK and timeframe filtered
# OHLC and related column names
class DATA_COLUMN_NAMES:
    
    TICK_DATA               = ['timestamp','ask','bid','vol']
    TF_DATA                 = ['timestamp','open','high','low', 'close']
    TICK_DATA_TIME_INDEX    = ['ask','bid','vol','p']
    TF_DATA_TIME_INDEX      = ['open','high','low', 'close']             ## SELECTED AS SINGLE BASE DATA COMPOSION TEMPLATE          
    POLYGON_IO_AGGS         = ['open','high','low', 'close', 'volume', 'vwap', \
                               'timestamp', 'transactions' ]
    
BASE_DATA = DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX ## SELECTED AS SINGLE BASE DATA COMPOSION TEMPLATE  
BASE_DATA_WITH_TIME = DATA_COLUMN_NAMES.TF_DATA
       
class DTYPE_DICT:
    
    TICK_DTYPE = {'ask': 'float32', 
                  'bid': 'float32',
                  'vol': 'float32',
                  'p'  : 'float32'}
    TF_DTYPE   = {'open': 'float32', 'high': 'float32', 
                  'low': 'float32', 'close': 'float32'}
    TIME_TICK_DTYPE = {'timestamp' : 'datetime64[ms]',
                       'ask': 'float32', 
                       'bid': 'float32',
                       'vol': 'float32',
                       'p'  : 'float32'}
    TIME_TF_DTYPE   = {'timestamp' : 'datetime64[ms]',
                       'open': 'float32', 'high': 'float32', 
                       'low': 'float32', 'close': 'float32'}

class REALTIME_DATA_PROVIDER:
    
    ALPHA_VANTAGE           = 'ALPHA_VANTAGE'
    POLYGON_IO              = 'POLYGON-IO'

class DB_MODE:
    
    FULL_MODE           = 'FULL_MODE'
    HISTORICAL_MODE     = 'HISTORICAL_MODE'
    REALTIME_MODE       = 'REALTIME_MODE'
    
class ASSET_TYPE:
    
    STOCK               = 'STOCK'
    ETF                 = 'ETF'
    FOREX               = 'FOREX'
    
class BASE_DATA_COLUMN_NAME:
    
    TIMESTAMP = 'timestamp'    
    OPEN      = 'open'
    HIGH      = 'high'
    LOW       = 'low'
    CLOSE     = 'close'

class CANONICAL_INDEX:

    LATEST_DATA_INDEX       = 0
    AV_DF_DATA_INDEX        = 0
    AV_DICT_INFO_INDEX      = 1    

        
    
### auxiliary fast functions

# parse argument to get datetime object with date format as input
def infer_date_from_format_dt(s, date_format='ISO8601', unit=None):
    
    if unit:
        
        return to_datetime(s, 
                           unit     = unit,
                           utc      = True)
    
    else:
        
        return to_datetime(s, 
                           format   = date_format,
                           utc      = True)


# parse timeframe as string and validate if it is valid
# following pandas DateOffset freqstr rules and 'TICK' (=lowest timeframe available)
# link to official pandas doc
# https://pandas.pydata.org/docs/user_guide/timeseries.html#dateoffset-objects
def check_timeframe_str(tf):
    
    if tf == 'TICK':
        
        return tf
    
    else:
    
        try:
            to_offset(tf) 
        except ValueError:
            raise ValueError("Invalid timeframe: %s" % (tf))
        else: 
            return tf


## PAIR functions        
def get_pair_symbols(ticker):
        
    components  = findall(SINGLE_CURRENCY_PATTERN_STR, ticker)
    
    if len(components) == 2:
         
        return components[0], components[1]
    
    else:
        
        return None
    

def check_symbol(symbol, source):
    
    if source == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:
         
        if fullmatch(PAIR_ALPHAVANTAGE_PATTERN, symbol):
            
            return True
        
        else:
            
            return False
    
    elif source == REALTIME_DATA_PROVIDER.POLYGON_IO:
        
        if fullmatch(PAIR_POLYGON_FORMAT, symbol):
            
            return True
        
        else:
            
            return False
        
    else:
        
        if fullmatch(PAIR_POLYGON_FORMAT, symbol):
            
            return True
        
        else:
            
            return False
    
       
def to_source_symbol(ticker, source):
    
    to_symbol, from_symbol = get_pair_symbols(ticker) 
    
    if source == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:
    
        return PAIR_ALPHAVANTAGE_FORMAT.format(TO=to_symbol,
                                               FROM=from_symbol)
        
    elif source == REALTIME_DATA_PROVIDER.POLYGON_IO:
    
        return PAIR_POLYGON_FORMAT.format(TO=to_symbol,
                                          FROM=from_symbol)
    
    else:
        
        return PAIR_GENERIC_FORMAT.format(TO=to_symbol,
                                          FROM=from_symbol)
    

def check_time_offset_str(timeoffset_str):

    return isinstance(to_offset(timeoffset_str), DateOffset)


def timewindow_str_to_timedelta(time_window_str):
    
    
    if fullmatch(TIME_WINDOW_PATTERN_STR, time_window_str):
        
        return Timedelta(time_window_str)
    
    else:
        
        raise ValueError('time window pattern not match: "<integer_multiplier><unit>" str')
        
        
def any_date_to_datetime64(any_date, 
                           date_format='ISO8601',
                           unit=None):
    
    try:
        
        any_date = infer_date_from_format_dt(any_date, 
                                             date_format,
                                             unit=unit)
            
    except Exception as e:
        
        #TODO: to log
        raise ValueError(f'date {any_date} conversion failed, '
                         f'faile conversion to {date_format} '
                         'date format')
    
    if not any_date.tzinfo:
        
        any_date = any_date.tz_localize('utc')
    
    return any_date


def get_date_interval(start=None,
                      end=None,
                      interval_start_mode=None,
                      interval_end_mode='now',
                      interval_timespan=None,
                      freq=None,
                      normalize=False,
                      bdays=False):

    # create start and end date as timestamp instances
    start_date = Timestamp(start)
    end_date = Timestamp(end)
                
    if interval_timespan:
    
        # a variety of interval mode could be implemented
        
        # 'now' - end of date interval is timestamp now
        if interval_end_mode == 'now':
            
            end_date = Timestamp.utcnow()
            start_date = end_date - timewindow_str_to_timedelta(interval_timespan)
            
        
        if bdays:
            
            components  = findall(TIME_WINDOW_COMPONENTS_PATTERN_STR,
                                  interval_timespan)    
            
            # fixed days redundancy check available only with 'd' type requested timespan
            if components[1] == 'd':
                
                days_list = list(
                                    rrule(freq=DAILY, 
                                          dtstart=start_date,
                                          until  =end_date,
                                          byweekday=(MO,TU,WE,TH,FR))
                                )
                
                while len(days_list) < int(components[0]):
                    
                    start_date = start_date - Timedelta(days=1)
                    
                    days_list = list(
                                        rrule(freq=DAILY, 
                                        dtstart=start_date,
                                        until  =end_date,
                                        byweekday=(MO,TU,WE,TH,FR))
                                    )
                
    assert isinstance(start_date, Timestamp) \
           and isinstance(end_date, Timestamp), \
           'start or end is not a valid Timestamp instance'
    
    if normalize:
        
        if not isnull(start_date):
            start_date = Timestamp.normalize(start_date)
            
        if not isnull(end_date):
            end_date   = Timestamp.normalize(end_date)
            
    start_date = any_date_to_datetime64(start_date)
    end_date   = any_date_to_datetime64(end_date)
            
    # generate DateTimeIndex if freq is set
    # otherwise return just start and end of interval
    if freq:
        
        bdate_dtindex = bdate_range(start = start_date,
                                       end   = end_date,
                                       freq  = freq,
                                       tz    = 'UTC',
                                       normalize = normalize,
                                       name  = 'timestamp')
        
        return start_date, end_date, bdate_dtindex
        
    else:
        
        return start_date, end_date
    
    
def reframe_data(data, tf):
    
    # assert timeframe input value
    # TODO: to be different from 'TICK'
    tf = check_timeframe_str(tf)

    assert tf != TICK_TIMEFRAME, \
        f'reframe not possible wih target {TICK_TIMEFRAME}'
    assert isinstance(
        data, DataFrame), 'data input must be pandas DataFrame type'
    assert not data.empty, 'data input must not be empty'
   
    assert is_datetime64_any_dtype(data.index), \
        'data index column must be datetime dtype'

    if isinstance(data, DataFrame):
        
        ## use pandas functions to reframe data on pandas Dataframe
        
        # resample based on p value
        if all([col in DATA_COLUMN_NAMES.TICK_DATA_TIME_INDEX
                for col in data.columns]):
            
            # resample along 'p' column, data in ask, bid, p format
            return data.p.resample(tf).ohlc().interpolate(method=
                                                          'nearest')
            
        elif all([col in DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX
                  for col in data.columns]): 
            
            # resample along given data already in ohlc format
            return data.resample(tf).interpolate(method=
                                                 'nearest')
            
        else:
            
            raise ValueError(f'data columns {data.columns} invalid, '
                             f'required {DATA_COLUMN_NAMES.TICK_DATA_TIME_INDEX} '
                             f'or {DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX}')
            
    elif isinstance(data, Table):
        
        ## use pyarrow functions to reframe data on pyarrow Table
        pass


### UTILS FOR DOTTY DICTIONARY

def get_dotty_key_field(key, index):
    
    assert isinstance(key, str), \
            f'dotty key {key} invalid type, str required'
    
    try:
    
        field = key.split('.')[index]
        
    except IndexError:
        
        raise IndexError(f'index {index} invalid for key {key}')
        
    
    return field


def get_dotty_keys(dotty_dict,
                   root=False,
                   level=None,
                   parent_key=None):
    
    dotty_copy = dotty_dict.copy()
    
    if root:
        
        return dotty_copy.keys()
    
    elif level:
        
        assert isinstance(level, int) \
               and level >= 0, 'level must be zero or positive integer'
               
        # default start at root key
        level_counter = 0
        
        pass
        
    elif parent_key:
        
        assert isinstance(parent_key, str), 'parent key must be str'
        
        parent_dict = dotty_copy.pop(parent_key)
        
        if parent_dict:
            
            try:
                keys = parent_dict.keys()
            except KeyError as err:
                
                # TODO: error to be logged, now print
                print( '{error} : keys not found under {parent}'.format(error=str(err),
                                                                        parent=parent_key) )
                return []
                
            else:
                
               return [str(k) for k in keys]
                
        
        else:
            
            raise KeyError( '{parent} key not exist'.format(parent=parent_key) )
                
                
def get_dotty_leafs(dotty_dict):
    
    leaf_keys = list()
    
    def get_leaf(dotty_dict, parent_key):
        
        try: 
            
            if dotty_dict.keys():
                
                for key in dotty_dict.keys():
                    
                    key_w_parent = '{parent}.{key}'.format(parent=parent_key,
                                                           key=key)
                    
                    get_leaf(dotty_dict.get(key), key_w_parent)
            
        except AttributeError:
            
            leaf_keys.append( parent_key )
            
        except ValueError:
            
            leaf_keys.append( parent_key )
        
    # root field is temporary to have common start in any case in all leafs
    get_leaf(dotty_dict, 'root')
    
    # pull out root field from all paths to leafs
    leaf_keys = [ search('(?<=root.)\S+', leaf).group(0) for leaf in leaf_keys]
    
    return leaf_keys
        

def get_dotty_key_parent(key):
    
    assert isinstance(key, str), 'dotty key must be str type'
    
    # prune last field and rejoin with '.' separator
    # to recreate a dotty key
    parent_key = '.'.join( key.split('.')[:-2] )
    
    return parent_key


#def get_list_key_values(index):
    
    # generic function to return key value
    # as a list





# TODO: function that returns all leafs at a given
        # given level
        
        
### ATTRS GENERIC VALIDATORS

def validator_dir_path(instance, attribute, value):
    
    if not (
        Path(value).exists()
        and 
        Path(value).is_dir()
    ):
        
        raise ValueError('Required a valid directory path')
        
              
def validator_list_timeframe(instance, attribute, value):
    
    if not isinstance(value, list):
        
        raise TypeError(f'Required type list for argument {attribute}')
        
    if not all([
                check_time_offset_str(val) 
                for val in value
                ]):
        
        fails = value[
                 [
                    check_time_offset_str(val) 
                    for val in value
                ]           
        ]
        
        return ValueError('Values are not timeframe compatible: '
                          f'{fails}')
        
    
def validator_list_ge(min_value):
    
    def validator_list_values(instance, attribute, value):
        
        if not(
                isinstance(value, list)
                and 
                all([isinstance(val, int)
                     for val in value])
        ):
            
            raise TypeError('Required list of int type for argument '
                            f'{attribute}')
            
        if any([ 
                val < min_value
                for val in value
            ]):
            
            fails = value[ 
                [ 
                    val < min_value
                    for val in value
                ]
            ]
            
            raise ValueError(f'Values in {attribute}: {fails} '
                             f'are not greater than {min_value}')
    
    
    
# GENERIC UTILITIES

def list_remove_duplicates(list_in):
    
    return list(dict.fromkeys(list_in))
    

                             
        