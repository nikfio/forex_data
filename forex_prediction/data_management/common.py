# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 09:23:19 2022

@author: fiora
"""

# python base
from re import fullmatch, findall
import pandas as pd
from pandas.tseries.frequencies import to_offset
from pandas import to_datetime, Timedelta
from dateutil.rrule import rrule, DAILY, MO, TU, WE, TH, FR


# common functions, constants and templates


URL_TEMPLATE                    = 'http://www.histdata.com/download-free-forex-historical-data/?/' \
                                  'ascii/tick-data-quotes/{pair}/{year}/{month_num}'
# TODO: template for 1-minute bar data request  
                        
DOWNLOAD_URL                    = "http://www.histdata.com/get.php"
DOWNLOAD_METHOD                 = 'POST'

MONTHS                          = ['January', 'February', 'March', 'April', 'May', 'June',
                                   'July', 'August', 'September', 'October', 'November', 'December']
YEARS                           = list(range(2000, 2022, 1))

RAW_DATE_FORMAT                 = '%Y%m%d %H%M%S%f' 
DATE_FORMAT                     = '%Y-%m-%d %H:%M:%S' 
DATE_NO_HOUR_FORMAT             = '%Y-%m-%d'

FILENAME_STR                    = '{pair}_Y{year}_{tf}.csv'

PAIR_FORMAT_ALPHAVANTAGE_STR    = '{TO}/{FROM}'
PAIR_MATCH_ALPHAVANTAGE_STR     = '^[A-Z]{3}/[A-Z]{3}$'
POLY_FX_SYMBOL_FORMAT           = 'C:{TO}{FROM}'
SINGLE_CURRENCY_PATTERN_STR     = '[A-Z]{3}'
TIME_WINDOW_PATTERN_STR         = '^[-+]?[0-9]+[A-Za-z]{1,}$'
TIME_WINDOW_COMPONENTS_PATTERN_STR         = '^[-+]?[0-9]+|[A-Za-z]{1,}$'
TIME_WINDOW_UNIT_PATTERN_STR    = '[A-Za-z]{1,}$'

ALPHA_VANTAGE_KEY_ENV           = 'ALPHA_VANTAGE_KEY'
POLY_IO_KEY_ENV                 = 'POLYGON_IO_KEY'

AV_LIST_URL                     = 'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={api_key}'

DEFAULT_TIMEZONE                = 'utc'

# actual environment variable name containing key

### auxiliary CONSTANT DEFINITIONS

# TIMEFRAME macro
class TIMEFRAME_MACRO:
    
    MIN_TICK_TF      = 'TICK'
    ONE_HOUR_TF      = '1H'
    FOUR_HOUR_TF     = '4H'
    ONE_DAY_TF       = '1D'
    ONE_WEEK_TF      = '1W'
    ONE_MONTH_TF     = '1M'
    
# filename template : <PAIR>_Y<year>_<timeframe>.<filetype>
class FILENAME_TEMPLATE:
    
    PAIR_INDEX              = 0
    YEAR_INDEX              = 1
    YEAR_NUMERICAL_CHAR     = 1
    TF_INDEX                = 2
    FILETYPE_INDEX          = 3

class DEFAULT_PATHS:
    
    FOREX_LOCAL_HIST_DATA_PATH     = "C:/Database/Forex/Historical"
    FOREX_LOCAL_REALTIME_DATA_PATH = "C:/Database/Forex/RealTime"

### SINGLE BASE DATA COMPOSIION TEMPLATE: ['open','close','high','low']
###                                       with datetime/timestamp as index     
# column names for dataframes TICK and timeframe filtered
# OHLC and related column names
class DATA_COLUMN_NAMES:
    
    TICK_DATA               = ['timestamp','ask','bid','vol','p']
    TF_DATA                 = ['timestamp','open','high','low', 'close']
    TICK_DATA_TIME_INDEX    = ['ask','bid','vol','p']
    TF_DATA_TIME_INDEX      = ['open','high','low', 'close']             ## SELECTED AS SINGLE BASE DATA COMPOSION TEMPLATE          
    POLYGON_IO_AGGS         = ['open','high','low', 'close', 'volume', 'vwap', \
                               'timestamp', 'transactions' ]
    
BASE_DATA = DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX
BASE_DATA_WITH_TIME = DATA_COLUMN_NAMES.TF_DATA
       
class DTYPE_DICT:
    
    HISTORICAL_TICK_DTYPE = {'ask': 'float32', 'bid': 'float32',
                             'vol': 'float16', 'p': 'float32'}
    TF_DTYPE   = {'open': 'float32', 'high': 'float32', 
                  'low': 'float32', 'close': 'float32'}
    TIME_TF_DTYPE   = {'timestamp' : 'datetime64[ns]',
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
    
class BASE_DATA_FEATURE_NAME:
    
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

# parse date obj and return datetime as template defined 
def infer_date_dt(s): return to_datetime(s, 
                                         format   = DATE_FORMAT,
                                         exact    = True,
                                         utc      = True)

# parse RAW FILE date obj and return datetime as template defined 
def infer_raw_date_dt(s): return to_datetime(s, 
                                             format   = RAW_DATE_FORMAT,
                                             exact    = True,
                                             utc      = True)

# parse argument to get datetime object with date format as input
def infer_date_from_format_dt(s, date_format): 
    
    return to_datetime(s, 
                       format   = date_format,
                       exact    = True,
                       utc      = True)


# parse timeframe as string and validate if it is valid
# following pandas DateOffset freqstr rules and 'TICK' (=lowest timeframe available)
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
        
        
def check_AV_FX_symbol(symbol):
    
    if fullmatch(PAIR_MATCH_ALPHAVANTAGE_STR, symbol):
        
        return True
    
    else:
        
        return False
    
    
def get_fxpair_symbols(ticker):
    
    if check_AV_FX_symbol(ticker):
        
        components  = findall(SINGLE_CURRENCY_PATTERN_STR, ticker)
        
        to_symbol     = components[0]
        from_symbol   = components[1]
        
        return to_symbol, from_symbol
    
    else:
        
        return None
    
def get_poly_fx_symbol_format(ticker):
    
    to_symbol, from_symbol = get_fxpair_symbols(ticker) 
    
    return POLY_FX_SYMBOL_FORMAT.format(TO=to_symbol, FROM=from_symbol)
    
    
def timewindow_str_to_timedelta(time_window_str):
    
    
    if fullmatch(TIME_WINDOW_PATTERN_STR, time_window_str):
        
        return Timedelta(time_window_str)
    
    else:
        
        raise ValueError('time window pattern not match: "<integer_multiplier><unit>" str')
        
        
def any_date_to_datetime64(any_date, 
                           date_format=DATE_FORMAT):
    
    any_date_dt64 = any_date
    
    # set datetime64 type
    if not pd.api.types.is_datetime64_any_dtype(any_date_dt64):
        any_date_dt64 = infer_date_from_format_dt(any_date_dt64, 
                                                  date_format)
        
    if isinstance(any_date_dt64, pd.Timestamp):
        any_date_dt64 = any_date_dt64.to_datetime64()
            
    return any_date_dt64


def get_date_interval(start=None,
                      end=None,
                      interval_start_mode=None,
                      interval_end_mode='now',
                      interval_timespan=None,
                      freq=None,
                      normalize=False,
                      bdays=False):

    # create start and end date as timestamp instances
    start_date = pd.Timestamp(start)
    end_date = pd.Timestamp(end)
                
    if interval_timespan:
    
        # a variety of interval mode could be implemented
        
        # 'now' - end of date interval is timestamp now
        if interval_end_mode == 'now':
            
            end_date = pd.Timestamp.utcnow()
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
                
    assert isinstance(start_date, pd.Timestamp) \
           and isinstance(end_date, pd.Timestamp), \
           'start or end is not a valid Timestamp instance'
    
    if normalize:
        
        if not pd.isnull(start_date):
            start_date = pd.Timestamp.normalize(start_date)
            
        if not pd.isnull(end_date):
            end_date   = pd.Timestamp.normalize(end_date)
            
    start_date = any_date_to_datetime64(start_date)
    end_date   = any_date_to_datetime64(end_date)
            
    # generate DateTimeIndex if freq is set
    # otherwise return just start and end of interval
    if freq:
        
        bdate_dtindex = pd.bdate_range(start = start_date,
                                       end   = end_date,
                                       freq  = freq,
                                       tz    = 'UTC',
                                       normalize = normalize,
                                       name  = 'timestamp')
        
        return start_date, end_date, bdate_dtindex
        
    else:
        
        return start_date, end_date
    
    
def reframe_tf_data(data, tf):
    
    pass
                                               
        