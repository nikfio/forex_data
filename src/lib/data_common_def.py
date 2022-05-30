# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 09:23:19 2022

@author: fiora
"""

# python base
from re import fullmatch
from pandas.tseries.frequencies import to_offset
from pandas import to_datetime


# common functions, constants and templates

# from matplotlib.dates import MONDAY, DateFormatter, DayLocator, WeekdayLocator

URL_TEMPLATE                    = 'http://www.histdata.com/download-free-forex-historical-data/?/' \
                                  'ascii/tick-data-quotes/{pair}/{year}/{month_num}'
DOWNLOAD_URL                    = "http://www.histdata.com/get.php"
DOWNLOAD_METHOD                 = 'POST'
DEFAULT_timeframe_TEMPLATE      = '{}s'
MONTHS                          = ['January', 'February', 'March', 'April', 'May', 'June',
                                   'July', 'August', 'September', 'October', 'November', 'December']
YEARS                           = list(range(2000, 2022, 1))

RAW_DATE_FORMAT                 = '%Y%m%d %H%M%S%f' 
DATE_FORMAT                     = '%Y-%m-%d %H:%M:%S' 

FILENAME_STR                    = '{pair}_Y{year}_{tf}.csv'

PAIR_FORMAT_ALPHAVANTAGE_STR    = '{TO}/{FROM}'
PAIR_MATCH_ALPHAVANTAGE_STR     = '^[A-Z]{3}/[A-Z]{3}$'

ALPHA_VANTAGE_KEY_ENV           = 'ALPHA_VANTAGE_KEY'


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
    
# column names for dataframes TICK and timeframe filtered
class DATA_COLUMN_NAMES:
    
    TICK_DATA               = ['timestamp','ask','bid','vol','p']
    TF_DATA                 = ['timestamp','open','close','high','low']
    TICK_DATA_TIME_INDEX    = ['ask','bid','vol','p']
    TF_DATA_TIME_INDEX      = ['open','close','high','low']
        
class DTYPE_DICT:
    
    TICK_DTYPE = {'ask': 'float32', 'bid': 'float32',
                  'vol': 'float16', 'p': 'float32'}
    TF_DTYPE   = {'open': 'float32', 'close': 'float32',
                  'high': 'float32', 'low': 'float32'}

class DATA_ACCESS:
    
    PANDAS_DATAREADER       = 'PANDAS-DATAREADER'
    ALPHA_VANTAGE_WRAPPER   = 'ALPHA_VANTAGE_WRAPPER'
    
class REALTIME_DATA_SOURCE:
    
    ALPHA_VANTAGE           = 'ALPHA_VANTAGE'
    YAHOO_FINANCE           = 'YAHOO_FINANCE'

class DB_MODE:
    
    FULL_MODE           = 'FULL_MODE'
    HISTORICAL_MODE     = 'HISTORICAL_MODE'
    REALTIME_MODE       = 'REALTIME_MODE'
    
    
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
    
    
def get_fxpair_components(symbol):
    
    components = fullmatch(PAIR_MATCH_ALPHAVANTAGE_STR, symbol)
    
    if components:
        
        from_symbol = components[0]
        to_symbol   = components[1]
        
        return from_symbol, to_symbol
    
    else:
        
        return None
    
    
        
        