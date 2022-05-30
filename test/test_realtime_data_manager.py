# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 22:13:04 2022

@author: fiora
"""

import os
from absl import app

from datetime import datetime, timedelta

# custom lib
import data_common_def as dc_def
from data_manager import db_manager, db_parameters

import pandas_datareader.data as web

# example: get 2 days data setting today as end date
example_end_date   = datetime.today()
example_start_date = example_end_date - timedelta(days=-2)



def main(argv):
    
    
    av_key = os.getenv(dc_def.ALPHA_VANTAGE_KEY_ENV)
    
    f = web.DataReader("USD/JPY", "av-forex", \
                       api_key=av_key)
    
    rt_param = db_parameters(mode            = dc_def.DB_MODE.REALTIME_MODE,
                             pair            = 'EUR/USD',
                             timeframe       = '1H',
                             rt_data_access  = dc_def.DATA_ACCESS.PANDAS_DATAREADER,
                             rt_data_source  = dc_def.REALTIME_DATA_SOURCE.ALPHA_VANTAGE,
                             api_key         = av_key )
    
    test_rt_data_manager = db_manager(rt_param)
    
    

    
if __name__ == '__main__':
    app.run(main)
