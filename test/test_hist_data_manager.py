# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 22:03:27 2022

@author: fiora
"""

"""
Description: 
    test data_manager object:
        
        1) download historical data and save to file in minimal timeframe
           available (TICK) to avoid further downloads at each run
           
        2) resample TICK data to to have any larger timeframe specified
            2.1) manage read/write of lmdb files 
    
        3) add real time data download 
    
        4) add download of any indicator or generic data other than 
           pair exchange data
    
"""

from absl import app
from data_manager import db_manager, db_parameters

from src import *

#TODO: add logging options via input FLAGS

def main(argv):
    
    #TODO: get logging handler
    
    init_param = db_parameters(pair           = 'EURUSD',
                               timeframe      = '4H',
                               years          = [2000,2001,2002],
                               data_source    = 'HISTDATA')
    
    # db instantiation                            
    db_test = db_manager(init_param)
    
    # further data loading 
    # managed internally if it is necessary to download from the net
    # or if data is available in local folder
    db_test.download_histdata([2003,2004,2005])
    
    # plot data with
    # timestamp start and end bounds
    # timeframe specified
    db_test.plot(source_type = 'Historical_data',
                 tf          = '1D',
                 start_date  = '2001-10-02 18:00:00',
                 end_date    = '2002-06-23 23:00:00')
    
    
if __name__ == '__main__':
    app.run(main)
