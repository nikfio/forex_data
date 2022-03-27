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
           
        2) resample TICK data to to have any larger timeframe desired
    
        3) add real time data download 
    
        4) add download of any indicator or generic data specified by the 
           user
    
"""

from absl import app
from data_manager import db_manager, db_parameters

def main(argv):
    
    param = db_parameters(pair           = 'EURUSD',
                          timescale      = '1H',
                          years          = [2000],
                          data_source    = 'HISTDATA',
                          nrows_per_file = 50000
                          )
    
    db_test = db_manager(param)
    
    db_test.download_histdata()
    
    
    
if __name__ == '__main__':
    app.run(main)
