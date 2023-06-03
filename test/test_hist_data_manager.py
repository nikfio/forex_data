# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 22:03:27 2022

@author: fiora
"""

"""
Description: 
    test data_manager object historical data feature:
        
        1) download historical data and save to file in minimal timeframe
           available (TICK) to avoid further downloads at each run
           
        2) resample TICK data to to have any larger timeframe specified
            2.1) manage read/write of lmdb files 
    
        3) add download of any indicator or generic data other than 
           pair exchange data
    
"""

from absl import app

from forex_prediction import (
                                read_config_file,
                                db_parameters,
                                db_manager
                             )

# TODO: add logging options via input FLAGS

def main(argv):
    
    # TODO: get logging handler
    
    # load settings 
    config_set = read_config_file(r'C:\Database\settings\general_config.yaml')
    
    # create parameters structure
    init_param = db_parameters(pair           = config_set['PAIR'],
                               timeframe      = config_set['TIMEFRAME'],
                               years          = config_set['YEARS'])
    
    # db instantiation                            
    db_test = db_manager(init_param)
    
    # further data loading 
    # managed internally if it is necessary to download from the net
    # or if data is available in local folder
    db_test.add_historical_data([2003,2004,2005])
    
    # add new timeframe
    db_test.add_timeframe(['1M', '1W'],
                          update_all=True)
    
    # plot data with
    # timestamp start and end bounds
    # timeframe specified
    db_test.plot(source_type = 'Historical_data',
                 tf          = '1D',
                 start_date  = '2001-10-02 18:00:00',
                 end_date    = '2002-06-23 23:00:00')
    
    
if __name__ == '__main__':
    app.run(main)
