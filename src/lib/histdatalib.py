import sys
import pandas as pd
from pandas.tseries.frequencies import to_offset
import zipfile
import re
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from pathlib import Path
from requests import Session
from datetime import datetime as dt
from io import BytesIO
from mpl_finance import candlestick_ohlc
from unittest import TestCase
from absl import logging
from dotty_dict import dotty
from typing import NamedTuple

# external lib to download data from histdata site
import histdata.api


# from matplotlib.dates import MONDAY, DateFormatter, DayLocator, WeekdayLocator

URL_TEMPLATE = 'http://www.histdata.com/download-free-forex-historical-data/?/' \
               'ascii/tick-data-quotes/{pair}/{year}/{month_num}'
DOWNLOAD_URL = "http://www.histdata.com/get.php"
DOWNLOAD_METHOD = 'POST'
DEFAULT_timeframe_TEMPLATE = '{}s'
MONTHS = ['January', 'February', 'March', 'April', 'May', 'June',
          'July', 'August', 'September', 'October', 'November', 'December']
YEARS = list(range(2000, 2022, 1))

# parse string and return datetime as template defined 
def infer_date(s): return dt.strptime(s + '000', "%Y%m%d %H%M%S%f")

# parse timeframe as string and validate if it is valid
# following pandas DateOffset freqstr rules and 'TICK' (=lowest timeframe available)
def check_timeframe_str(tf):
    
    try:
        to_offset(tf) or tf == 'TICK'
    except ValueError:
        raise ValueError("Invalid timeframe: %s" % (tf))
    else: 
        return tf
    
### auxiliary def
class TIMEFRAME_MACRO:
    
    MIN_TICK_TF      = 'TICK'
    ONE_HOUR_TF      = '1H'
    FOUR_HOUR_TF     = '4H'
    ONE_DAY_TF       = '1D'
    ONE_WEEK_TF      = '1W'
    ONE_MONTH_TF     = '1M'
    
# filename template : <PAIR>_<year>_<timeframe>.<filetype>
class FILENAME_TEMPLATE:
    
    PAIR_INDEX       = 0
    YEAR_INDEX       = 1
    TF_INDEX         = 2
    FILETYPE_INDEX   = 3
    

### class using hddl repo
class HistDataManager(TestCase):

    def __init__(self, pair, data_path, years=None, months=None, timeframe='1H',
                 perform_download=False, nrows_per_file=None):
        """
        The Hist Data Manager is capable of downloading tick data of one Month
        and to convert it to the desired timeframe.
        :param pair (str):  the currency pair to download eg. EURUSD, USDNZD, ...
        :param data_path (str):  the folder path to search for data or save data files downloaded
        :param years (list of int):    the years of interest
        :param month (list of str):   the months of interest (1-12)
        :param timeframe (freq param):  timeframe of candledata,
                                        compliant to pandas Grouper freq param
        :param perform_download (bool): load data based constructor info params

        :method download:   starts the download and returns a pandas.DataFrame
                            with columns (open, close, high, low) indexed by date.
        """
        
        # assert timeframe is a valid freq string value 
        # following pandas DateOffset freqstr rules
        self._tf = check_timeframe_str(timeframe)
        
        # internal
        # list of years currently managed by object instance  
        self._years = list()                      
        
        # Fundamentals parameters initialization
        self._pair = pair.upper()
        self.session = Session()
        self.url = str()

        # files details variable initialization
        self._data_path        = Path(data_path)
        self._nrows_per_file   = nrows_per_file
        
        # db currently loaded on object instance
        self._db_dotdict       = dotty()
        # local data db
        self._local_db_dotdict = dotty()

        # perform data download at object instantiate
        if perform_download:
            
            if not years:
                years          = YEARS
            else:
                self.assertTrue(set(years).issubset(YEARS))
                
            self.download(years, timeframe=self._tf)

    def db_key(self, pair, year, timeframe, data_type):
        """
        
        get a str key of dotted divided elements
        
        key template = pair.year.timeframe.data_type

        Parameters
        ----------
        pair : TYPE
            DESCRIPTION.
        year : TYPE
            DESCRIPTION.
        data_type : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
            
        tf = check_timeframe_str(timeframe)
        
        return '.'.join([str(pair), str(year), str(tf), str(data_type)])
    
    def _db_all_key(pair, timeframe, data_type):
        
        # all key template = pair.ALL.timeframe.data_type
        
        tf = check_timeframe_str(timeframe)
        
        return '.'.join([str(pair), 'ALL', str(tf), str(data_type)])
        
    def _prepare(self):
        """
        

        Returns
        -------
        None.

        """
        
        r = self.session.get(self.url)
        m = re.search('id="tk" value="(.*?)"', r.text)
        tk = m.groups()[0]
        self.tk = tk

    def _download_raw(self, year, month_num):
        """
        
        Download a month data as a unique block
        
        
        Parameters
        ----------
        year : TYPE
            DESCRIPTION.
        month_num : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        
        headers = {'Referer': self.url}
        data = {'tk': self.tk, 'date': year, 'datemonth': "%d%02d" % (year, month_num), 'platform': 'ASCII',
                'timeframe': 'T', 'fxpair': self.pair}
        r = self.session.request(DOWNLOAD_METHOD, DOWNLOAD_URL, data=data, headers=headers, stream=True)
        bio = BytesIO()
        size = 0
        if logging.level_info():
            logging.info("Starting to download: %s - %d - %s\n" % (self.pair, year, MONTHS[month_num-1]))
        for chunk in r.iter_content(chunk_size=2 ** 19):
            bio.write(chunk)
            size += len(chunk)
            if logging.level_info():
                sys.stdout.write("\rDownloaded %.2f kB" % (1. * size / 2 ** 10))

        print(flush=True)
        try:
            zf = zipfile.ZipFile(bio)
        except zipfile.BadZipFile:
            
            print('%s - %d - %s not found or invalid download' % (self.pair, year, MONTHS[month_num-1]))
            
        else:
            
            # return raw zip files 
            return zf.open(zf.namelist()[0])

    def _reframe_data(self, tick_data=None, tf=None):
        """
        
        Resample data to have the whole block in the target timeframe

        Parameters
        ----------
        tick_data : TYPE, optional
            DESCRIPTION. The default is None.
        tf : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        data : TYPE
            DESCRIPTION.

        """
        
        # assert timeframe input value
        tf = check_timeframe_str(tf)

        assert bool(tick_data), 'tick_data input must not be empty'
        assert isinstance(tick_data, pd.DataFrame), 'tick_data input must be pandas DataFrame type'
        assert tick_data.names() == ['ask','bid','vol'], 'tick data input must be raw downloaded tick data'

        grp = tick_data['p'].groupby(pd.Grouper(freq=tf))
        # TEST: .resample method to ease data reframe given a timeframe as input
        grp_resample = tick_data.resample(tf)
        first = grp.first()
        data = pd.DataFrame(columns='open close high low'.split(), index=first.index)
        # data.open=grp.first() # better to use last close price
        data.close = grp.last()
        data.open = data.close.shift(1)
        data.high = grp.max()
        data.low = grp.min()
        data = data.fillna(method='pad')
        data.high = data.max(axis=1)
        data.low = data.min(axis=1)
        data.index = data.index.tz_localize('CET').tz_convert(None)

        return data

    def _parse_raw_download_files(self,raw_files_list=None):
        """
        

        Parameters
        ----------
        raw_files_list : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        None.

        """
        
        assert raw_files_list, 'raw files list must not be empty'
       
        # funtions is specific for format of files downloaded
        # parse all available files and concatenate data
        df = pd.concat(pd.read_csv(file, header=None, names='ask bid vol'.split(),
                                        parse_dates=True, date_parser=infer_date)
                            for file in raw_files_list)

        
        df['p'] = (self.df.ask + self.df.bid) / 2

        return df
    
    
    def _get_years_str_list(self):
        
        # get year keys
        years_filter_key = '{pair}.:'
        years_keys = self._db_dotdict[years_filter_key]
        years_keys.sort(key=int)
        
        # return list of int elements as manipulation is easier
        return [int(year) for year in years_keys]
            
            
    def _update_all_block_data(self, timeframe=None):
        
        # get year keys
        years_keys_str = self._get_years_list()
        
        all_df = pd.DataFrame()
        
        # loop through years in order from oldest to most recent
        for year in years_keys_str:
        
            # get df from year key
            year_tick_key = self.db_key(self.pair.upper(), year, 'TICK', 'df')
            assert isinstance(self._db_dotdict[year_tick_key], pd.DataFrame)
            
            # append to all_df 
            pd.concat(all_df, self._db_dotdict[year_tick_key])
            
        # assign 'ALL' tick key
        all_tick_key = self._db_all_key('TICK', 'df')
        self._db_dotdict[all_tick_key] = all_df
        
        # assign 'ALL' key with specified timeframe if set
        if timeframe:
            all_timeframe_df = self._reframe_data(tick_data=all_df,
                                                  tf=timeframe)
            
            all_tf_key = self._db_all_key(timeframe, 'df')
            self._db_dotdict[all_tf_key] = all_timeframe_df
        
            
    def _slice_data(self, start=None, end=None, tf=None):
        """
        

        Parameters
        ----------
        start : string, optional
            DESCRIPTION. The default is None.
        end : string, optional
            DESCRIPTION. The default is None.
        tf : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        TYPE
            return a slice of data currently managed
            by object instance

        """
        
        # parse date input to have specific format date type
        start_dt = dt.strptime(start, "%Y-%m-%d %H:%M:%S")
        end_dt = dt.strptime(end, "%Y-%m-%d %H:%M:%S") 
        
        # slice data from the 'all' block, key reference
        all_tick_key = self._db_all_key('TICK', 'df')
        all_df = self._db_dotdict[all_tick_key]
        
        # create a index mask
        slice_index = (all_df.index >= start_dt) & (all_df.index <= end_dt)
        
        if tf:
            
            return self._reframe_data(timeframe=tf, 
                                      data=all_df.loc[slice_index])
        
        else: 
            
            return all_df.loc[slice_index]

    def year_data_to_file(self, year, tf=None):
        """
        

        Parameters
        ----------
        year : TYPE
            DESCRIPTION.
        tf : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        None.

        """
        
        pair_path = self.data_path / self.pair.upper()
        if not pair_path.is_dir() or not pair_path.exists():
            pair_path.mkdir(parents=True, exist_ok=False)

        year_path = pair_path / str(year).upper()
        if not year_path.is_dir() or not year_path.exists():
            year_path.mkdir(parents=True, exist_ok=False)
            
        # alternative: get year by referenced key
        year_key = self.db_key(self._pair, year, tf, 'df')
        year_data = self._db_dotdict[year_key]

        filename = "{pair}_{year}_{tf}.csv".format(pair=self._pair, year=year, tf=tf)
        filepath = year_path / filename
        
        if filepath.exists() and filepath.is_file():
            logging.info("File {} already exists".format(filepath))
            return
        
        year_data.to_csv(filepath.absolute(), index=True, 
                         header=True,
                         date_format="%Y-%m-%d %H:%M:%S")

    def download(self, years, timeframe=None, search_local=False):
        """
        Execute download of all years and months specified
        :param years: years data to be loaded from offline folder
        :param timeframe: target timeframe selected
        :return:    a pandas.DataFrame with columns (open, close, high low) indexed
                    by date
        """
        
        # check timeframe str ok even if here is redundant
        
        if not timeframe:
            timeframe = self._tf 
        else:
            timeframe = check_timeframe_str(timeframe)
        
        # convert to list of int
        if not all(isinstance(year, int) for year in years):
            years = [int(year) for year in years]

        self.assertTrue(set(years).issubset(YEARS), 'years input must be included in available YEARS list')

        # CHECK IF YEARS TO DOWNLOAD IS NOT ALREADY IN SELF INTERNAL MEMORY
        self._years = self._get_years_str_list()
        new_years = set(years).difference(self._years)

        # search if years data are already available offline
        if search_local:
            years_offline = self.load_data_folder(self.data_path, new_years, timeframe)
        else:
            years_offline = list()
            
        # years not found on disk must be downloaded
        new_years_to_download = set(new_years).difference(years_offline)

        # download data not available offline (on disk) and save to file on disk
        months_file_list  = list()
        year_tick_df      = pd.DataFrame()
        year_timeframe_df = pd.DataFrame()
        
        if new_years_to_download:
            
            for year in new_years_to_download:
                # Loop through selected months
                for month in MONTHS:
                    month_num = MONTHS.index(month) + 1
                    self.url = URL_TEMPLATE.format(pair=self.pair, year=year, month_num=month_num)
                    self._prepare()
                    months_file_list.append(self._download_raw(year, month_num))

                # parse year data and save it to file on disk
                year_tick_df = self._parse_raw_download_files(raw_files_list=months_file_list)
                
                # get key for dotty dict: TICK
                year_tick_key = self.db_key(self.pair.upper(), year, 'TICK', 'df')
                self._db_dotdict[year_tick_key] = year_tick_df
                
                year_timeframe_df = self._reframe_data(tick_data=year_tick_df, tf=timeframe)
                # get key for dotty dict: timeframe
                year_tf_key = self.db_key(self.pair.upper(), year, timeframe, 'df')
                self._db_dotdict[year_tf_key] = year_timeframe_df
                
                # dump new year data just downloaded to local folder data
                self.year_data_to_file(year)
                
                months_file_list.clear()
                
        # data collecting done --> update 'ALL' key block data
        self._update_all_block_data(timeframe=timeframe)


    def load_data_folder(self, folderpath, years_to_load, timeframe=None):
        """
        

        Parameters
        ----------
        folderpath : TYPE
            DESCRIPTION.
        years_to_load : TYPE
            DESCRIPTION.
        timeframe : TYPE, optional
            DESCRIPTION. The default is None.

        Raises
        ------
        FileNotFoundError
            DESCRIPTION.
        NotADirectoryError
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        
        # if year = None load every data found?
        if not years_to_load:
            logging.error("Called load_data function but no years to load specified")
            return None

        # prepare predefined path and check if exists
        folderpath = Path(folderpath) / self.pair.upper()
        if not folderpath.exists():
            raise FileNotFoundError("Directory {0} Not Found".format(folderpath.name))
        elif not folderpath.is_dir():
            raise NotADirectoryError("{0} is Not a Directory".format(folderpath.name))

        # list all specifed pair data files in folder path and subdirs 
        local_files = list(folderpath.glob('**/{pair}_*.csv'.format(pair=self._pair)))
        local_files_name = [file.name for file in local_files]
        
        # intiate list: set false if file with specified timeframe is not found --> call reframe
        years_found = [False] * len(years_to_load)
        years_tf_found = [False] * len(years_to_load)
        local_tick_df  = pd.DataFrame()
        
        # parse files and fill details list
        for file in local_files:
            
            # get years available in offline data (local disk)
            local_filepath_key = file.name.replace('_','.')
            local_file_details = local_filepath_key.split(sep='.')
            
            file_pair = local_file_details[FILENAME_TEMPLATE.PAIR_INDEX]
            file_year = int(local_file_details[FILENAME_TEMPLATE.YEAR_INDEX])
            file_tf   = local_file_details[FILENAME_TEMPLATE.TF_INDEX]
            
            # check at timeframe index file has a valid timeframe
            if check_timeframe_str(file_tf) == file_tf:
                    
                # assign filepath key in local db
                self._local_db[local_filepath_key] = file.resolve()
                
                # search if years to load is present in local db
                if file_pair == self._pair \
                   and (int(file_year) in years_to_load):
                           
                    
                    if file_tf == TIMEFRAME_MACRO.MIN_TICK_TF:
                        
                        # create key for dataframe type
                        year_tick_key = self.db_key(file_pair,
                                                    file_year,
                                                    'TICK',
                                                    'df')
                            
                        local_tick_df = pd.read_csv(file, sep=',', header=0,
                                                    parse_dates=True,
                                                    date_parser=infer_date)
                        
                        self._db_dotdict[year_tick_key] = local_tick_df
                        
                        year_tf_key = self.db_key(file_pair,
                                                  file_year,
                                                  timeframe,
                                                  'df')
                        
                        if self._db_dotdict.get(year_tf_key) is None:
                                
                            year_tf_filename = '{pair}_{year}_{tf}.csv'.format(pair=self._pair,
                                                                               year=file_year,
                                                                               tf=timeframe)
                            
                            year_tf_key = year_tf_filename.replace('_','.')[-1] 
                        
                            # search for timeframe data in local files
                            if year_tf_filename in local_files_name:
                                
                                # get timeframe complete file
                                tf_file = local_files[local_files_name.index(year_tf_filename)]
                                
                                # read file and load data to db
                                self._db_dotdict[year_tick_key] = pd.read_csv(tf_file, sep=',', header=0,
                                                                              parse_dates=True,
                                                                              date_parser=infer_date)
                                
                            else:
                                
                                # timeframe file not found --> call reframe function
                                self._db_dotdict[year_tick_key] = self._reframe_data(tick_data=local_tick_df,
                                                                                     tf=timeframe)
                        
                    
                    elif file_tf == timeframe:
                        
                        # create key for dataframe type
                        year_tf_key = local_filepath_key
                        year_tf_key[-1] = 'df'
                            
                        self._db_dotdict[year_tf_key] = pd.read_csv(file, sep=',', header=0,
                                                                    parse_dates=True,
                                                                    date_parser=infer_date)
                        
                
                    years_tf_found[years_to_load.index(file_year)] = True
                            
                            
        # return years data loaded from offline database folder (disk)
        return years_to_load[years_found]


    def plot_data(self, date_bounds=None, timeframe=None):
        """
        Plot data in selected time frame and start and end date bound
        :param date_bounds: start and end of plot
        :param timeframe: timeframe to visualize
        :return: void
        """

        self.assertIsNotNone(date_bounds, msg='date_bounds param is required')
        self.assertTrue((len(date_bounds) == 2), msg='date_bounds must be two items')
        if not all(isinstance(date, str) for date in date_bounds):
            raise TypeError("date_bounds items type must be string")

        # parse date input to have specific format date type
        start_date = dt.strptime(date_bounds[0], "%Y-%m-%d %H:%M:%S")
        end_date = dt.strptime(date_bounds[1], "%Y-%m-%d %H:%M:%S")

        # consider also pandas date_range() function

        # make sure a timeframe specific is active
        chart_tf = timeframe or self._tf
        logging.info("Chart request: from {start} to {end} with {tf} time interval".format(start=str(start_date),
                                                                                           end=str(date_bounds[1]),
                                                                                           tf=str(chart_tf)))

        # different methods to frame data to plot using chart request info
        # create a index mask
        chart_index_mask = (self.df.index >= start_date) & (self.df.index <= end_date)
        chart_data = self.df.loc[chart_index_mask]
        # use directly a start and end index
        chart_data = self.df.loc[start_date:end_date]
        chart_data = self._reframe_data(timeframe=chart_tf, data=chart_data)

        # create fgure
        fig, ax = plt.subplots()
        fig.subplots_adjust(bottom=0.2)

        chart_d = (mdates.date2num(chart_data.index),
                                 chart_data.open, chart_data.high,
                                 chart_data.low, chart_data.close,
                                 chart_data),

        # candlestick chart type
        candlestick_ohlc(ax, zip(mdates.date2num(chart_data.index),
                                 chart_data.open, chart_data.high,
                                 chart_data.low, chart_data.close,
                                 chart_data),
                         width=0.6)

        ax.xaxis_date()
        ax.autoscale_view()
        plt.show()
        
        
        
        
### class using histdata.api repo     
