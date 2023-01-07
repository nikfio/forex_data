
# python base imports
import sys
import zipfile
import re
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import mplfinance as mpf
from pathlib import Path
from requests import Session
from io import BytesIO

# external 
from absl import logging
from dotty_dict import dotty

# alternative historical data query lib 
import histdata.api

# internally defined 
from .common import *

# TODO: replace 'pair' with 'symbol' so it is semantically correct 
# for any market item 


### class HISTORICAL DATA MANAGER
class HistDataManager:

    def __init__(self, 
                 pair, 
                 data_path, 
                 years=None, 
                 timeframe=None):
        """
        

        Parameters
        ----------
        pair : TYPE
            DESCRIPTION.
        data_path : TYPE
            DESCRIPTION.
        years : TYPE, optional
            DESCRIPTION. The default is None.
        months : TYPE, optional
            DESCRIPTION. The default is None.
        timeframe : TYPE, optional
            DESCRIPTION. The default is None.
        perform_download : TYPE, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        None.

        """
        
        # internal
        # list of years currently managed by object instance  
        # data type: int
        self._years_int = list()      

        # timeframe list                
        self._tf_list = list()
        
        # Fundamentals parameters initialization
        self._pair = pair.upper()
        self.session = Session()
        self.url = str()

        # files details variable initialization
        self._data_path        = Path(data_path)
        
        # db currently loaded on object instance
        self._db_dotdict       = dotty()
        # local data db
        self._local_db_dotdict = dotty()

        # perform data download at object instantiate
        if not years:
            years          = YEARS
            
        # initial download at object instantiation
        self.download(years=years, 
                      timeframe_list=timeframe,
                      search_local=True)


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
        
        return '.'.join([str(pair), 'Y'+str(year),
                         str(tf), str(data_type)])
    
    
    def db_all_key(self, pair, timeframe, data_type):
        
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


    def _download_month_raw(self, year, month_num):
        """
        
        Download a month data
        
        
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
                'timeframe': 'T', 'fxpair': self._pair}
        r = self.session.request(DOWNLOAD_METHOD, 
                                 DOWNLOAD_URL, 
                                 data=data, 
                                 headers=headers, 
                                 stream=True)
        bio = BytesIO()
        size = 0
        if logging.level_info():
            logging.info("Starting to download: %s - %d - %s" % (self._pair, 
                                                                 year, 
                                                                 MONTHS[month_num-1]))
        for chunk in r.iter_content(chunk_size=2 ** 19):
            bio.write(chunk)
            size += len(chunk)
            if logging.level_info():
                sys.stdout.write("\rDownloaded %.2f kB" % (1. * size / 2 ** 10))

        print(flush=True)
        try:
            zf = zipfile.ZipFile(bio)
        except zipfile.BadZipFile:
            
            # here will be a warning log
            print('%s - %d - %s not found or invalid download' % (self._pair, 
                                                                  year, 
                                                                  MONTHS[month_num-1]))
            return None
        
        else:
            
            # return raw zip files 
            return zf.open(zf.namelist()[0])


    def _reframe_tick_data(self, tick_data, tf):
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

        assert not tick_data.empty, 'tick_data input must not be empty'
        assert isinstance(tick_data, pd.DataFrame), 'tick_data input must be pandas DataFrame type'
        assert all(tick_data.columns == DATA_COLUMN_NAMES.TICK_DATA_TIME_INDEX), \
                'tick data input must be raw downloaded tick data'
                
        assert pd.api.types.is_datetime64_any_dtype(tick_data.index), \
               'index column must be datetime dtype'
        
        # resample along 'p' column
        df_resampler = tick_data.p.resample(tf)
        first        = df_resampler.first()
        data = pd.DataFrame(columns=DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX, 
                            index=first.index)
        
        # set timeframed data
        data.close   = df_resampler.last()
        # TODO: why not using resampler.first() ?
        data.open    = data.close.shift(1) 
        data.high    = df_resampler.max()
        data.low     = df_resampler.min()
        data         = data.fillna(method='pad')
        data.high    = data.max(axis=1)
        data.low     = data.min(axis=1)
        data.index   = data.index
        data.fillna(method='bfill', 
                    axis='columns', 
                    inplace=True)

        return data


    def _raw_file_to_df(self, raw_file):
        """
        

        Parameters
        ----------
        raw_files_list : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        None.

        """
        
        assert raw_file, 'raw files list must not be empty'
       
        # funtions is specific for format of files downloaded
        # parse file passed as input
        df = pd.read_csv(raw_file, 
                         sep         = ',', 
                         names       = DATA_COLUMN_NAMES.TICK_DATA,
                         dtype       = DTYPE_DICT.TICK_DTYPE,
                         index_col   = BASE_DATA_FEATURE_NAME.TIMESTAMP,
                         parse_dates = [BASE_DATA_FEATURE_NAME.TIMESTAMP],
                         date_parser = infer_raw_date_dt)
                          
        # set index name to BASE_DATA_FEATURE_NAME.TIMESTAMP

        # calculate 'p'
        df['p'] = (df.ask + df.bid) / 2

        return df
    
    
    def _get_years_list(self, pair, vartype):
        
        # work on copy as pop operation is 'inplace'
        # so the original db is not modified
        db_copy = self._db_dotdict.copy()
        
        # get keys at year level
        years_filter_keys = '{pair}'.format(pair=self._pair)
        
        # pop at year level in data copy 
        year_db = db_copy.pop(years_filter_keys)
        
        if year_db:
        
            try: 
                years_keys = year_db.keys()
            except KeyError:
                # no active year found --> return empty list
                return []
            else:
                
                # if present do not include 'ALL' key element 
                # remove first identifier character 'Y' to have canonical 
                # year values
                years_list = [key[FILENAME_TEMPLATE.YEAR_NUMERICAL_CHAR:] 
                              for key in years_keys 
                              if key != 'ALL']
                
        else:
            
            # empty db --> return empty list
            return []
            
        # sort to have oldest year first
        years_list.sort(key=int)
        
        # return list of elements as manipulation is easier
        # return type based on input specification
        if vartype == 'str':
            
            return [str(year) for year in years_list]
        
        elif vartype == 'int':
            
            return [int(year) for year in years_list]
        
        else:
            
            return [int(year) for year in years_list]
        
            
    def _get_year_timeframe_list(self, pair, year):
        
        # work on copy as pop operation is 'inplace'
        # so the original db is not modified
        db_copy = self._db_dotdict.get(self._pair).copy()
        
        # get key at timeframe level
        tf_key = 'Y{year}'.format(year=year)
        
        # pop at timeframe level in data copy 
        tf_db = db_copy.pop(tf_key)
        
        if tf_db:
        
            try: 
                tf_keys = tf_db.keys()
            except KeyError:
                # no active timeframe found --> return empty list
                return []
            else:
                
                return [key for key in tf_keys]
                
        else:
            
            # empty db --> return empty list
            return []
        
        
    def _update_all_block_data(self, timeframe_list=None):
        
        # get year keys
        years_list_str = self._get_years_list(self._pair, 'str')
        
        all_df = pd.DataFrame()
        
        # loop through years in order from oldest to most recent
        for year in years_list_str:
        
            # get df from year key
            year_tick_key = self.db_key(self._pair, year, 'TICK', 'df')
            assert isinstance(self._db_dotdict.get(year_tick_key), pd.DataFrame), \
                   'key %s is no valid DataFrame' % (year_tick_key)
            
            # concat to all_df 
            all_df = pd.concat([all_df, self._db_dotdict.get(year_tick_key)],
                               ignore_index = False,
                               copy         = True)
            
        # assign 'ALL' tick key
        all_tick_key = self.db_all_key(self._pair, 'TICK', 'df')
        self._db_dotdict[all_tick_key] = all_df
        
        # assign 'ALL' key with specified timeframe if set
        if timeframe_list:
            
            for tf in tf_list:
                
                all_timeframe_df = self._reframe_data(tick_data=all_df,
                                                      tf=tf)
                
                all_tf_key = self.db_all_key(self._pair, tf, 'df')
                self._db_dotdict[all_tf_key] = all_timeframe_df
            
            
    def add_timeframe(self, timeframe):
        
        tf = check_timeframe_str(timeframe)
        
        # cehck if already existing
        current_years = self._get_years_list(self._pair, 'str')
        
        for year in current_years:
            
            year_tf_list = self._get_year_timeframe_list(self._pair, year)
            
            if timeframe not in year_tf_list:
                
                pass
            
        # at op conclude append timeframe to general list
        self._tf_list.append(timeframe)
        
            
    def time_slice_data(self, timeframe, start=None, end=None):
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
        # TODO: use get_date_interval()
        
        # try to convert to datetime data type if not already is
        if not pd.api.types.is_datetime64_any_dtype(start):
            
            start_dt = infer_date_dt(start)
            
        if not pd.api.types.is_datetime64_any_dtype(end):
            
            end_dt   = infer_date_dt(end)
            
        # check timeframe req format
        # and if data is available
        check_timeframe_str(timeframe)
            
        if tf == self._tf:
            
            all_tf_key = self.db_all_key(self._pair, timeframe, 'df')
            all_df = self._db_dotdict.get(all_tf_key)
            
            
            # create an index mask along timestamp axis
            slice_index = (all_df.index >= start_dt) & (all_df.index <= end_dt)
            
            return all_df.loc[slice_index]
            
        else:
            
            # slice data from the 'all' block, key reference
            all_tick_key = self.db_all_key(self._pair, 'TICK', 'df')
            all_df = self._db_dotdict.get(all_tick_key)
            
            # create an index mask along timestamp axis
            slice_index = (all_df.index >= start_dt) & (all_df.index <= end_dt)
            
            if timeframe != TIMEFRAME_MACRO.MIN_TICK_TF:
                
                # timeframe different from currently managed, further reframe
                # needed
                return self._reframe_data(timeframe=tf, 
                                          data=all_df.loc[slice_index])
            
            else: 
                
                # TICK timeframe specified, no further reframe opeartion needed
                return all_df.loc[slice_index]


    def _year_data_to_file(self, year, tf=None):
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
        
        pair_path = self._data_path / self._pair
        if not pair_path.is_dir() or not pair_path.exists():
            pair_path.mkdir(parents=True, exist_ok=False)

        year_path = pair_path / str(year).upper()
        if not year_path.is_dir() or not year_path.exists():
            year_path.mkdir(parents=True, exist_ok=False)
            
        # alternative: get year by referenced key
        year_tf_key = self.db_key(self._pair, year, tf, 'df')
        assert isinstance(self._db_dotdict.get(year_tf_key), pd.DataFrame), \
               'key %s is no valid DataFrame' % (year_tf_key)
        year_data = self._db_dotdict.get(year_tf_key)

        filepath = year_path / self._get_filename(self._pair, year, tf)
        
        if filepath.exists() and filepath.is_file():
            logging.info("File {} already exists".format(filepath))
            return
        
        # IMPORTANT
        # avoid date_format parameter since it is reported that 
        # it makes to_csv to be excessively long with column data 
        # being datetime data type
        # see: https://github.com/pandas-dev/pandas/issues/37484
        #      https://stackoverflow.com/questions/65903287/pandas-1-2-1-to-csv-performance-with-datetime-as-the-index-and-setting-date-form
        year_data.to_csv(filepath.absolute(), 
                         index       = True, 
                         header      = True)
        
        
    def _get_file_details(self, filename):
        
        assert isinstance(filename, str), 'filename type must be str'
        
        # get years available in offline data (local disk)
        filename_details = filename.replace('_','.').split(sep='.')
     
        # store each file details in local variables 
        file_pair = filename_details[FILENAME_TEMPLATE.PAIR_INDEX]
        file_year = int(filename_details[FILENAME_TEMPLATE.YEAR_INDEX][FILENAME_TEMPLATE.YEAR_NUMERICAL_CHAR:])
        file_tf   = filename_details[FILENAME_TEMPLATE.TF_INDEX]
        
        # return each file details 
        return file_pair, file_year, file_tf
    
    
    def _get_filename(self, pair, year, tf):
        
        # based on standard filename template
        return FILENAME_STR.format(pair = self._pair,
                                   year = year,
                                   tf   = tf)
        
    
    def _update_local_data_folder(self, local_folderpath):
        
        # get active years loaded on db manager
        years_active = self._get_years_list(self._pair, 'int')
        
        # get file names in local folder
        _, local_files_name = self._list_local_data(local_folderpath)
        
        # loop through years loaded
        for year in years_active:
            
            year_tf_list = self._get_year_timeframe_list(self._pair, year)
            
            # loop through timeframes loaded
            for tf in year_tf_list:
                
                
                tf_filename = self._get_filename(self._pair,
                                                 year,
                                                 tf)
                
                key = self.db_key(self._pair, year, tf, 'df')
                
                # check if file is present in local data folder
                # and if valid dataframe is currently loaded in database
                if tf_filename not in local_files_name \
                   and isinstance(self._db_dotdict.get(key), \
                                  pd.DataFrame):
                                       
                    self._year_data_to_file(year, tf=tf)
                    
    
    def _download_year(self, year):
        
        year_tick_df = pd.DataFrame()
        
        # TODO: wrap download of a whole year in a function
        for month in MONTHS:
            
            month_num = MONTHS.index(month) + 1
            self.url = URL_TEMPLATE.format(pair      = self._pair, 
                                                  year      = year, 
                                                  month_num = month_num)
            self._prepare()
            file = self._download_raw(year, month_num)
            if file:
                month_data = self._raw_file_to_df(file)
                year_tick_df = pd.concat([year_tick_df, month_data], 
                                         ignore_index = False,
                                         copy         = True)
                
        return year_tick_df
    
    
    def download(self, 
                 years,
                 timeframe_list=None,
                 search_local=False,
                 update_db=False):
        
        # assert on years req
        assert isinstance(years, list), \
        'years {} invalid, must be list type'.format(years)
        
        assert set(years).issubset(YEARS), \
        'YEARS requested must contained in available years'
    
        # assert on timeframe req
        # check timeframe str to complain format convention
        for tf in timeframe_list:
            
            assert tf == check_timeframe_str, \
                   'timeframe {0} is invalid '.format(tf)
        
        # convert to list of int
        if not all(isinstance(year, int) for year in years):
            years = [int(year) for year in years]

        assert set(years).issubset(YEARS), \
               'years input must be included in available YEARS list'

        # CHECK IF YEARS TO DOWNLOAD IS NOT ALREADY IN SELF INTERNAL MEMORY
        self._years_int = self._get_years_list(self._pair, 'int')
        new_years = set(years).difference(self._years_int)

        # search if years data are already available offline
        if search_local:
            years_offline = self.load_data(self._data_path, list(new_years), timeframe)
        else:
            years_offline = list()
            
        # years not found on disk must be downloaded
        new_years_to_download = set(new_years).difference(years_offline)

        # update internal list of years loaded before new files download
        self._years_int = self._get_years_list(self._pair, 'int')
        
        if new_years_to_download:
            
            for year in new_years_to_download:
                
                # download data not available offline (on disk) and save to file on disk
                year_timeframe_df = pd.DataFrame()
                
                year_tick_df      = self._download_year()

                # get key for dotty dict: TICK
                year_tick_key = self.db_key(self._pair, year, 'TICK', 'df')
                self._db_dotdict[year_tick_key] = year_tick_df
                
                for tf in tf_list:
                    
                    # create dataframe
                    year_timeframe_df = self._reframe_data(tick_data=year_tick_df, tf=timeframe)
                    
                    # get key for dotty dict: timeframe
                    year_tf_key = self.db_key(self._pair, year, timeframe, 'df')
                    self._db_dotdict[year_tf_key] = year_timeframe_df
                    
                    
        if update_db:
        
            # data collecting done --> update 'ALL' key block data
            self._update_all_block_data(timeframe_list=tf_list)
            
            # dump new downloaded data not already present in local data folder
            self._update_local_data_folder(self._data_path)


    def _list_local_data(self, folderpath):
        
        # prepare predefined path and check if exists
        folderpath = Path(folderpath) / self._pair
        if not folderpath.exists():
            raise FileNotFoundError("Directory {0} Not Found".format(folderpath.name))
        elif not folderpath.is_dir():
            raise NotADirectoryError("{0} is Not a Directory".format(folderpath.name))

        # list all specifed pair data files in folder path and subdirs 
        local_files = list(folderpath.glob('**/{pair}_*.csv'.format(pair=self._pair)))
        local_files_name = [file.name for file in local_files]


        # check compliance of files to convention (see notes)
        # TODO: warning if no compliant and filter out from files found
        
        return local_files, local_files_name    


    def check_local_filename(self, filename):
        
        pass
        

    def local_load_data(self, folderpath, years_to_load, timeframe=None):
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
         
        # intiate list: set false if file with specified timeframe is not found --> call reframe
        years_found = list()
        local_tick_df  = pd.DataFrame()
        
        # list data available in local folder
        local_files, local_files_name = self._list_local_data(folderpath)
        
        # parse files and fill details list
        for file in local_files:
            
            # get years available in offline data (local disk)
            local_filepath_key = file.name.replace('_','.')
        
            # get file details
            file_pair, file_year, file_tf = self._get_file_details(file.name)
            
            # check at timeframe index file has a valid timeframe
            if check_timeframe_str(file_tf) == file_tf:
                    
                # assign filepath key in local db
                self._local_db_dotdict[local_filepath_key] = file.resolve()
                
                # create key for dataframe type
                year_tick_key = self.db_key(file_pair,
                                            file_year,
                                            'TICK',
                                            'df')
                
                # create key for dataframe type
                year_tf_key = self.db_key(file_pair,
                                          file_year,
                                          timeframe,
                                          'df')
                
                # search if years to load is present in local db
                if file_pair == self._pair \
                   and (int(file_year) in years_to_load):
                           
                    
                    if file_tf == TIMEFRAME_MACRO.MIN_TICK_TF \
                       and self._db_dotdict.get(year_tick_key) is None:
                        
                        local_tick_df = pd.read_csv(file, 
                                                    sep         = ',', 
                                                    header      = 0,
                                                    dtype       = DTYPE_DICT.TICK_DTYPE,
                                                    index_col   = 'timestamp',
                                                    parse_dates = ['timestamp'],
                                                    date_parser = infer_date_dt)
                        
                        self._db_dotdict[year_tick_key] = local_tick_df
                        
                        if self._db_dotdict.get(year_tf_key) is None:
                                
                            
                            year_tf_filename = self._get_filename(self._pair, 
                                                                  file_year,
                                                                  timeframe)
                        
                            # search for timeframe data in local files
                            if year_tf_filename in local_files_name:
                                
                                # get timeframe complete file
                                tf_file = local_files[local_files_name.index(year_tf_filename)]
                                
                                # read file and load data to db
                                self._db_dotdict[year_tf_key] = pd.read_csv(tf_file, 
                                                                            sep         = ',', 
                                                                            header      = 0,
                                                                            dtype       = DTYPE_DICT.TF_DTYPE,
                                                                            index_col   = 'timestamp',
                                                                            parse_dates = ['timestamp'],
                                                                            date_parser = infer_date_dt)
                                
                            else:
                                
                                # timeframe file not found --> call reframe function
                                self._db_dotdict[year_tf_key] = self._reframe_data(tick_data=local_tick_df,
                                                                                     tf=timeframe)
                        
                    
                    elif file_tf == timeframe \
                         and self._db_dotdict.get(year_tf_key) is None:
                        
                         self._db_dotdict[year_tf_key] = pd.read_csv(file, 
                                                                     sep         = ',', 
                                                                     header      = 0,
                                                                     dtype       = DTYPE_DICT.TF_DTYPE,
                                                                     index_col   = 'timestamp',
                                                                     parse_dates = ['timestamp'],
                                                                     date_parser = infer_date_dt)
                        
                    # append year in list of data found in local folder
                    years_found.append(file_year)
                    
                local_tick_df  = pd.DataFrame()
                            
        # return years data loaded from local database folder 
        return years_found


    def plot_data(self, chart_tf, start_date, end_date):
        """
        Plot data in selected time frame and start and end date bound
        :param date_bounds: start and end of plot
        :param timeframe: timeframe to visualize
        :return: void
        """

        
        logging.info("Chart request: from {start} to {end} with {tf} time interval".format(start= str(start_date),
                                                                                           end  = str(end_date),
                                                                                           tf   = str(chart_tf)))

        chart_data = self.time_slice_data(tf    = tf,
                                          start = start_date,
                                          end   = end_date)
        
        # create fgure
        fig, ax = plt.subplots()
        fig.subplots_adjust(bottom=0.2)

        # candlestick chart type
        # use mplfinance
        
        # mpf.plot(data,type='candle')

        # add details on axis and title
        
        ax.xaxis_date()
        ax.autoscale_view()
        plt.show()
        
        
        
        
### class using histdata.api     



### lmdb 

# adopt lmdb to reduce memory occupied by data in local folder
# and to speed read/write operations when using data 
