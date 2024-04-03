
import logging

from attrs import ( 
                    define,
                    field,
                    Factory,
                    validate,
                    validators
                )

# PANDAS
from pandas import (
                    DataFrame as pandas_dataframe,
                    read_csv as pandas_read_csv
    )

# PYARROW
from pyarrow import (
                    BufferReader
    )

# POLARS
from polars import (
                    String as polars_string,
                    col,
                    read_csv as polars_read_csv
    )

from polars.dataframe import ( 
                    DataFrame as polars_dataframe
    )

from datetime import datetime

from zipfile import (
                    ZipFile,
                    BadZipFile
                )

from re import (
                findall,
                search
            )

from mplfinance import (
                        plot as mpf_plot,
                        show as mpf_show
                    )

from re import (
                search
            )

from numpy import array

from pathlib import Path
from requests import Session
from io import BytesIO
from dask import dataframe as dd

from dotty_dict import (Dotty, 
                        dotty
                    )

# internally defined
from .common import *
from ..config import read_config_file


__all__ = ['historical_manager']


    

# HISTORICAL DATA MANAGER
@define(kw_only=True, slots=True)
class historical_manager:
    
    # interface parameters
    config_file     : str = field(default=None,
                                  validator=validators.instance_of(str))
    ticker          : str = field(default=None,
                                  validator=validators.instance_of(str))
    data_filetype   : str = field(default='parquet',
                                  validator=validators.in_(SUPPORTED_DATA_FILES))
    data_path       : str = field(default=Path(DEFAULT_PATHS.HIST_DATA_PATH),
                              validator=validator_dir_path)
    engine          : str = field(default='pandas',
                                  validator=validators.in_(SUPPORTED_DATA_ENGINES))
    
    
    # internal parameters
    _db_dict = field(factory=dotty, validator=validators.instance_of(Dotty))
    _years   = field(factory=list, validator=validators.instance_of(list))
    _tf_list = field(factory=list, validator=validators.instance_of(list))
    _dataframe_type = field(default=pandas_dataframe)
    
    # if a valid config file is passed
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
        
        if kwargs['config_file']:
            
            self.config_file = kwargs['config_file']
            config_path = Path(kwargs['config_file'])
            if config_path.exists() \
                and  \
                config_path.is_file() \
                and  \
                config_path.suffix == '.yaml':
                
                # read parameters from config file 
                # and force keys to lower case
                config_args = {key.lower(): val for key, val in 
                               read_config_file(config_path.absolute()).items()
                               }
                
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
                        
                        logging.warning('KeyError: initializing object has no '
                                        f'attribute {attr.name}')
                        
                    except IndexError:
                        
                        logging.warning('IndexError: initializing object has no '
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
                
                raise ValueError('invalid config_file')
                        
        else:
            
            # no config file is defined
            # call generated init 
            self.__attrs_init__(**kwargs)
            
        validate(self)
        
        self.__attrs_post_init__()
        
            
    def __attrs_post_init__(self):
        
        # Fundamentals parameters initialization
        self.ticker = self.ticker.upper()
        
        # files details variable initialization
        self.data_path = Path(self.data_path)
        
        if ( 
            not self.data_path.is_dir() 
            or
            not self.data_path.exists()
            ):
                    
            self.data_path.mkdir(parents=True,
                                 exist_ok=True)
        
        if self.engine == 'pandas':
            
            self._dataframe_type = pandas_dataframe 

        elif self.engine == 'polars':
            
            self._dataframe_type = polars_dataframe
            
        
    def _db_key(self, ticker, year, timeframe, data_type):
        """

        get a str key of dotted divided elements

        key template = ticker.year.timeframe.data_type

        Parameters
        ----------
        ticker : TYPE
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

        return '.'.join([str(ticker), 'Y'+str(year),
                         str(tf), str(data_type)])


    def _get_years_list(self, ticker, vartype):

        # work on copy as pop operation is 'inplace'
        # so the original db is not modified
        db_copy = self._db_dict.copy()

        # get keys at year level
        years_filter_keys = '{ticker}'.format(ticker=self.ticker)

        # pop at year level in data copy
        year_db = db_copy.pop(years_filter_keys)

        if year_db:

            try:
                years_keys = year_db.keys()
            except KeyError:
                # no active year found --> return empty list
                return []
            else:

                # get year value from data keys
                years_list = [key[FILENAME_TEMPLATE.YEAR_NUMERICAL_CHAR:]
                              for key in years_keys]
                
                # remove duplicates
                years_list = list_remove_duplicates(years_list)

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


    def _get_year_timeframe_list(self, ticker, year):

        # work on copy as pop operation is 'inplace'
        # so the original db is not modified
        db_copy = self._db_dict.get(self.ticker).copy()

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


    def _get_tf_complete_years(self):

        # check input years list if each year is complete
        # across tick and timeframes requested

        # instantiate empty list
        years_complete = list()

        for year in self._get_years_list(self.ticker, 'int'):

            year_complete = all([
                # create key for dataframe type
                isinstance(self._db_dict.get(self._db_key(self.ticker,
                                                            year,
                                                            tf,
                                                            'df')),
                           self._dataframe_type)
                for tf in self._tf_list
            ])

            if year_complete:

                # append year in list of data found in local folder
                years_complete.append(year)

        return years_complete


    def _download_month_raw(self, url, year, month_num):
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

        session = Session()
        r = session.get(url)
        tk = search('id="tk" value="(.*?)"', r.text).groups()[0]
        
        headers = {'Referer': url}
        data = {'tk': tk, 
                'date': year,
                'datemonth': "%d%02d" % (year, month_num), 
                'platform': 'ASCII',
                'timeframe': 'T', 
                'fxpair': self.ticker
            }
        
        r = session.request(HISTDATA_BASE_DOWNLOAD_METHOD,
                            HISTDATA_BASE_DOWNLOAD_URL,
                            data=data,
                            headers=headers,
                            stream=True
                        )
        
        bio = BytesIO()
    
        logging.warning("Starting to download: %s - %d - %s" 
                         % (self.ticker,
                            year,
                            MONTHS[month_num-1])
        )
            
        # write content to stream
        bio.write(r.content)
        
        print(flush=True)
        try:
            
            zf = ZipFile(bio)
            
        except BadZipFile:

            # here will be a warning log
            logging.error('%s - %d - %s not found or invalid download'
                            %  (self.ticker,
                                year,
                                MONTHS[month_num-1])
            )
            
            return None

        else:

            # return opened zip file
            return zf.open(zf.namelist()[0])


    def _add_tf_data_key(self, year, tf):
        
        year_tf_key = self._db_key(self.ticker,
                                  year,
                                  tf,
                                  'df')

        if self._db_dict.get(year_tf_key) is None \
            or not isinstance(self._db_dict.get(year_tf_key),
                              self._dataframe_type):

            # get tick key
            year_tick_key = self._db_key(self.ticker,
                                        year,
                                        'TICK',
                                        'df')

            try:

                aux_base_df = self._db_dict.get(year_tick_key)

            except KeyError:

                # to logging
                logging.error(f'Requested to reframe {self.ticker} '
                              f'{year} in timeframe {tf} '
                              f'but tick data was not found')

            else:

                # produce reframed data at the timeframe requested
                self._db_dict[year_tf_key] \
                    = reframe_data(aux_base_df, tf)
        

    def _complete_years_timeframe(self):

        # get all years available from db keys
        years_list = self._get_years_list(self.ticker, 'int')

        # get years that has not all timeframes
        years_complete = self._get_tf_complete_years()

        # get years not having all timeframes data
        years_incomplete = set(years_list).difference(years_complete)

        # get years missing timeframes data but with tick data available
        # in current data instance (no further search offline)
        incomplete_with_tick = [int(get_dotty_key_field(key, 
                                                    DATA_KEY.YEAR_INDEX)[1:])
                                for key in get_dotty_leafs(self._db_dict)
                                if get_dotty_key_field(key, DATA_KEY.TF_INDEX)
                                == TICK_TIMEFRAME
                                and
                                int(get_dotty_key_field(key, 
                                                    DATA_KEY.YEAR_INDEX)[1:])
                                in years_incomplete
                                ]

        aux_base_df = self._dataframe_type()

        # complete years reframing from tick/minimal timeframe data
        for year in incomplete_with_tick:

            for tf in self._tf_list:

                self._add_tf_data_key(year, tf)

        # assert no incomplete years found after operation
        assert self._get_years_list(self.ticker, 'int') \
            == self._get_tf_complete_years(), \
            'timeframe completing operation NOT OK'


    def _raw_zipfile_to_df(self, 
                           raw_file,
                           temp_filepath,
                           engine='polars'):
        """


        Parameters
        ----------
        raw_files_list : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        None.

        """

        if engine == 'pandas':
        
            # pandas with python engine can read a runtime opened
            # zip file
        
            # funtions is specific for format of files downloaded
            # parse file passed as input
            df = read_csv(  
                    raw_file,
                    sep=',',
                    names=DATA_COLUMN_NAMES.TICK_DATA,
                    dtype=DTYPE_DICT.TICK_DTYPE,
                    parse_dates=[DATA_FILE_COLUMN_INDEX.TIMESTAMP],
                    date_format=DATE_FORMAT_HISTDATA_CSV,
                    engine = 'python'
            )
            
            # calculate 'p'
            df['p'] = (df['ask'] + df['bid']) / 2
            
        elif engine == 'pyarrow':
        
            # no way found to directly open a runtime zip file
            # with pyarrow
            # strategy rolls back to temporary file download 
            # open and read all
            # delete temporary file
            
            # alternative using pyarrow
            buf = BufferReader(raw_file.read())
            tempdir_path = Path(temp_filepath).parent
            # create temperary files directory if not present
            tempdir_path.mkdir(exist_ok=True)
            # download buffer to file
            buf.download(temp_filepath)
            
            # from histdata raw files column 'p' is not present
            raw_file_dtypes = DTYPE_DICT.TICK_DTYPE.copy()
            raw_file_dtypes.pop('p')
                        
            # read temporary csv file
            df = read_csv(  temp_filepath,
                            sep=',',
                            index_col=0,
                            names=DATA_COLUMN_NAMES.TICK_DATA,
                            dtype=raw_file_dtypes,
                            parse_dates=[0],
                            date_format=DATE_FORMAT_HISTDATA_CSV,
                            engine = 'pyarrow'
            )
            
            # perform step to covnert index
            # into a datetime64 dtype
            df.index = any_date_to_datetime64(df.index,
                                    date_format=DATE_FORMAT_HISTDATA_CSV,
                                    unit='ms')
                
            # calculate 'p'
            df['p'] = (df['ask'] + df['bid']) / 2
            
        elif engine == 'polars':
            
            # download to temporary csv file 
            # for best performance with polars
            
            # alternative using pyarrow
            buf = BufferReader(raw_file.read())
            tempdir_path = Path(temp_filepath).parent
            # create temperary files directory if not present
            tempdir_path.mkdir(exist_ok=True)
            # download buffer to file
            buf.download(temp_filepath)
            
            # from histdata raw files column 'p' is not present
            raw_file_dtypes = POLARS_DTYPE_DICT.TIME_TICK_DTYPE.copy()
            raw_file_dtypes.pop('p')
            raw_file_dtypes[BASE_DATA_COLUMN_NAME.TIMESTAMP] = polars_string
            
            # read file
            # set schema for columns but avoid timestamp columns
            df = polars_read_csv(temp_filepath,
                                 separator  = ',',
                                 has_header = False,
                                 new_columns = DATA_COLUMN_NAMES.TICK_DATA,
                                 schema      = raw_file_dtypes,
                                 use_pyarrow = True
            )
            
            # check schema, recall cast if needed
            if not dict(df.schema) == raw_file_dtypes:
                df = df.cast(raw_file_dtypes)
            
            # convert timestamp column to datetime data type
            df = df.with_columns(
                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).str.strptime(
                        polars_datetime('ms'), 
                        format=DATE_FORMAT_HISTDATA_CSV
                    )
                )
    
            # calculate 'p'
            df = df.with_columns(
                    ( (col('ask') + col('bid')) / 2).alias('p') 
                 ) 
        
        else:
            
            raise TypeError(f'Engine {engine} is not supported')
            
        return df


    def _year_data_to_file(self, year, tf=None, engine='polars'):
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

        ticker_path = self.data_path / self.ticker
        if not ticker_path.is_dir() or not ticker_path.exists():
            ticker_path.mkdir(parents=True, exist_ok=False)

        year_path = ticker_path / str(year).upper()
        if not year_path.is_dir() or not year_path.exists():
            year_path.mkdir(parents=True, exist_ok=False)

        # alternative: get year by referenced key
        year_tf_key = self._db_key(self.ticker, year, tf, 'df')
        assert isinstance(self._db_dict.get(year_tf_key), self._dataframe_type), \
            'key %s is no valid DataFrame' % (year_tf_key)
        year_data = self._db_dict.get(year_tf_key)

        filepath = year_path / self._get_filename(self.ticker, year, tf)

        if filepath.exists() and filepath.is_file():
            logging.info("File {} already exists".format(filepath))
            return

        if self.data_filetype == DATA_FILE_TYPE.CSV_FILETYPE:
            
            # csv data files
            
            # IMPORTANT 
            # pandas dataframe case
            # avoid date_format parameter since it is reported that
            # it makes to_csv to be excessively long with column data
            # being datetime data type
            # see: https://github.com/pandas-dev/pandas/issues/37484
            #      https://stackoverflow.com/questions/65903287/pandas-1-2-1-to-csv-performance-with-datetime-as-the-index-and-setting-date-form
            
            write_csv(year_data, filepath)
            
            
        elif self.data_filetype == DATA_FILE_TYPE.PARQUET_FILETYPE:
            
            # parquet data files
            write_parquet(year_data, str(filepath.absolute()))
            

    def _get_file_details(self, filename):

        assert isinstance(filename, str), 'filename type must be str'

        # get years available in offline data (local disk)
        filename_details = filename.replace('_', '.').split(sep='.')

        # store each file details in local variables
        file_ticker = filename_details[FILENAME_TEMPLATE.PAIR_INDEX]
        file_year = int(
            filename_details[FILENAME_TEMPLATE.YEAR_INDEX][FILENAME_TEMPLATE.YEAR_NUMERICAL_CHAR:])
        file_tf = filename_details[FILENAME_TEMPLATE.TF_INDEX]

        # return each file details
        return file_ticker, file_year, file_tf


    def _get_filename(self, ticker, year, tf):

        # based on standard filename template
        return FILENAME_STR.format(ticker=self.ticker,
                                   year=year,
                                   tf=tf,
                                   file_ext=self.data_filetype)


    def _update_local_data_folder(self):

        # get active years loaded on db manager
        years_list = self._get_years_list(self.ticker, 'int')

        # get file names in local folder
        _, local_files_name = self._list_local_data()

        # loop through years
        for year in years_list:

            year_tf_list = self._get_year_timeframe_list(self.ticker, year)

            # loop through timeframes loaded
            for tf in year_tf_list:

                tf_filename = self._get_filename(self.ticker,
                                                 year,
                                                 tf)

                tf_key = self._db_key(self.ticker, year, tf, 'df')

                # check if file is present in local data folder
                # and if valid dataframe is currently loaded in database
                if tf_filename not in local_files_name \
                   and isinstance(self._db_dict.get(tf_key),
                                  self._dataframe_type):

                    self._year_data_to_file(year,
                                            tf=tf,
                                            engine=self.engine)


    def _download_year(self, year):

        year_tick_df = self._dataframe_type()        

        for month in MONTHS:

            month_num = MONTHS.index(month) + 1
            url = HISTDATA_URL_TICKDATA_TEMPLATE.format(
                                        ticker=self.ticker,
                                        year=year,
                                        month_num=month_num)
            
            file = self._download_month_raw(url, year, month_num)
            
            if file:
                
                month_data = self._raw_zipfile_to_df(file,
                                                     str(self.data_path 
                                                         / TEMP_FOLDER
                                                         / TEMP_CSV_FILE),
                                                     engine = self.engine
                )
                 
                year_tick_df = concat_data([year_tick_df, month_data])
                
                    
        (self.data_path / TEMP_FOLDER / TEMP_CSV_FILE).unlink(missing_ok=True)
            
        return year_tick_df


    def _download(self,
                 years,
                 search_local=False):

        # assert on years req
        assert isinstance(years, list), \
            'years {} invalid, must be list type'.format(years)

        assert set(years).issubset(YEARS), \
            'YEARS requested must contained in available years'

        # convert to list of int
        if not all(isinstance(year, int) for year in years):
            years = [int(year) for year in years]

        # search if years data are already available offline
        if search_local:
            years_tickdata_offline = self._local_load_data(years, 
                                                           engine=self.engine)
        else:
            years_tickdata_offline = list()

        # years not found on local offline path
        # must be downloaded from the net
        tick_years_to_download = set(years).difference(years_tickdata_offline)

        if tick_years_to_download:

            for year in tick_years_to_download:

                year_tick_df = self._download_year(year)

                # get key for dotty dict: TICK
                year_tick_key = self._db_key(self.ticker, year, 'TICK', 'df')
                self._db_dict[year_tick_key] = year_tick_df

        # update manager database
        self._update_db()
        

    def _list_local_data(self):

        local_files = []
        local_files_name = []
        
        # prepare predefined path and check if exists
        tickerfolder_path = Path(self.data_path) / self.ticker
        if not (
            tickerfolder_path.exists() \
            and 
            tickerfolder_path.is_dir()
        ):
            
            tickerfolder_path.mkdir(parents=True, 
                                    exist_ok=False)
            
        else:
            
            # list all specifed ticker data files in folder path and subdirs
            local_files = list(tickerfolder_path.glob(
                f'**/{self.ticker}_*.{self.data_filetype}'
                )
            )
            local_files_name = [file.name for file in local_files]

            # check compliance of files to convention (see notes)
            # TODO: warning if no compliant and filter out from files found

        return local_files, local_files_name


    def _local_load_data(self, years_list, engine='polars'):
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

        # list data available in local folder
        local_files, local_files_name = self._list_local_data()

        years_tick_files_found = list()

        # parse files and fill details list
        for file in local_files:

            # get years available in offline data (local disk)
            local_filepath_key = file.name.replace('_', '.')

            # get file details
            file_ticker, file_year, file_tf = self._get_file_details(file.name)

            # check at timeframe index file has a valid timeframe
            if check_timeframe_str(file_tf) == file_tf:

                # create key for dataframe type
                year_tf_key = self._db_key(file_ticker,
                                          file_year,
                                          file_tf,
                                          'df')

                # check if year is needed to be loaded
                if file_ticker == self.ticker \
                        and (int(file_year) in years_list):

                    if file_tf == TICK_TIMEFRAME:
                        years_tick_files_found.append(file_year)

                    if self.data_filetype == DATA_FILE_TYPE.CSV_FILETYPE:
                        
                        if engine == 'pandas':
                            
                            # year requested: upload tick file if not already present
                            if file_tf == TICK_TIMEFRAME \
                                    and self._db_dict.get(year_tf_key) is None:
        
                                # perform tick file upload
        
                                # use dask library as tick csv files can be very large
                                # time gain is significative even compared to using
                                # pandas read_csv with chunksize tuning
                                dask_df = dd.read_csv(file,
                                                      sep=',',
                                                      header=0,
                                                      dtype=DTYPE_DICT.TICK_DTYPE,
                                                      parse_dates=['timestamp'],
                                                      date_format=DATE_FORMAT_ISO8601)
        
                                self._db_dict[year_tf_key] = \
                                    dask_df.compute().set_index('timestamp')
        
                            # year requested: upload tf file if not already present
                            #                 and tf is requested
                            elif self._db_dict.get(year_tf_key) is None \
                                    and file_tf in self._tf_list:
        
                                self._db_dict[year_tf_key] = \
                                    read_csv(
                                    file,
                                    sep=',',
                                    header=0,
                                    dtype=DTYPE_DICT.TF_DTYPE,
                                    index_col='timestamp',
                                    parse_dates=[
                                        'timestamp'],
                                    date_format=DATE_FORMAT_ISO8601)
                                
                        elif engine == 'pyarrow':
                            
                            read_csv('pyarrow', file, separator=',')
                            
                        elif engine == 'polars':
                            
                            read_csv('polars', file)
                            
                        else:
                            
                            raise TypeError('')
                    
                    elif self.data_filetype == DATA_FILE_TYPE.PARQUET_FILETYPE:
                    
                        self._db_dict[year_tf_key] = read_parquet(engine,
                                                                  file)
                            
                        
        # return list of years which tick file has been found and loaded
        return years_tick_files_found
    

    def _update_db(self):

        # complete year keys along timeframes required
        self._complete_years_timeframe()

        # dump new downloaded data not already present in local data folder
        self._update_local_data_folder()


    def add_timeframe(self, timeframe, update_data=False):

        if not hasattr(self, '_tf_list'):
            self._tf_list = []

        if isinstance(timeframe, str):

            timeframe = [timeframe]

        assert isinstance(timeframe, list) \
            and all([isinstance(tf, str) for tf in timeframe]), \
            'timeframe invalid: str or list required'

        tf_list = [check_timeframe_str(tf) for tf in timeframe]

        if not set(tf_list).issubset(self._tf_list):

            # concat timeframe accordingly
            # only just new elements not already present
            self._tf_list.extend(set(tf_list).difference(self._tf_list))

            if update_data:

                self._update_db()


    def get_data(self,
                timeframe,
                start,
                end,
                add_timeframe = True):
        
        """
        
        Main function to get data, considering data currently managed
        in instance, then searchin in local folders and ultimately
        recurring to web download.

        Parameters
        ----------
        timeframe : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        add_timeframe : TYPE, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        data_df : TYPE
            DESCRIPTION.

        """

        assert check_time_offset_str(timeframe), \
            f'timeframe request {timeframe} invalid'
            
        # try to convert to datetime data type if not already is
        if self.engine == 'polars':
            
            start = any_date_to_datetime64(start,
                                           to_pydatetime=True)
            
            end = any_date_to_datetime64(end, 
                                         to_pydatetime=True)
            
        else:
            
            start = any_date_to_datetime64(start)
            end = any_date_to_datetime64(end)

        assert end > start, \
            'date interval not coherent, start must be older than end'

        if not timeframe in self._tf_list \
            and add_timeframe:

            # timeframe list
            self.add_timeframe([timeframe])

        # get years including interval requested
        years_interval_req = list(range(start.year, end.year+1, 1))
        
        # get data keys referred to interval years 
        # at the given timeframe
        interval_keys = [key for key in get_dotty_leafs(self._db_dict)
                          if int(get_dotty_key_field(key, 
                                                     DATA_KEY.YEAR_INDEX)[1:])
                              in years_interval_req \
                              and get_dotty_key_field(key, 
                                                      DATA_KEY.TF_INDEX) 
                                  == timeframe]
        
        # get years covered by interval keys
        interval_keys_years = [int(get_dotty_key_field(key, 
                                                       DATA_KEY.YEAR_INDEX)[1:])
                               for key in interval_keys]
        
        # aggregate data to current instance if necessary
        if years_interval_req != interval_keys_years:
            
            year_tf_missing = list(set(years_interval_req).difference(interval_keys_years))
            
            year_tick_keys = [int(get_dotty_key_field(key, 
                                                      DATA_KEY.YEAR_INDEX)[1:])
                              for key in get_dotty_leafs(self._db_dict)
                                if get_dotty_key_field(key, 
                                                       DATA_KEY.TF_INDEX) \
                                    == TICK_TIMEFRAME ]
            
            year_tick_missing = list(set(years_interval_req).difference(year_tick_keys))
            
            # if tick is missing --> download missing years
            if year_tick_missing:
                
                self._download(year_tick_missing, 
                               search_local=True)
                
            # if timeframe req is in tf_list 
            # data requested should at this point be available
            # call add data for specific timeframe requested 
            if not timeframe in self._tf_list:
                
                for year in year_tf_missing:
                    
                    # call add single tf data
                    self._add_tf_data_key(year, timeframe)
                    
                    
            # get years covered by interval keys
            interval_keys_years = [int(get_dotty_key_field(key, DATA_KEY.YEAR_INDEX)[1:]) 
                                   for key in get_dotty_leafs(self._db_dict)
                                   if int(get_dotty_key_field(key, DATA_KEY.YEAR_INDEX)[1:])
                                       in years_interval_req and \
                                        get_dotty_key_field(key, 
                                                            DATA_KEY.TF_INDEX) \
                                        == timeframe]
            
            assert years_interval_req == interval_keys_years, \
                    f'processing year data completion for {years_interval_req} ' \
                    'not ok'
                    
        # at this point data keys necessary are completed
        
        # get data keys referred to interval years 
        # at the given timeframe
        interval_keys = [key for key in get_dotty_leafs(self._db_dict)
                          if int(get_dotty_key_field(key, DATA_KEY.YEAR_INDEX)[1:])
                              in years_interval_req \
                              and get_dotty_key_field(key, DATA_KEY.TF_INDEX) \
                                  == timeframe]
            
        data_df = self._dataframe_type()
        
        if len(interval_keys) == 1: 

            data_df = self._db_dict.get(interval_keys[0])

            # return data slice
            if self.engine == 'pandas':
            
                data_df = data_df[(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP]
                                   >= start)
                                  & 
                                  (data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP]
                                   <= end)].copy()
            
            elif self.engine == 'polars':
                
                data_df = \
                    \
                    (
                        data_df
                        .filter(
                            col(BASE_DATA_COLUMN_NAME.TIMESTAMP).is_between(start,
                                                                            end   
                            )
                        ).clone()
                    )
            
            else:
                
                raise TypeError(f'engine {self.engine} not supported'
                                ' for get_data function')
            
        else:

            # order keys by ascending year value
            interval_keys.sort(key=lambda x: 
                               int(get_dotty_key_field(x, 
                                                       DATA_KEY.YEAR_INDEX)[1:])
                               )

                
            # TODO: need to differentiate dataframe slicing filterin and
            #       selection depending on engine used
                
            # get data interval
            
            if self.engine == 'pandas':
                
                data_df = concat_data([self._db_dict.get(key)[
                              (self._db_dict.get(key)[BASE_DATA_COLUMN_NAME.TIMESTAMP] 
                                   >= start)
                              & (self._db_dict.get(key)[BASE_DATA_COLUMN_NAME.TIMESTAMP] 
                                   <= end)].copy()
                            for key in interval_keys]
                )
                
            elif self.engine == 'polars':
            
                # slice data using polars sintax:
                    
                data_df = \
                    \
                    concat_data(
                        [
                            (
                                self._db_dict.get(key)
                                .filter(
                                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).is_between(start,
                                                                                    end   
                                    )
                                ).clone()
                            )
                            for key in interval_keys
                        ]
                    )
                    
            else:
                
                raise TypeError(f'engine {self.engine} not supported'
                                ' for get_data function')
            
    
        return data_df
    

    def plot(self, timeframe, start_date, end_date):
        """
        Plot data in selected time frame and start and end date bound
        :param date_bounds: start and end of plot
        :param timeframe: timeframe to visualize
        :return: void
        """

        logging.info(f'Chart request: from {start_date} '
                     f'to {end_date} with timeframe {timeframe}')
        
        chart_data = self.get_data(timeframe     = timeframe,
                                   start         = start_date,
                                   end           = end_date,
                                   add_timeframe = True)

        chart_data = to_pandas_dataframe(chart_data)
        
        if chart_data.index.name != BASE_DATA_COLUMN_NAME.TIMESTAMP:
            
            chart_data.set_index(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                 inplace=True)
            
            chart_data.index = to_datetime(chart_data.index)
        
        # candlestick chart type
        # use mplfinance
        chart_kwargs = dict(style    = 'charles',
                            volume   = False,
                            figratio = (10,8),
                            figscale = 1
        )
        
        mpf_plot(chart_data,type='candle',**chart_kwargs)

        mpf_show()
