
from loguru import logger

from attrs import ( 
                define,
                field,
                validate,
                validators
    )

# PANDAS
from pandas import (
                DataFrame as pandas_dataframe,
                to_datetime
    )

# PYARROW
from pyarrow import (
                int64 as pyarrow_int64,
                string as pyarrow_string,
                BufferReader,
                csv as arrow_csv,
                compute as pc,
                schema,
                Table,
                table as pyarrow_table,
                duration
    )

# POLARS
from polars import (
                String as polars_string,
                col,
                DataFrame as polars_dataframe,
                LazyFrame as polars_lazyframe
    )

from zipfile import (
                ZipFile,
                BadZipFile
    )

from re import (
                search,
                match
    )

from mplfinance import (
                plot as mpf_plot,
                show as mpf_show
    )

from numpy import array

from pathlib import Path
from requests import Session
from io import BytesIO
from shutil import rmtree
from datetime import datetime

from dotty_dict import (
                Dotty, 
                dotty
    )

from iteration_utilities import (
                duplicates,
                unique_everseen
    )

# internally defined
from .common import *
from ..config import ( 
        read_config_file,
        read_config_string,
        read_config_folder
    )

from .database import (
        DatabaseConnector,
        DuckDBConnector,
        LocalDBConnector
    )


__all__ = ['historical_manager']

        
# HISTORICAL DATA MANAGER
@define(kw_only=True, slots=True)
class historical_manager_db:
    
    # interface parameters
    config          : str = field(default='',
                                  validator=validators.instance_of(str))
    data_type       : str = field(default='parquet',
                                  validator=validators.in_(SUPPORTED_DATA_FILES))
    engine          : str = field(default='polars_lazy',
                                  validator=validators.in_(SUPPORTED_DATA_ENGINES))
    
    # internal
    _db_connector = field(factory=DatabaseConnector)
    _tf_list = field(factory=list, validator=validators.instance_of(list))
    _dataframe_type = field(default = pandas_dataframe)
    _data_path = field(default = Path(DEFAULT_PATHS.BASE_PATH), 
                           validator=validator_dir_path(create_if_missing=True))
    _histdata_path = field(
                        default = Path(DEFAULT_PATHS.BASE_PATH) / DEFAULT_PATHS.HIST_DATA_FOLDER, 
                        validator=validator_dir_path(create_if_missing=True))
    _temporary_data_path = field(
                        default = (Path(DEFAULT_PATHS.BASE_PATH) 
                                    / DEFAULT_PATHS.HIST_DATA_FOLDER
                                    / TEMP_FOLDER), 
                        validator=validator_dir_path(create_if_missing=True))
    
    # if a valid config file or string
    # is passed
    # arguments contained are assigned here 
    # if instantiation passed values are present
    # they will override the related argument
    # value in the next initialization step
    
    # if neither by instantation or config file
    # an argument value is set, the argument
    # will be set by asociated defined default 
    # or factory generator
        
    def __init__(self, **kwargs):
            
        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)
        
        if 'config' in kwargs.keys():
        
            if kwargs['config']:
            
                config_path = Path(kwargs['config'])
                
                if (
                    config_path.exists() 
                    and  
                    config_path.is_dir() 
                    ):
                    
                    config_filepath = read_config_folder(config_path,
                                                         file_pattern='data_config.yaml')
                
                else:
                    
                    config_filepath = Path()
                    
                config_args = {}
                if config_filepath.exists() \
                    and  \
                    config_filepath.is_file() \
                    and  \
                    config_filepath.suffix == '.yaml':
                    
                    # read parameters from config file 
                    # and force keys to lower case
                    config_args = {key.lower(): val for key, val in 
                                   read_config_file(str(config_filepath)).items()}
                
                elif isinstance(kwargs['config'], str):
                    
                    # read parameters from config file 
                    # and force keys to lower case
                    config_args = {key.lower(): val for key, val in 
                                   read_config_string(kwargs['config']).items()}
                    
                else:
                
                    logger.critical('invalid config type '
                                    f'{kwargs["config"]}: '
                                    'required str or Path, got '
                                    f'{type(kwargs["config"])}')
                    raise TypeError
                            
                # check consistency of config_args
                if  (
                        not isinstance(config_args, dict)
                        or
                        not bool(config_args)
                    ):
                    
                    logger.critical(f'config {kwargs["config"]} '
                                     'has no valid yaml formatted data')
                    raise TypeError
                
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
                        
                        logger.warning('KeyError: initializing object has no '
                                        f'attribute {attr.name}')
                        
                    except IndexError:
                        
                        logger.warning('IndexError: initializing object has no '
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
            
            # no config file is defined
            # call generated init 
            self.__attrs_init__(**kwargs)
            
        validate(self)
        
        self.__attrs_post_init__()
    
    
    def __attrs_post_init__(self, **kwargs):
        
        # reset logging handlers
        logger.remove()
        
        # add logging file handle
        logger.add(self._data_path / 'forexdata.log', 
                   level="TRACE",
                   rotation="5 MB"
        )
        
        # set up dataframe engine internal var based on config selection
        if self.engine == 'pandas':
            
            self._dataframe_type = pandas_dataframe 

        elif self.engine == 'pyarrow':
            
            self._dataframe_type = pyarrow_table 
            
        elif self.engine == 'polars':
            
            self._dataframe_type = polars_dataframe 
        
        elif self.engine == 'polars_lazy':

            self._dataframe_type = polars_lazyframe  
            
        self._temporary_data_path = self._histdata_path \
                                        / TEMP_FOLDER
                                        
        self._clear_temporary_data_folder()
        
        # instance database connector if selected
        if self.data_type == DATA_TYPE.DUCKDB:
            
            self._db_connector = DuckDBConnector(duckdb_filepath =
                                                 str(self._histdata_path 
                                                     / 'DuckDB'
                                                     / 'marketdata.duckdb')
                                                )
            
        elif (
                self.data_type == DATA_TYPE.CSV_FILETYPE
                or
                self.data_type == DATA_TYPE.PARQUET_FILETYPE
            ):
        
            self._db_connector = \
                LocalDBConnector(
                    local_data_folder = str(self._histdata_path 
                                            / 'LocalDB'),
                    data_type         = self.data_type,
                    engine            = self.engine
                )
            
        
    def _clear_temporary_data_folder(self):
        
        # delete temporary data path
        if (
            self._temporary_data_path.exists()
            and
            self._temporary_data_path.is_dir()
            ):
            
            try:
                
                rmtree(str(self._temporary_data_path))
                
            except Exception as e:
                
                logger.warning('Deleting temporary data folder '
                               f'{str(self._temporary_data_path)} not successfull: {e}')
                 
                 
    def _get_ticker_list(self):
        
        # return list of tickers elements as str
        
        return self._db_connector.get_tickers_list()
    
    
    def _get_ticker_keys(self, ticker, timeframe=None):
        
        # return list of ticker keys elements as str
        
        return self._db_connector.get_ticker_keys(ticker,
                                                  timeframe = timeframe)
    
    
    def _get_ticker_years_list(self, ticker, timeframe=TICK_TIMEFRAME):
        
        # return list of ticker years covered in data elements as str
        # if timeframe is None means years in data in tick or 1m timeframe
        
        return self._db_connector.get_ticker_years_list(ticker,
                                                        timeframe = timeframe)
            
    
    def _complete_timeframe(self) -> None:

        for ticker in self._get_ticker_list():
            
            years_tick = self._get_ticker_years_list(ticker)

            for tf in self._tf_list:
                
                ticker_years_list = self._get_ticker_years_list(ticker, timeframe=tf)
            
                if set(years_tick).difference(ticker_years_list):
                    years = set(years_tick).difference(ticker_years_list)
                    
                    end_year = max(years)
                    start_year = min(years)
                    
                    year_start = f'{start_year}-01-01 00:00:00.000'
                    year_end = f'{end_year+1}-01-01 00:00:00.000'
                    # read missing years from tick timeframe
                    start = datetime.strptime(year_start, DATE_FORMAT_SQL)
                    end = datetime.strptime(year_end, DATE_FORMAT_SQL)
                    
                    dataframe = self._db_connector.read_data(
                                    market = 'forex',
                                    ticker = ticker,
                                    timeframe = TICK_TIMEFRAME,
                                    start = start,
                                    end = end
                    )
                    
                    # reframe to timeframe
                    dataframe_tf = reframe_data(dataframe, tf)
                    
                    # get key for dotty dict: TICK
                    tf_key = self._db_connector._db_key(
                                            'forex', 
                                            ticker,
                                            tf
                            )
                    
                    # write to database to complete the years 
                    # call to upload df to database
                    self._db_connector.write_data(tf_key,
                                                  dataframe_tf) 
                
                ticker_years_list = self._get_ticker_years_list(ticker, 
                                                                timeframe=tf)
                
                # REDO THE CHECK FOR CONSISTENCY
                if set(years_tick).difference(ticker_years_list):
                    
                    logger.critical(f'ticker {ticker}: {tf} timeframe completing'
                                    ' operation FAILED')
                    
                    raise KeyError
                
                
    def _update_db(self):
        
        self._complete_timeframe()
    
        
    def _download_month_raw(self,
                            ticker,
                            url, 
                            year, 
                            month_num
        ):
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
        
        with logger.catch(exception=AttributeError,
                          level='CRITICAL',
                          message=f'token value was not found scraping '
                                  f'url {url}: probably {ticker} '
                                  f'is not supported by histdata.com'):
            
            token = search('id="tk" value="(.*?)"', r.text).groups()[0]

        ''' Alternative: using BeautifulSoup parser
        r = session.get(url, allow_redirects=True)
        soup = BeautifulSoup(r.content, 'html.parser')
        
        with logger.catch(exception=AttributeError,
                          level='CRITICAL',
                          message=f'token value was not found scraping url {url}'):
            
            token = soup.find('input', {'id': 'tk'}).attrs['value']
        
        '''
        
        headers = {'Referer': url}
        data = {
            'tk': token, 
            'date': year,
            'datemonth': "%d%02d" % (year, month_num), 
            'platform': 'ASCII',
            'timeframe': 'T', 
            'fxpair': ticker
        }
            
        r = session.request(
            HISTDATA_BASE_DOWNLOAD_METHOD,
            HISTDATA_BASE_DOWNLOAD_URL,
            data=data,
            headers=headers,
            stream=True
        )
        
        bio = BytesIO()
          
        # write content to stream
        bio.write(r.content)
        
        try:
            
            zf = ZipFile(bio)
            
        except BadZipFile:

            # here will be a warning log
            logger.error(f'{ticker} - {year} - {MONTHS[month_num-1]}: '
                         f'not found or invalid download')
                            
            return None

        else:

            # return opened zip file
            return zf.open(zf.namelist()[0])
        
        
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
                    'pandas',
                    raw_file,
                    sep=',',
                    names=DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                    dtype=DTYPE_DICT.TICK_DTYPE,
                    parse_dates=[DATA_FILE_COLUMN_INDEX.TIMESTAMP],
                    date_format=DATE_FORMAT_HISTDATA_CSV,
                    engine = 'c'
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
            
            if (
                Path(temp_filepath).exists()
                and
                Path(temp_filepath).is_file()
                ):
                
                Path(temp_filepath).unlink(missing_ok=True)    
                
            else:
                
                # create temporary files directory if not present
                tempdir_path = Path(temp_filepath).parent
                tempdir_path.mkdir(exist_ok=True)
                
            # download buffer to file
            buf.download(temp_filepath)
            
            # from histdata raw files column 'p' is not present
            # raw_file_dtypes = DTYPE_DICT.TICK_DTYPE.copy()
            # raw_file_dtypes.pop('p')
                        
            # read temporary csv file
            
            # use panda read_csv an its options with 
            # engine = 'pyarrow'
            # dtype_backend = 'pyarrow'
            # df = read_csv(  
            #             'pyarrow',
            #             temp_filepath,
            #             sep=',',
            #             index_col=0,
            #             names=DATA_COLUMN_NAMES.TICK_DATA,
            #             dtype=raw_file_dtypes,
            #             parse_dates=[0],
            #             date_format=DATE_FORMAT_HISTDATA_CSV,
            #             engine = 'pyarrow',
            #             dtype_backend = 'pyarrow'
            # )
            # perform step to convert index
            # into a datetime64 dtype
            # df.index = any_date_to_datetime64(df.index,
            #                         date_format=DATE_FORMAT_HISTDATA_CSV,
            #                         unit='ms')
            
            # use pyarrow native options
            read_opts = arrow_csv.ReadOptions(
                        use_threads  = True,
                        column_names = DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                        
                )
            
            parse_opts = arrow_csv.ParseOptions(
                        delimiter = ','
                )
            
            modtypes = PYARROW_DTYPE_DICT.TIME_TICK_DTYPE.copy()
            modtypes[BASE_DATA_COLUMN_NAME.TIMESTAMP] = pyarrow_string()
            modtypes.pop(BASE_DATA_COLUMN_NAME.P_VALUE)
            
            convert_opts = arrow_csv.ConvertOptions(
                        column_types = modtypes
            )
            
            # at first read file with timestmap as a string
            df = read_csv(  
                'pyarrow',
                temp_filepath,
                read_options    = read_opts,
                parse_options   = parse_opts,
                convert_options = convert_opts
            )
            
            # convert timestamp  string array to pyarrow timestamp('ms')
            
            # pandas/numpy solution
            # std_datetime = to_datetime(df[BASE_DATA_COLUMN_NAME.TIMESTAMP].to_numpy(), 
            #                            format=DATE_FORMAT_HISTDATA_CSV)
            
            # timecol = pyarrow_array(std_datetime, 
            #                         type=pyarrow_timestamp('ms'))
            
            # all pyarrow ops solution
            # suggested here
            # https://github.com/apache/arrow/issues/41132#issuecomment-2052555361
            
            mod_format = DATE_FORMAT_HISTDATA_CSV.removesuffix('%f')
            ts2 = pc.strptime(pc.utf8_slice_codeunits(df[BASE_DATA_COLUMN_NAME.TIMESTAMP], 
                                                      0,
                                                      15),
                              format=mod_format, 
                              unit="ms")
            d = pc.utf8_slice_codeunits(df[BASE_DATA_COLUMN_NAME.TIMESTAMP], 
                                        15,
                                        99).cast(pyarrow_int64()).cast(duration("ms"))
            timecol = pc.add(ts2, d)
            
            # calculate 'p'
            p_value = pc.divide(
                            pc.add_checked(df['ask'], df['bid']),
                            2 
            )
       
            # aggregate in a new table
            df = Table.from_arrays(
                [
                    timecol,
                    df[BASE_DATA_COLUMN_NAME.ASK],
                    df[BASE_DATA_COLUMN_NAME.BID],
                    df[BASE_DATA_COLUMN_NAME.VOL],
                    p_value
                ],
                schema = schema(PYARROW_DTYPE_DICT.TIME_TICK_DTYPE.copy().items())
            )
            
        elif engine == 'polars':
            
            # download to temporary csv file 
            # for best performance with polars
            
            # alternative using pyarrow
            buf = BufferReader(raw_file.read())
            
            if (
                Path(temp_filepath).exists()
                and
                Path(temp_filepath).is_file()
                ):
                
                Path(temp_filepath).unlink(missing_ok=True)    
                
            else:
                
                # create temporary files directory if not present
                tempdir_path = Path(temp_filepath).parent
                tempdir_path.mkdir(exist_ok=True)
                
            buf.download(temp_filepath)
            
            # from histdata raw files column 'p' is not present
            raw_file_dtypes = POLARS_DTYPE_DICT.TIME_TICK_DTYPE.copy()
            raw_file_dtypes.pop('p')
            raw_file_dtypes[BASE_DATA_COLUMN_NAME.TIMESTAMP] = polars_string
            
            # read file
            # set schema for columns but avoid timestamp columns
            df = read_csv(
                        'polars',
                        temp_filepath,
                        separator   = ',',
                        has_header  = False,
                        new_columns = DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                        schema      = raw_file_dtypes,
                        use_pyarrow = True
            )
            
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
            
            # final cast to standard dtypes
            df = df.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
            
            # clean duplicated timestamps rows, keep first by default
            df = df.unique(subset=[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                           keep='first')
                
        elif engine == 'polars_lazy':
            
            # download to temporary csv file 
            # for best performance with polars
            
            # alternative using pyarrow
            buf = BufferReader(raw_file.read())
            
            if (
                Path(temp_filepath).exists()
                and
                Path(temp_filepath).is_file()
                ):
                
                Path(temp_filepath).unlink(missing_ok=True)    
                
            else:
                
                # create temporary files directory if not present
                tempdir_path = Path(temp_filepath).parent
                tempdir_path.mkdir(exist_ok=True)
                
            # download buffer to file
            buf.download(temp_filepath)
            
            # from histdata raw files column 'p' is not present
            raw_file_dtypes = POLARS_DTYPE_DICT.TIME_TICK_DTYPE.copy()
            raw_file_dtypes.pop('p')
            raw_file_dtypes[BASE_DATA_COLUMN_NAME.TIMESTAMP] = polars_string
            
            # read file
            # set schema for columns but avoid timestamp columns
            df = read_csv(
                        'polars_lazy',
                        temp_filepath,
                        separator   = ',',
                        has_header  = False,
                        new_columns = DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                        schema      = raw_file_dtypes
            )
            
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
            
            # final cast to standard dtypes
            df = df.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
            
            # clean duplicated timestamps rows, keep first by default
            df = df.unique(subset=[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                           keep='first')
            
        else:
            
            logger.error(f'Engine {engine} is not supported')
            raise TypeError
            
        # return dataframe
        return df
    
    
    def _download_year(self,
                       ticker,
                       year
        ):

        year_tick_df = self._dataframe_type([])        

        for month in MONTHS:

            month_num = MONTHS.index(month) + 1
            url = HISTDATA_URL_TICKDATA_TEMPLATE.format(
                                        ticker=ticker.lower(),
                                        year=year,
                                        month_num=month_num)
            
            logger.info(f'To download: {ticker} - {year} - '
                        f'{MONTHS[month_num-1]}') 
            
            file = self._download_month_raw(
                            ticker, 
                            url,
                            year,
                            month_num
                    )
            
            if file:
                
                month_data = self._raw_zipfile_to_df(file,
                                                     str(self._temporary_data_path
                                                         / ( f'{ticker}_' +
                                                         f'{year}_' +
                                                         f'{month}_' +
                                                         TEMP_CSV_FILE )
                                                         ),
                                                     engine = self.engine
                )
                 
                # if first iteration, assign instead of concat
                if is_empty_dataframe(year_tick_df):
                    
                    year_tick_df = month_data
                    
                else:
                    
                    year_tick_df = concat_data([year_tick_df, month_data])
                    
        return sort_dataframe(year_tick_df,
                              BASE_DATA_COLUMN_NAME.TIMESTAMP)
    
    def _download(self,
                  ticker,
                  years):

        if not(
                isinstance(years, list)
            ):
            
            logger.error('years {years} invalid, must be list type')
            raise TypeError

        if not(
                set(years).issubset(YEARS)
            ):
            
            logger.error('requestedyears{years} not available. '
                        'Years must be limited to: {YEARS}')
            raise ValueError
            
        # convert to list of int
        if not all(isinstance(year, int) for year in years):
            years = [int(year) for year in years]

        for year in years:

            year_tick_df = self._download_year(
                                    ticker,
                                    year
                            )

            # get key for dotty dict: TICK
            tick_key = self._db_connector._db_key('forex',
                                                  ticker,
                                                  'TICK')
            
            # call to upload df to database
            self._db_connector.write_data(tick_key,
                                          year_tick_df) 

        # update manager database
        self._update_db()
        
        
    def add_timeframe(self, timeframe, update_data=False):

        if not hasattr(self, '_tf_list'):
            self._tf_list = []

        if isinstance(timeframe, str):

            timeframe = [timeframe]

        if not(
            isinstance(timeframe, list) \
            and 
            all([isinstance(tf, str) for tf in timeframe])
            ):
            
            logger.error('timeframe invalid: str or list required')
            raise TypeError

        tf_list = [check_timeframe_str(tf) for tf in timeframe]

        if not set(tf_list).issubset(self._tf_list):

            # concat timeframe accordingly
            # only just new elements not already present
            self._tf_list.extend(set(tf_list).difference(self._tf_list))

            if update_data:

                self._update_db()
                
    
    def get_data(self,
                ticker,
                timeframe,
                start,
                end,
                add_timeframe = True):
        
        # force ticker parameter to upper case
        ticker = ticker.upper()

        if not check_timeframe_str(timeframe):
            
            logger.error(f'timeframe request {timeframe} invalid')
            raise ValueError
            
        else:
            
            start = any_date_to_datetime64(start)
            end = any_date_to_datetime64(end)

        if end < start:
            
            logger.error('date interval not coherent, '
                         'start must be older than end')
            return self._dataframe_type([])
        
        # get years including interval requested
        years_interval_req = list(range(start.year, end.year+1, 1))
        
        # get all keys referring to specific ticker
        ticker_years_list = self._get_ticker_years_list(ticker, timeframe=timeframe)
        
        # aggregate data to current instance if necessary
        if not set(years_interval_req).issubset(ticker_years_list):
            
            year_tf_missing = list(set(years_interval_req).difference(ticker_years_list))
            
            year_tick_keys = self._get_ticker_years_list(ticker, timeframe=TICK_TIMEFRAME)
            
            year_tick_missing = list(set(years_interval_req).difference(year_tick_keys))
            
            # if tick is missing --> download missing years
            if year_tick_missing:
                
                self._download(
                    ticker,
                    year_tick_missing
                )
                
            # if timeframe req is in tf_list 
            # data requested should at this point be available
            # call add data for specific timeframe requested 
            if not timeframe in self._tf_list:
                
                # call add single tf data
                self.add_timeframe(timeframe, 
                                   update_data=True)
                    
            # get all keys referring to specific ticker
            ticker_keys = self._get_ticker_keys(ticker)
             
            # get all keys referring to specific ticker
            ticker_years_list = self._get_ticker_years_list(ticker, timeframe=timeframe)
            
            if not set(years_interval_req).issubset(ticker_years_list):
            
                logger.critical(f'processing year data completion for '
                                f'{years_interval_req} not ok')
                raise ValueError
                
        # at this point all data requested have been aggregated to the database
        
        # we can proceed by just executing a query from the database
        # meaning read_data()
        
        return self._db_connector.read_data(
                        market      = 'forex',
                        ticker      = ticker,
                        timeframe   = timeframe,
                        start       = start,
                        end         = end
        )
    
    
    def plot(self, 
             ticker,
             timeframe,
             start_date,
             end_date
        ):
        """
        Plot data in selected time frame and start and end date bound
        :param date_bounds: start and end of plot
        :param timeframe: timeframe to visualize
        :return: void
        """

        logger.info(f'''Chart request:
                    ticker {ticker}
                    timeframe {timeframe}
                    from {start_date}
                    to {end_date}''')
        
        chart_data = self.get_data(ticker        = ticker,
                                   timeframe     = timeframe,
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
                            title    = ticker,
                            ylabel   = 'Quotation',
                            xlabel   = 'Timestamp',
                            volume   = False,
                            figratio = (12,8),
                            figscale = 1
        )
        
        mpf_plot(chart_data,type='candle',**chart_kwargs)

        mpf_show()
        
        
        
        
        
        
        
    
    
