
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
        TDengineConnector
    )


__all__ = ['historical_manager']


# HISTORICAL DATA MANAGER
@define(kw_only=True, slots=True)
class historical_manager:
    
    # interface parameters
    config          : str = field(default='',
                                  validator=validators.instance_of(str))
    data_type       : str = field(default='parquet',
                                  validator=validators.in_(SUPPORTED_DATA_FILES))
    engine          : str = field(default='polars_lazy',
                                  validator=validators.in_(SUPPORTED_DATA_ENGINES))
    
    # internal parameters
    _db_dict = field(factory=dotty, validator=validators.instance_of(Dotty))
    _years   = field(factory=list, validator=validators.instance_of(list))
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
        
        if kwargs['config']:
            
            config_path = Path(kwargs['config'])
            
            if (
                config_path.exists() 
                and  
                config_path.is_dir() 
                ):
                
                config_filepath = read_config_folder(config_path,
                                                     file_pattern='_config.yaml')
            
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
        
        self.__attrs_post_init__(**kwargs)
        
            
    def __attrs_post_init__(self, **kwargs):
        
        # reset logging handlers
        logger.remove()
        
        # checks on data folder path
        if ( 
            not self._histdata_path.is_dir() 
            or
            not self._histdata_path.exists()
            ):
                    
            self._histdata_path.mkdir(parents=True,
                                      exist_ok=True)
            
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
        
    # TODO: EVALUATE TO REMOVE DATA_TYPE FROM KEY
    def _db_key(self, 
                ticker    : str, 
                year      : str,
                timeframe : str
        ) -> str:
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

        return '.'.join([ticker, 'Y'+str(year), tf])


    def _get_ticker_list(self):
        
        return self._db_dict.keys()
        
    
    def _get_ticker_keys(self, ticker):
        
        return [
            key for key in get_dotty_leafs(self._db_dict)
            if search(f'^{ticker}',
                      key)
        ]
        
    
    def _get_years_list(self, ticker, vartype):

        # work on copy as pop operation is 'inplace'
        # so the original db is not modified
        db_copy = self._db_dict.copy()

        try:
            # pop at year level in data copy
            year_db = db_copy.pop(ticker.upper())
            years_keys = year_db.keys()
        except KeyError:
            
            logger.info(f'ticker {ticker} no years data in instance '
                        'in memory database')    
            return []
        
        else:

            # get year value from data keys
            years_list = [key[FILENAME_TEMPLATE.YEAR_NUMERICAL_CHAR:]
                          for key in years_keys]
            
            # remove duplicates
            years_list = list_remove_duplicates(years_list)

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
        db_copy = self._db_dict.get(ticker).copy()

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


    def _get_tf_complete_years(self, 
                               ticker):

        # check input years list if each year is complete
        # across tick and timeframes requested

        # instantiate empty list
        years_complete = list()

        for year in self._get_years_list(ticker, 'int'):

            year_complete = all([
                # create key for dataframe type
                isinstance(self._db_dict.get(self._db_key(ticker,
                                                          year,
                                                          tf)),
                           type(self._dataframe_type([])))
                for tf in self._tf_list
            ])

            if year_complete:

                # append year in list of data found in local folder
                years_complete.append(year)

        return years_complete


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


    def _add_tf_data_key(self,
                         ticker,
                         year,
                         tf):
        
        year_tf_key = self._db_key(ticker,
                                  year,
                                  tf)

        if self._db_dict.get(year_tf_key) is None \
            or not isinstance(self._db_dict.get(year_tf_key),
                              type(self._dataframe_type([]))):

            # get tick key
            year_tick_key = self._db_key(ticker,
                                         year,
                                         'TICK')

            try:

                aux_base_df = self._db_dict.get(year_tick_key)

            except KeyError:

                # to logging
                logger.error(f'Requested to reframe {ticker} '
                             f'{year} in timeframe {tf} '
                             'but tick data was not found')

            else:

                # produce reframed data at the timeframe requested
                self._db_dict[year_tf_key] \
                    = reframe_data(aux_base_df, tf)
        

    def _complete_years_timeframe(self) -> None:

        for ticker in self._get_ticker_list():
            
            # get all years available from db keys
            years_list = self._get_years_list(ticker, 'int')
    
            # get years that has not all timeframes
            years_complete = self._get_tf_complete_years(ticker)
    
            # get years not having all timeframes data
            years_incomplete = set(years_list).difference(years_complete)
    
            ticker_keys = self._get_ticker_keys(ticker)
                
            # get years missing timeframes data but with tick data available
            # in current data instance (no further search offline)
            incomplete_with_tick = [
                                    int(get_dotty_key_field(key, 
                                                        DATA_KEY.YEAR_INDEX)[1:])
                                    for key in ticker_keys
                                    if get_dotty_key_field(key, DATA_KEY.TF_INDEX)
                                    == TICK_TIMEFRAME
                                    and
                                    int(get_dotty_key_field(key, 
                                                        DATA_KEY.YEAR_INDEX)[1:])
                                    in years_incomplete
                                    ]
    
            # complete years reframing from tick/minimal timeframe data
            for year in incomplete_with_tick:
    
                for tf in self._tf_list:
    
                    self._add_tf_data_key(
                                ticker,
                                year, 
                                tf
                    )
    
            # check consistency, exit if fail
            if not (
                self._get_years_list(ticker, 'int') 
                == self._get_tf_complete_years(ticker)
                ):
                
                logger.critical('timeframe completing operation FAILED')
                
                raise KeyError
                
    
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
            
        else:
            
            logger.error(f'Engine {engine} is not supported')
            raise TypeError
            
        # return dataframe
        return df


    def _year_data_to_file(self,
                           ticker,
                           year,
                           tf=None,
                           engine='polars'
        ):
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

        ticker_path = self._histdata_path / ticker
        if not ticker_path.is_dir() or not ticker_path.exists():
            ticker_path.mkdir(parents=True, exist_ok=False)

        year_path = ticker_path / str(year).upper()
        if not year_path.is_dir() or not year_path.exists():
            year_path.mkdir(parents=True, exist_ok=False)

        # alternative: get year by referenced key
        year_tf_key = self._db_key(ticker, year, tf)
        
        if not (
            isinstance(self._db_dict.get(year_tf_key), 
                       type(self._dataframe_type([])))
            ):
            
            logger.error(f'dat with key {year_tf_key} is no valid DataFrame')
            raise KeyError
            
        year_data = self._db_dict.get(year_tf_key)

        filepath = year_path / self._get_filename(ticker, year, tf)

        if filepath.exists() and filepath.is_file():
            
            logger.info('File {filepath} already exists, skip dump to file')
            return

        # dump data to file
        if self.data_type == DATA_TYPE.CSV_FILETYPE:
            
            # csv data file
            write_csv(year_data, filepath)
            
            
        elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
            
            # parquet data file
            write_parquet(year_data, str(filepath.absolute()))
            

    def _get_file_details(self, filename):

        if not (
                isinstance(filename, str)
        ):
            
            logger.error('filename {filename} invalid type: required str')

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
        return FILENAME_STR.format(ticker   = ticker,
                                   year     = year,
                                   tf       = tf,
                                   file_ext = self.data_type)


    def _update_local_data_folder(self):

        for ticker in self._get_ticker_list():
            
            # get active years loaded on db manager
            years_list = self._get_years_list(ticker, 'int')
    
            # get file names in local folder
            _, local_files_name = self._list_local_data(ticker)
    
            # loop through years
            for year in years_list:
    
                year_tf_list = self._get_year_timeframe_list(ticker, year)
    
                # loop through timeframes loaded
                for tf in year_tf_list:
    
                    tf_filename = self._get_filename(ticker,
                                                     year,
                                                     tf)
    
                    tf_key = self._db_key(ticker, year, tf)
    
                    # check if file is present in local data folder
                    # and if valid dataframe is currently loaded in database
                    if tf_filename not in local_files_name \
                       and isinstance(self._db_dict.get(tf_key),
                                      type(self._dataframe_type([]))):
    
                        self._year_data_to_file(ticker,
                                                year,
                                                tf=tf,
                                                engine=self.engine
                        )


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
                  years,
                  search_local=False):

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

        # search if years data are already available offline
        if search_local:
            years_tickdata_offline = self._local_load_data(
                                            ticker,
                                            years, 
                                            engine=self.engine
            )
            
        else:

            years_tickdata_offline = list()

        # years not found on local offline path
        # must be downloaded from the net
        tick_years_to_download = set(years).difference(years_tickdata_offline)

        if tick_years_to_download:

            for year in tick_years_to_download:

                year_tick_df = self._download_year(
                                        ticker,
                                        year
                                )

                # get key for dotty dict: TICK
                year_tick_key = self._db_key(ticker, year, 'TICK')
                self._db_dict[year_tick_key] = year_tick_df

        # update manager database
        self._update_db()
        

    def _list_local_data(self,
                         ticker):

        local_files = []
        local_files_name = []
        
        # prepare predefined path and check if exists
        tickerfolder_path = Path(self._histdata_path) / ticker
        if not (
            tickerfolder_path.exists() \
            and 
            tickerfolder_path.is_dir()
        ):
            
            tickerfolder_path.mkdir(parents=True, 
                                    exist_ok=False)
            
        else:
            
            # list all specifed ticker data files in folder path and subdirs
            # list for all data filetypes supported
            
            local_files = [file for file in list(tickerfolder_path.glob(
                                            f'**/{ticker}_*.*'))
                            if match('.(\w+)$', file.suffix).groups()[0]
                            in SUPPORTED_DATA_FILES]
            
            local_files_name = [file.name for file in local_files]

            # check compliance of files to convention (see notes)
            # TODO: warning if no compliant and filter out from files found

        return local_files, local_files_name


    def _local_load_data(self, 
                         ticker,
                         years_list,
                         engine='polars'
        ):
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
        local_files, local_files_name = self._list_local_data(ticker)

        years_tick_files_found = list()
        
        # avoid to load data if there are file duplicates by name
        # identify duplicates and remove csv type file
        # in order increase speed if parquet file is available
        local_files_stem = [file.stem for file in local_files]
        
        duplicates_unique = list(unique_everseen(duplicates(local_files_stem)))
        
        local_files_filtered = local_files.copy()
        local_files_name_filtered = local_files_name.copy()
        
        if duplicates_unique:
        
            for item in duplicates_unique:
                
                parquet_file = item \
                                + '.' \
                                + DATA_TYPE.PARQUET_FILETYPE
                                
                csv_file = item \
                            + '.' \
                            + DATA_TYPE.CSV_FILETYPE
                
                if (
                    parquet_file in local_files_name_filtered
                    and
                    csv_file in local_files_name_filtered
                    ):
                    
                    index_to_remove = local_files_name_filtered.index(csv_file)
                    local_files_name_filtered.pop(index_to_remove)
                    local_files_filtered.pop(index_to_remove)
                
        # parse files and fill details list
        for file in local_files_filtered:

            # get years available in offline data (local disk)
            local_filepath_key = file.name.replace('_', '.')

            # get file details
            file_ticker, file_year, file_tf = self._get_file_details(file.name)

            # check at timeframe index file has a valid timeframe
            if check_timeframe_str(file_tf) == file_tf:

                # create key for dataframe type
                year_tf_key = self._db_key(file_ticker,
                                          file_year,
                                          file_tf)

                if self._db_dict.get(year_tf_key) is None:
                    
                    # check if year is needed to be loaded
                    if file_ticker == ticker \
                            and (int(file_year) in years_list):

                        if file_tf == TICK_TIMEFRAME:
                            years_tick_files_found.append(file_year)
    
                        if match(f'.({DATA_TYPE.CSV_FILETYPE})$',
                                file.suffix):
                            
                            if engine == 'pandas':
                                
                                # tick data file 
                                if file_tf == TICK_TIMEFRAME:
            
                                    self._db_dict[year_tf_key] = \
                                        read_csv(  
                                            'pandas',
                                            file,
                                            sep=',',
                                            header=0,
                                            names=DATA_COLUMN_NAMES.TICK_DATA,
                                            dtype=DTYPE_DICT.TICK_DTYPE,
                                            parse_dates=[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                            date_format=DATE_FORMAT_ISO8601,
                                            engine = 'c'
                                    )
                                    
                                # year standard data file
                                elif file_tf in self._tf_list:
            
                                    self._db_dict[year_tf_key] = \
                                        read_csv(
                                            'pandas',
                                            file,
                                            header=0,
                                            names=DATA_COLUMN_NAMES.TF_DATA,
                                            dtype=DTYPE_DICT.TF_DTYPE,
                                            parse_dates=[
                                                BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                            date_format=DATE_FORMAT_ISO8601,
                                            engine='c'
                                    )
                                    
                            elif engine == 'pyarrow':
                                
                                
                                # use panda read_csv an its options with 
                                # engine = 'pyarrow'
                                # dtype_backend = 'pyarrow'
                                # self._db_dict[year_tf_key] = \
                                #     read_csv(  
                                #        engine = 'pyarrow',
                                #        dtype_backend = 'pyarrow'
                                # )
                                
                                # use pyarrow native options
                                read_opts = arrow_csv.ReadOptions(
                                            use_threads  = True,
                                            autogenerate_column_names = False
                                            
                                    )
                                
                                parse_opts = arrow_csv.ParseOptions(
                                            delimiter = ','
                                    )
                                
                                # tick data file 
                                if file_tf == TICK_TIMEFRAME:
                                    
                                    convert_opts = arrow_csv.ConvertOptions(
                                                column_types = PYARROW_DTYPE_DICT.TIME_TICK_DTYPE,
                                                timestamp_parsers = [arrow_csv.ISO8601]
                                    )
                                 
                                    
                                else:
                                    
                                    convert_opts = arrow_csv.ConvertOptions(
                                                column_types = PYARROW_DTYPE_DICT.TIME_TF_DTYPE,
                                                timestamp_parsers = [arrow_csv.ISO8601]
                                    )
                                    
                                
                                self._db_dict[year_tf_key] = \
                                    read_csv(  
                                            'pyarrow',
                                            file,
                                            read_options    = read_opts,
                                            parse_options   = parse_opts,
                                            convert_options = convert_opts
                                    )
                            
                            # year standard data file    
                            elif engine == 'polars':
                                
                                # tick data file 
                                if file_tf == TICK_TIMEFRAME:
                                    
                                    self._db_dict[year_tf_key] = \
                                        read_csv('polars', 
                                                 file,
                                                 new_columns=DATA_COLUMN_NAMES.TICK_DATA,
                                                 schema=POLARS_DTYPE_DICT.TIME_TICK_DTYPE,
                                                 try_parse_dates=True
                                        )
                                                     
                                elif file_tf in self._tf_list:
                                    
                                    self._db_dict[year_tf_key] = \
                                        read_csv('polars', 
                                                 file,
                                                 new_columns=DATA_COLUMN_NAMES.TF_DATA,
                                                 schema=POLARS_DTYPE_DICT.TIME_TF_DTYPE,
                                                 try_parse_dates=True
                                        )
                                        
                            elif  engine == 'polars_lazy':
                                
                                # tick data file 
                                if file_tf == TICK_TIMEFRAME:
                                    
                                    self._db_dict[year_tf_key] = \
                                        read_csv('polars_lazy', 
                                                 file,
                                                 new_columns=DATA_COLUMN_NAMES.TICK_DATA,
                                                 schema=POLARS_DTYPE_DICT.TIME_TICK_DTYPE,
                                                 try_parse_dates=True
                                        )
                                                     
                                elif file_tf in self._tf_list:
                                    
                                    self._db_dict[year_tf_key] = \
                                        read_csv('polars_lazy', 
                                                 file,
                                                 new_columns=DATA_COLUMN_NAMES.TF_DATA,
                                                 schema=POLARS_DTYPE_DICT.TIME_TF_DTYPE,
                                                 try_parse_dates=True
                                        )
                        
                            else:
                                
                                logger.error(f'Engine {engine} not supported '
                                             f'available: {SUPPORTED_DATA_ENGINES}')
                                raise TypeError
                    
                        elif  match(f'.({DATA_TYPE.PARQUET_FILETYPE})$',
                                    file.suffix):
                        
                            self._db_dict[year_tf_key] = read_parquet(engine,
                                                                      file)
                            
                        else:
                            
                            logger.error(f'file type {self.data_type} not supported '
                                         f'available: {SUPPORTED_DATA_FILES}')
                            raise TypeError
                            
                            
                        
                else:
                    
                    # continue as data is already present
                    continue
                            
                        
        # return list of years which tick file 
        # has been found and loaded
        return years_tick_files_found
    

    def _update_db(self):

        # complete year keys along timeframes required
        self._complete_years_timeframe()

        # dump new downloaded data not already present in local data folder
        self._update_local_data_folder()
        
        # all data is dumped in database folder
        # okay to clear temporary data folder
        #self._clear_temporary_data_folder()


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
                add_timeframe = True,
                add_in_memory = True):
        
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
        
        # force ticker parameter to upper case
        ticker = ticker.upper()

        if not check_time_offset_str(timeframe):
            
            logger.error(f'timeframe request {timeframe} invalid')
            raise ValueError
            
        else:
            
            start = any_date_to_datetime64(start)
            end = any_date_to_datetime64(end)

        if end < start:
            
            logger.error('date interval not coherent, '
                         'start must be older than end')
            return self._dataframe_type([])

        if not timeframe in self._tf_list \
            and add_timeframe:

            # timeframe list
            self.add_timeframe([timeframe])

        # get years including interval requested
        years_interval_req = list(range(start.year, end.year+1, 1))
        
        # get all keys referring to specific ticker
        ticker_keys = self._get_ticker_keys(ticker)
        
        # get data keys referred to interval years 
        # at the given timeframe
        interval_keys = [key for key in ticker_keys
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
                              for key in ticker_keys
                                if get_dotty_key_field(key, 
                                                       DATA_KEY.TF_INDEX) \
                                    == TICK_TIMEFRAME ]
            
            year_tick_missing = list(set(years_interval_req).difference(year_tick_keys))
            
            # if tick is missing --> download missing years
            if year_tick_missing:
                
                self._download(
                    ticker,
                    year_tick_missing, 
                    search_local=True
                )
                
            # if timeframe req is in tf_list 
            # data requested should at this point be available
            # call add data for specific timeframe requested 
            if not timeframe in self._tf_list:
                
                for year in year_tf_missing:
                    
                    # call add single tf data
                    self._add_tf_data_key(
                            ticker, 
                            year,
                            timeframe
                    )
                    
            # get all keys referring to specific ticker
            ticker_keys = self._get_ticker_keys(ticker)
             
            # get years covered by interval keys
            interval_keys_years = [int(get_dotty_key_field(key, DATA_KEY.YEAR_INDEX)[1:]) 
                                   for key in ticker_keys
                                   if int(get_dotty_key_field(key, DATA_KEY.YEAR_INDEX)[1:])
                                       in years_interval_req and \
                                        get_dotty_key_field(key, 
                                                            DATA_KEY.TF_INDEX) \
                                        == timeframe]
            
            if not( years_interval_req == interval_keys_years ):
            
                logger.critical(f'processing year data completion for '
                                f'{years_interval_req} not ok')
                raise ValueError
                    
        # at this point data keys necessary are completed
        
        # get data keys referred to interval years 
        # at the given timeframe
        interval_keys = [key for key in ticker_keys
                          if int(get_dotty_key_field(key, DATA_KEY.YEAR_INDEX)[1:])
                              in years_interval_req \
                              and get_dotty_key_field(key, DATA_KEY.TF_INDEX) \
                                  == timeframe]
            
        data_df = self._dataframe_type([])
        
        # TODO: do I have to return explicit copies of date sliced results?
        
        if len(interval_keys) == 1: 

            data_df = self._db_dict.get(interval_keys[0])

            # return data slice
            if self.engine == 'pandas':
            
                data_df = data_df[(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP]
                                   >= start)
                                  & 
                                  (data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP]
                                   <= end)].copy()
                
                # reset index
                data_df.reset_index(drop=True, inplace=True)
            
            elif self.engine == 'pyarrow':
                
                mask = pc.and_(
                            pc.greater(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                       start),
                            pc.less(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                       end)
                            )
                
                data_df = Table.from_arrays(data_df.filter(mask).columns,
                                            schema=data_df.schema)
                
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
            
            elif self.engine == 'polars_lazy':
                
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
                
                logger.error(f'engine {self.engine} not supported'
                             ' for get_data function')
                raise TypeError
            
        else:

            # order keys by ascending year value
            interval_keys.sort(key=lambda x: 
                               int(get_dotty_key_field(x, 
                                                       DATA_KEY.YEAR_INDEX)[1:])
                               )

                
            # TODO: need to differentiate dataframe slicing, filtering and
            #       selection depending on engine used
                
            # get data interval
            
            if self.engine == 'pandas':
                
                data_df = concat_data([self._db_dict.get(key)[
                    (ticker_db_dict.get(key)[BASE_DATA_COLUMN_NAME.TIMESTAMP] 
                         >= start)
                    & (ticker_db_dict.get(key)[BASE_DATA_COLUMN_NAME.TIMESTAMP] 
                         <= end)].copy()
                    for key in interval_keys]
                )
                
                data_df.reset_index(drop=True, inplace=True)
                
            elif self.engine == 'pyarrow':
                
                mask = pc.and_(
                            pc.greater(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                       start),
                            pc.less(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                       end)
                            )
                
                data_df = concat_data(
                    [
                        Table.from_arrays(
                            self._db_dict.get(key).filter(mask).columns,
                            schema=data_df.schema
                        )
                        for key in interval_keys
                    ]
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
                    
            elif self.engine == 'polars_lazy':
            
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
                
                logger.error(f'engine {self.engine} not supported'
                             ' for get_data function')
                raise TypeError
                
        # if not requested to save data in memory
        # wipe out db keys dictionary
        if not add_in_memory:
            
            self._db_dict.clear()
            
        return data_df
    

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
        
        if kwargs['config']:
            
            config_path = Path(kwargs['config'])
            
            if (
                config_path.exists() 
                and  
                config_path.is_dir() 
                ):
                
                config_filepath = read_config_folder(config_path,
                                                     file_pattern='_config.yaml')
            
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
        
        self.__attrs_post_init__(**kwargs)
    
    def __attr_post_init__(self, **kwargs):
        
        # instance database connector if selected
        if self.data_type == DATA_TYPE.TDENGINE_DATABASE:
            
            self._db_connector = TDengineConnector(**kwargs)
