
from loguru import logger
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

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
    ZipExtFile,
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


__all__ = ['HistoricalManagerDB']


# HISTORICAL DATA MANAGER
@define(kw_only=True, slots=True)
class HistoricalManagerDB:

    # interface parameters
    config: str = field(default='',
                        validator=validators.instance_of(str))
    data_type: str = field(default='parquet',
                           validator=validators.in_(SUPPORTED_DATA_FILES))
    engine: str = field(default='polars_lazy',
                        validator=validators.in_(SUPPORTED_DATA_ENGINES))

    # internal
    _db_connector = field(factory=DatabaseConnector)
    _tf_list = field(factory=list, validator=validators.instance_of(list))
    _dataframe_type = field(default=pandas_dataframe)
    _histdata_path = field(
        default=Path(DEFAULT_PATHS.BASE_PATH) / DEFAULT_PATHS.HIST_DATA_FOLDER,
        validator=validator_dir_path(create_if_missing=True))
    _temporary_data_path = field(
        default=(Path(DEFAULT_PATHS.BASE_PATH) /
                 DEFAULT_PATHS.HIST_DATA_FOLDER /
                 TEMP_FOLDER),
        validator=validator_dir_path(create_if_missing=True))
    _histdata_tickers_list = field(factory=list, validator=validators.instance_of(list))
    _tickers_years_dict = field(factory=dict, validator=validators.instance_of(dict))

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

    def __init__(self, **kwargs: Any) -> None:

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

                    config_filepath = read_config_folder(
                        config_path, file_pattern='data_config.yaml')

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
                if (
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
                        raise

                    except IndexError:

                        logger.warning('IndexError: initializing object has no '
                                       f'attribute {attr.name}')
                        raise

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

                logger.trace(
                    f'config {kwargs["config"]} is empty, using default configuration')

        else:

            # no config file is defined
            # call generated init
            self.__attrs_init__(**kwargs)  # type: ignore[attr-defined]

        validate(self)

        self.__attrs_post_init__()

    def __attrs_post_init__(self, **kwargs: Any) -> None:

        # set up log sink for historical manager
        # Remove existing handlers for this sink to prevent duplicate log entries
        log_path = self._histdata_path / 'log' / 'forexhistdata.log'

        # Remove handlers that match this log file path
        # logger.remove() without args would remove ALL handlers including stderr
        # So we iterate and remove only matching handlers
        handlers_to_remove = []
        for handler_id, handler in logger._core.handlers.items():
            # Check if this handler writes to our log file
            if hasattr(handler, '_sink') and hasattr(handler._sink, '_path'):
                if str(handler._sink._path) == str(log_path):
                    handlers_to_remove.append(handler_id)

        for handler_id in handlers_to_remove:
            logger.remove(handler_id)

        # Now add the handler
        logger.add(log_path,
                   level="TRACE",
                   rotation="5 MB",
                   filter=lambda record: ('histmanager' == record['extra'].get('target') and
                                          bool(record["extra"].get('target'))))

        # set up dataframe engine internal var based on config selection
        if self.engine == 'pandas':

            self._dataframe_type = pandas_dataframe

        elif self.engine == 'pyarrow':

            self._dataframe_type = pyarrow_table

        elif self.engine == 'polars':

            self._dataframe_type = polars_dataframe

        elif self.engine == 'polars_lazy':

            self._dataframe_type = polars_lazyframe

        else:

            logger.bind(target='histmanager').error(f'Engine {self.engine} not supported')
            raise ValueError(f'Engine {self.engine} not supported')

        self._temporary_data_path = self._histdata_path \
            / TEMP_FOLDER

        self._clear_temporary_data_folder()

        # instance database connector if selected
        if self.data_type == DATA_TYPE.DUCKDB:

            self._db_connector = DuckDBConnector(duckdb_filepath=str(
                self._histdata_path / 'DuckDB' / 'marketdata.duckdb'))

        elif (
                self.data_type == DATA_TYPE.CSV_FILETYPE or
            self.data_type == DATA_TYPE.PARQUET_FILETYPE
        ):

            self._db_connector = \
                LocalDBConnector(
                    data_folder=str(self._histdata_path / 'LocalDB'),
                    data_type=self.data_type,
                    engine=self.engine
                )

        else:

            logger.bind(target='histmanager').error(f'Data type {self.data_type} not supported')
            raise ValueError(f'Data type {self.data_type} not supported')

        # cache histdata tickers list at initialization
        self._histdata_tickers_list = get_histdata_tickers()

        # initialize tickers years dict info of data available
        # with the current connector
        self._tickers_years_dict = self._db_connector.create_tickers_years_dict()

    def _clear_temporary_data_folder(self) -> None:

        # delete temporary data path
        if (
            self._temporary_data_path.exists() and
            self._temporary_data_path.is_dir()
        ):

            try:

                rmtree(str(self._temporary_data_path))

            except Exception as e:

                logger.bind(target='histmanager').warning(
                    'Deleting temporary data folder '
                    f'{str(self._temporary_data_path)} not successfull: {e}')

    def _get_ticker_list(self) -> List[str]:

        # return list of tickers elements as str

        return self._db_connector.get_tickers_list()

    def _get_ticker_keys(
            self,
            ticker: str,
            timeframe: Optional[str] = None) -> List[str]:

        # return list of ticker keys elements as str

        return self._db_connector.get_ticker_keys(ticker,
                                                  timeframe=timeframe)

    def _get_ticker_years_list(
            self,
            ticker: str,
            timeframe: str = TICK_TIMEFRAME) -> List[int]:

        # return list of ticker years covered in data elements as str
        # if timeframe is None means years in data in tick or 1m timeframe

        return self._db_connector.get_ticker_years_list(ticker,
                                                        timeframe=timeframe)

    def _update_db(self, ticker: str = None) -> None:

        if not ticker:
            ticker_list = self._get_ticker_list()
        else:
            ticker_list = [ticker]

        for ticker in ticker_list:

            years_tick = self._tickers_years_dict[ticker][TICK_TIMEFRAME]

            for tf in self._tf_list:

                # Initialize ticker/timeframe in dict if not present
                if tf not in self._tickers_years_dict[ticker]:
                    self._tickers_years_dict[ticker][tf] = []

                ticker_years_list = self._tickers_years_dict[ticker][tf]

                if set(years_tick).difference(ticker_years_list):
                    years = list(set(years_tick).difference(ticker_years_list))

                    end_year = max(years)
                    start_year = min(years)

                    year_start = f'{start_year}-01-01 00:00:00.000'
                    year_end = f'{end_year + 1}-01-01 00:00:00.000'
                    # read missing years from tick timeframe
                    start = datetime.strptime(year_start, DATE_FORMAT_SQL)
                    end = datetime.strptime(year_end, DATE_FORMAT_SQL)

                    dataframe = self._db_connector.read_data(
                        market='forex',
                        ticker=ticker,
                        timeframe=TICK_TIMEFRAME,
                        start=start,
                        end=end
                    )

                    # reframe to timeframe
                    dataframe_tf = reframe_data(dataframe, tf)

                    # get data id key
                    tf_key = self._db_connector._db_key(
                        'forex',
                        ticker,
                        tf
                    )

                    # write to database to complete the years
                    # call to upload df to database
                    self._db_connector.write_data(tf_key,
                                                  dataframe_tf)

                    # update years list in local info file
                    self._db_connector.add_tickers_years_info_to_file(ticker,
                                                                      tf,
                                                                      years)

                    # update internal ticker years list info
                    self._tickers_years_dict[ticker][tf].extend(years)
                    # sort and remove duplicates
                    self._tickers_years_dict[ticker][tf].sort()
                    self._tickers_years_dict[ticker][tf] = \
                        list_remove_duplicates(self._tickers_years_dict[ticker][tf])

                    # REDO THE CHECK FOR CONSISTENCY
                    if set(years_tick).difference(self._tickers_years_dict[ticker][tf]):

                        logger.bind(target='histmanager').critical(
                            f'ticker {ticker}: {tf} timeframe completing'
                            ' operation FAILED')

                        raise KeyError

                    else:
                        logger.bind(target='histmanager').trace(
                            f'ticker {ticker}: {tf} timeframe completing operation successful for {years}')

    def _download_month_raw(self,
                            ticker,
                            url,
                            year,
                            month_num
                            ) -> bytes:
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

        token = None
        try:
            token = search('id="tk" value="(.*?)"', r.text).groups()[0]
        except AttributeError:
            logger.bind(target='histmanager').critical(
                f'token value was not found scraping '
                f'url {url}: {ticker} not existing or'
                f'not supported by histdata.com: {ticker} - '
                f'{year}-{MONTHS[month_num - 1]}')

        # If exception was caught, token will still be None
        if token is None:
            raise TickerNotFoundError(
                f"Ticker {ticker} not found or not supported by histdata.com")

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

        # logger trace ticker year and month specifed are being downloaded
        logger.bind(target='histmanager').trace(f'{ticker} - {year} - {MONTHS[month_num - 1]}: downloading')
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

        except BadZipFile as e:

            # here will be a warning log
            logger.bind(target='histmanager').error(f'{ticker} - {year} - {MONTHS[month_num - 1]}: {e}')
            raise TickerDataBadTypeException(
                f"Data {ticker} - {year} - {MONTHS[month_num - 1]} BadZipFile error: {e}")

        else:

            # return opened zip file
            try:
                ExtFile = zf.open(zf.namelist()[0])
            except Exception as e:
                logger.bind(target='histmanager').error(
                    f'{ticker} - {year} - {MONTHS[month_num - 1]}: '
                    f'not found or invalid download: {e}')
                raise TickerDataNotFoundError(
                    f"Data {ticker} - {year} - {MONTHS[month_num - 1]} not found or not supported by histdata.com")

            else:
                if isinstance(ExtFile, ZipExtFile):
                    return ExtFile
                else:
                    logger.bind(target='histmanager').error(
                        f'{ticker} - {year} - {MONTHS[month_num - 1]}: '
                        f'data type not expected')
                    raise TickerDataBadTypeException(
                        f"Data {ticker} - {year} - {MONTHS[month_num - 1]} type not expected")

    def _raw_zipfile_to_df(self, raw_file, temp_filepath,
                           engine='polars') -> Union[polars_dataframe, polars_lazyframe]:
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
                engine='c'
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
                Path(temp_filepath).exists() and
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
                use_threads=True,
                column_names=DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,

            )

            parse_opts = arrow_csv.ParseOptions(
                delimiter=','
            )

            modtypes = PYARROW_DTYPE_DICT.TIME_TICK_DTYPE.copy()
            modtypes[BASE_DATA_COLUMN_NAME.TIMESTAMP] = pyarrow_string()
            modtypes.pop(BASE_DATA_COLUMN_NAME.P_VALUE)

            convert_opts = arrow_csv.ConvertOptions(
                column_types=modtypes
            )

            # at first read file with timestmap as a string
            df = read_csv(
                'pyarrow',
                temp_filepath,
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=convert_opts
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
            ts2 = pc.strptime(pc.utf8_slice_codeunits(
                df[BASE_DATA_COLUMN_NAME.TIMESTAMP], 0, 15), format=mod_format, unit="ms")
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
                schema=schema(PYARROW_DTYPE_DICT.TIME_TICK_DTYPE.copy().items())
            )

        elif engine == 'polars':

            # download to temporary csv file
            # for best performance with polars

            # alternative using pyarrow
            buf = BufferReader(raw_file.read())

            if (
                Path(temp_filepath).exists() and
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
                separator=',',
                has_header=False,
                new_columns=DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                schema=raw_file_dtypes,
                use_pyarrow=True
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
                ((col('ask') + col('bid')) / 2).alias('p')
            )

            # final cast to standard dtypes
            df = df.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)

            # clean duplicated timestamps rows, keep first by default
            df = df.unique(subset=[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                           keep='first')

            # remove business days
            df = business_days_data(df)

        elif engine == 'polars_lazy':

            # download to temporary csv file
            # for best performance with polars

            # alternative using pyarrow
            buf = BufferReader(raw_file.read())

            if (
                Path(temp_filepath).exists() and
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
                separator=',',
                has_header=False,
                new_columns=DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                schema=raw_file_dtypes
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
                ((col('ask') + col('bid')) / 2).alias('p')
            )

            # final cast to standard dtypes
            df = df.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)

            # clean duplicated timestamps rows, keep first by default
            df = df.unique(subset=[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                           keep='first')

            # remove business days
            df = business_days_data(df)

        else:

            logger.bind(target='histmanager').error(f'Engine {engine} is not supported')
            raise TypeError

        # return dataframe
        return df

    def _download_year(self,
                       ticker,
                       year) -> Union[polars_dataframe,
                                      polars_lazyframe,
                                      pandas_dataframe,
                                      Table,
                                      None]:

        year_tick_df = empty_dataframe(self.engine)

        for month in MONTHS:

            month_num = MONTHS.index(month) + 1
            url = HISTDATA_URL_TICKDATA_TEMPLATE.format(
                ticker=ticker.lower(),
                year=year,
                month_num=month_num)

            file = self._download_month_raw(
                ticker,
                url,
                year,
                month_num
            )

            if file and isinstance(file, ZipExtFile):

                month_data = self._raw_zipfile_to_df(file,
                                                     str(self._temporary_data_path /
                                                         (f'{ticker}_' +
                                                             f'{year}_' +
                                                             f'{month}_' +
                                                             TEMP_CSV_FILE)
                                                         ),
                                                     engine=self.engine
                                                     )

                # if first iteration, assign instead of concat
                if is_empty_dataframe(year_tick_df):

                    year_tick_df = month_data

                else:

                    year_tick_df = concat_data([year_tick_df, month_data])

            else:

                logger.bind(target='histmanager').critical(
                    f"Ticker {ticker}-{year}-{MONTHS[month_num - 1]} data not found or invalid")
                raise TickerDataInvalidException(
                    f"Ticker {ticker} - {year} - {MONTHS[month_num - 1]} data not found or invalid: generic error")

        return sort_dataframe(year_tick_df,
                              BASE_DATA_COLUMN_NAME.TIMESTAMP)

    def _download(self,
                  ticker,
                  years: List[int]) -> None:

        if not (
            isinstance(years, list)
        ):

            logger.bind(target='histmanager').error('years {years} invalid, must be list type')
            raise TypeError

        if not (
            set(years).issubset(YEARS)
        ):

            logger.bind(target='histmanager').error(
                f'requestedyears{years} not available. '
                f'Years must be limited to: {YEARS}')
            raise ValueError

        # convert to list of int
        if not all(isinstance(year, int) for year in years):
            years = [int(year) for year in years]

        # download data for each year
        years_data_df = empty_dataframe(self.engine)
        for year in years:

            year_tick_df = self._download_year(
                ticker,
                year
            )

            # if first iteration, assign instead of concat
            if is_empty_dataframe(years_data_df):

                years_data_df = year_tick_df

            else:

                years_data_df = concat_data([years_data_df, year_tick_df])

        # get data id key
        tick_key = self._db_connector._db_key('forex',
                                              ticker,
                                              TICK_TIMEFRAME)

        # call to upload df to database if not empty
        if not is_empty_dataframe(years_data_df):
            self._db_connector.write_data(tick_key,
                                          years_data_df)

            # update years list in local info file
            self._db_connector.add_tickers_years_info_to_file(ticker,
                                                              TICK_TIMEFRAME,
                                                              years)

            # update internal ticker years list info
            self._tickers_years_dict[ticker][TICK_TIMEFRAME].extend(years)
            # sort and remove duplicates
            self._tickers_years_dict[ticker][TICK_TIMEFRAME].sort()
            self._tickers_years_dict[ticker][TICK_TIMEFRAME] = \
                list_remove_duplicates(self._tickers_years_dict[ticker][TICK_TIMEFRAME])

        else:
            logger.bind(target='histmanager').warning(
                f'Years data dataframe for {tick_key} is empty, skipping database write')

    def clear_database(self, filter: Optional[str] = None) -> None:

        self._db_connector.clear_database(filter=filter)

    def add_timeframe(self, timeframe: str) -> None:
        """
        Add and cache a new timeframe to the database.

        Creates aggregated data for the specified timeframe from tick data and
        caches it in the database for faster future access. The timeframe is
        added to the internal list of available timeframes.

        Args:
            timeframe (str | List[str]): Timeframe(s) to add. Can be a single string
                or list of strings. Supported values: '1m', '5m', '15m', '30m',
                '1h', '4h', '1D', '1W', '1M'

        Returns:
            None

        Raises:
            TypeError: If timeframe is not a string or list of strings

        Example:
            >>> manager = HistoricalManagerDB(config='data_config.yaml')
            >>> manager.add_timeframe('1W')  # Add weekly timeframe
            >>> manager.add_timeframe(['4h', '1D'])  # Add multiple timeframes

        Note:
            - Only new timeframes (not already in the list) will be processed
            - Aggregation can take time for large datasets
            - Once added, the timeframe is permanently cached in the database
        """

        if not hasattr(self, '_tf_list'):
            self._tf_list = []

        if isinstance(timeframe, str):

            timeframe = [timeframe]

        if not (
            isinstance(timeframe, list) and
            all([isinstance(tf, str) for tf in timeframe])
        ):

            logger.bind(target='histmanager').error('timeframe invalid: str or list required')
            raise TypeError

        tf_list = [check_timeframe_str(tf, engine=self.engine) for tf in timeframe]

        if not set(tf_list).issubset(self._tf_list):

            # concat timeframe accordingly
            # only just new elements not already present
            self._tf_list.extend(set(tf_list).difference(self._tf_list))

    def get_data(
        self,
        ticker,
        timeframe,
        start,
        end,
        comparison_column_name: List[str] | str | None = None,
        check_level: List[int | float] | int | float | None = None,
        comparison_operator: List[SUPPORTED_SQL_COMPARISON_OPERATORS] | SUPPORTED_SQL_COMPARISON_OPERATORS | None = None,
        aggregation_mode: SUPPORTED_SQL_CONDITION_AGGREGATION_MODES | None = None,
    ) -> Union[polars_dataframe, polars_lazyframe]:
        """
        Retrieve OHLC historical data for the specified ticker and timeframe.

        Fetches historical forex data from the database, automatically downloading
        and aggregating data if not already available. Supports multiple timeframes
        and date ranges.

        Args:
            ticker (str): Currency pair symbol (e.g., 'EURUSD', 'GBPUSD', 'NZDUSD').
                Case-insensitive.
            timeframe (str): Candle timeframe for data aggregation. Supported values:
                '1m', '5m', '15m', '30m', '1h', '4h', '1D', '1W', '1M'
            start (str | datetime): Start date for data retrieval. Accepts:
                - ISO format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
                - datetime object
            end (str | datetime): End date for data retrieval. Same format as start.
                Must be after start date.
            comparison_column_name (List[str] | str | None): Column names to retrieve. Default is None.
            check_level (List[int | float] | int | float | None): Check level for conditions. Default is None.
            comparison_operator (List[SUPPORTED_SQL_COMPARISON_OPERATORS] | SUPPORTED_SQL_COMPARISON_OPERATORS | None): Condition for data retrieval. Default is None.
            aggregation_mode (SUPPORTED_SQL_CONDITION_AGGREGATION_MODES | None): Aggregation mode for data retrieval. Default is None.

        Returns:
            polars.DataFrame | polars.LazyFrame: DataFrame containing OHLC data with columns:
                - timestamp: datetime column with candle timestamps
                - open: Opening price (float32)
                - high: Highest price (float32)
                - low: Lowest price (float32)
                - close: Closing price (float32)

        Raises:
            TickerNotFoundError: If the ticker is not available in the historical database
            ValueError: If timeframe is invalid or end date is before start date

        Example:
            >>> manager = HistoricalManagerDB(config='data_config.yaml')
            >>> data = manager.get_data(
            ...     ticker='EURUSD',
            ...     timeframe='1h',
            ...     start='2020-01-01',
            ...     end='2020-01-31'
            ... )
            >>> print(f"Retrieved {len(data)} hourly candles")
            Retrieved 744 hourly candles

        Note:
            - Data is automatically downloaded from histdata.com if not cached locally
            - First call for a new timeframe may take longer as it builds the aggregation
            - Downloaded data is cached for faster subsequent access
            - Ticker names are case-insensitive and automatically normalized
        """

        # check ticker exists in available tickers
        # from histdata database
        if (
                ticker.upper() not in self._histdata_tickers_list
                and
                ticker.lower() not in self._get_ticker_list()
        ):
            logger.bind(target='histmanager').error(f'ticker {ticker.upper()} not found in database')
            raise TickerNotFoundError(f'ticker {ticker} not found in database')

        # force ticker parameter to lower case
        ticker = ticker.lower()
        timeframe = check_timeframe_str(timeframe, engine=self.engine).lower()

        if not check_timeframe_str(timeframe, engine=self.engine):

            logger.bind(target='histmanager').error(f'timeframe request {timeframe} invalid')
            raise ValueError

        else:

            start = any_date_to_datetime64(start)
            end = any_date_to_datetime64(end)

        if end < start:

            logger.bind(target='histmanager').error(
                'date interval not coherent, '
                'start must be older than end')
            return self._dataframe_type([])

        # get years including interval requested
        years_interval_req = list(range(start.year, end.year + 1, 1))

        # Initialize ticker/timeframe in dict if not present
        if ticker not in self._tickers_years_dict:
            self._tickers_years_dict[ticker] = {}
        if timeframe not in self._tickers_years_dict[ticker]:
            self._tickers_years_dict[ticker][timeframe] = []
        if TICK_TIMEFRAME not in self._tickers_years_dict[ticker]:
            self._tickers_years_dict[ticker][TICK_TIMEFRAME] = []

        # aggregate data to current instance if necessary
        if not set(years_interval_req).issubset(self._tickers_years_dict[ticker][timeframe]):

            year_tf_missing = list(
                set(years_interval_req).difference(self._tickers_years_dict[ticker][timeframe]))

            year_tick_missing = list(set(years_interval_req).difference(
                self._tickers_years_dict[ticker][TICK_TIMEFRAME]
            ))

            # if tick is missing --> download missing years
            if year_tick_missing:

                self._download(
                    ticker,
                    year_tick_missing
                )

            # add dataframe to the instance
            self.add_timeframe(timeframe)

            # update database
            self._update_db(ticker)

            if not set(years_interval_req).issubset(self._tickers_years_dict[ticker][timeframe]):

                logger.bind(target='histmanager').critical(
                    f'processing year data completion for '
                    f'{years_interval_req} not ok')
                raise ValueError

        # clear temporary data folder
        self._clear_temporary_data_folder()

        # execute a read query on database
        return self._db_connector.read_data(
            market='forex',
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end,
            comparison_column_name=comparison_column_name,
            check_level=check_level,
            comparison_operator=comparison_operator,
            comparison_aggregation_mode=aggregation_mode
        )

    def plot(
        self,
        ticker,
        timeframe,
        start_date,
        end_date
    ) -> None:
        """
        Plot candlestick chart for the specified ticker and date range.

        Generates an interactive candlestick chart using mplfinance, displaying
        OHLC (Open, High, Low, Close) data for the specified time period.

        Args:
            ticker (str): Currency pair symbol (e.g., 'EURUSD', 'GBPUSD')
            timeframe (str): Candle timeframe (e.g., '1m', '5m', '1h', '1D', '1W')
            start_date (str): Start date in ISO format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
            end_date (str): End date in ISO format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'

        Returns:
            None: Displays the chart using matplotlib

        Example:
            >>> manager = HistoricalManagerDB(config='data_config.yaml')
            >>> manager.plot(
            ...     ticker='EURUSD',
            ...     timeframe='1D',
            ...     start_date='2020-01-01',
            ...     end_date='2020-12-31'
            ... )

        Note:
            The chart will be displayed in a matplotlib window. The data is automatically
            fetched using get_data() and converted to the appropriate format for plotting.
        """

        chart_data = self.get_data(ticker=ticker,
                                   timeframe=timeframe,
                                   start=start_date,
                                   end=end_date)

        chart_data = to_pandas_dataframe(chart_data)

        if chart_data.index.name != BASE_DATA_COLUMN_NAME.TIMESTAMP:

            chart_data.set_index(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                 inplace=True)

            chart_data.index = to_datetime(chart_data.index)

        else:
            logger.bind(target='histmanager').trace(f'Chart data already has {BASE_DATA_COLUMN_NAME.TIMESTAMP} as index')

        # candlestick chart type
        # use mplfinance
        chart_kwargs = dict(style='charles',
                            title=ticker,
                            ylabel='Quotation',
                            xlabel='Timestamp',
                            volume=False,
                            figratio=(12, 8),
                            figscale=1
                            )

        mpf_plot(chart_data, type='candle', **chart_kwargs)

        mpf_show()

    def close(self):

        self._db_connector.save_tickers_years_info(self._tickers_years_dict)
