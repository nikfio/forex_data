# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 00:02:36 2025

@author: fiora
"""

#
#     Module to connect to a database instance
#
#     Design constraint:
#
#         start with only support for polars, prefer lazyframe when possibile
#
#         read and write using polars dataframe or lazyframe
#         exec requests using SQL query language
#         OSS versions for windows required
#

import os
import shutil
import time
from uuid import uuid4
import requests
from requests import Session
from io import BytesIO
from zipfile import ZipFile, ZipExtFile, BadZipFile
from textwrap import dedent
from bs4 import BeautifulSoup
import polars as pl
from attrs import define, field, validators, validate
from typing import Any, Dict, List, Union
from pathlib import Path
from datetime import datetime, timedelta
from re import search

from pyarrow import (
    int64 as pyarrow_int64,
    string as pyarrow_string,
    BufferReader,
    csv as arrow_csv,
    compute as pc,
    schema,
    Table,
    duration
)

from loguru import logger

from .common import (
    PolarsDataFrame,
    PolarsLazyFrame,
    PolarsFloat32,
    DATE_FORMAT_SQL,
    TEMP_FOLDER,
    TEMP_CSV_FILE,
    SUPPORTED_DATA_FILES,
    SUPPORTED_DATA_ENGINES,
    DATA_COLUMN_NAMES,
    DATA_FILE_COLUMN_INDEX,
    BASE_DATA_COLUMN_NAME,
    DATE_FORMAT_HISTDATA_CSV,
    HISTDATA_URL_TICKDATA_TEMPLATE,
    HISTDATA_BASE_DOWNLOAD_METHOD,
    HISTDATA_BASE_DOWNLOAD_URL,
    MONTHS,
    DTYPE_DICT,
    PYARROW_DTYPE_DICT,
    POLARS_DTYPE_DICT,
    TWELVE_DATA_CHUNK_SIZE,
    TWELVE_DATA_FREE_TIER_MINUTE_RATE_LIMIT,
    TWELVE_DATA_PRO_MINUTE_RATE_LIMIT,
    TWELVEDATA_PROVIDER_PLAN_LIST,
    TICK_TIMEFRAME,
    TWELVE_DATA_TIMEFRAMES,
    read_csv,
    PolarsDatetime,
    any_date_to_datetime64,
    business_days_data,
    reframe_data,
    TickerNotFoundError,
    TickerDataNotFoundError,
    TickerDataBadTypeException,
    get_attrs_names,
)

from ..config import _apply_config


@define(kw_only=True, slots=True)
class RemoteConnector:

    data_path: Union[str, Path] = field(default='', validator=validators.or_(
        validators.instance_of(str), validators.instance_of(Path)))
    data_type: str = field(default='parquet',
                           validator=validators.in_(SUPPORTED_DATA_FILES))
    engine: str = field(default='polars_lazy',
                        validator=validators.in_(SUPPORTED_DATA_ENGINES))

    _tickers_years_info_filepath = field(default=Path('.'))
    _temporary_data_path = field(default=Path('.'))

    def __init__(self, **kwargs: Any) -> None:

        pass

    def __attrs_post_init__(self) -> None:
        if self._temporary_data_path != Path('.'):
            return

        # create data folder if not exists
        self.data_path = Path(self.data_path).expanduser().resolve()
        if (
            not self.data_path.exists() or
            not self.data_path.is_dir()
        ):

            self.data_path.mkdir(parents=True,
                                 exist_ok=True)

        # Each instance gets its own unique temp subfolder under Temp/
        # so that parallel HistoricalManagerDB instances never share or
        # conflict on the same temporary directory.
        self._temporary_data_path = (
            self.data_path / TEMP_FOLDER / str(uuid4())
        )

        # remove any left over files in case of previous crash
        self.clear_temporary_folder()
        self._temporary_data_path.mkdir(parents=True, exist_ok=False)

    def connect(self) -> Any:
        """Connect to database - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement connect")

    def check_connection(self) -> bool:
        """Check database connection - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement check_connection")

    def clear_temporary_folder(self) -> None:
        try:
            import logging
            tv_logger = logging.getLogger("tick_vault")
            for handler in list(tv_logger.handlers):
                if isinstance(handler, logging.FileHandler):
                    handler.close()
                    tv_logger.removeHandler(handler)
        except Exception:
            pass

        if self._temporary_data_path.exists():
            try:
                shutil.rmtree(self._temporary_data_path)
            except Exception as e:
                logger.bind(target='dukascopy').warning(
                    f"Failed to delete temporary directory {self._temporary_data_path}: {e}"
                )
        temp_root = self.data_path / TEMP_FOLDER
        if temp_root.exists() and temp_root.is_dir():
            try:
                # If the directory is empty, remove it
                if not any(temp_root.iterdir()):
                    temp_root.rmdir()
            except Exception:
                pass

    def get_available_tickers(self) -> List[str]:
        """Get available tickers - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement get_available_tickers")

    def get_data(self, symbol: str, timeframe: str, start_date: str, end_date: str) -> PolarsLazyFrame:
        """Get data - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement get_data")

    def get_recent_data(self, symbol: str, timeframe: str, interval_window: timedelta) -> PolarsLazyFrame:
        """Get recent data - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement get_recent_data")

    def download_month_raw(
        self,
        ticker: str,
        year: int,
        month_num: int,
        engine: str = 'polars_lazy'
    ) -> Union[PolarsDataFrame, PolarsLazyFrame]:
        """Download a single month of tick data from histdata.com and
        parse it into a DataFrame.

        Scrapes the download token from the histdata page, POSTs to
        the download endpoint to retrieve the ZIP archive, extracts
        the CSV content, and converts it to a DataFrame using the
        specified engine.

        Parameters
        ----------
        ticker : str
            The ticker symbol for the currency pair (e.g. 'EURUSD').
        year : int
            The year of the data to download (e.g. 2023).
        month_num : int
            The month number (1-12) of the data to download.
        engine : str, optional
            The DataFrame engine to use: 'pandas' for pandas.DataFrame
            or 'polars' for PolarsDataFrame. Defaults to 'polars'.

        Returns
        -------
        Union[PolarsDataFrame, PolarsLazyFrame]
            The tick data loaded into a DataFrame of the specified engine.
        """
        raise NotImplementedError("Subclasses must implement download_month_raw")


# HISTORICAL DATA CONNECTOR CLASS (histdata.com)


@define(kw_only=True, slots=True)
class HistDataConnector(RemoteConnector):
    """
    Connector class that encapsulates all HTTP interactions with histdata.com.

    Wraps connectivity checks, ticker list scraping, download token extraction,
    and monthly tick data ZIP downloads behind a single remote
    connector interface.
    """

    ssl_verify: bool = field(default=True, validator=validators.instance_of(bool))
    _session: Session = field(factory=Session)
    _tickers_cache: List[str] = field(factory=list,
                                      validator=validators.instance_of(list))

    def __init__(self, **kwargs: Any) -> None:

        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)

        if not _apply_config(
                self,
                kwargs,
                _class_attributes_name,
                _not_assigned_attrs_index_mask):

            # no config file is defined
            # call generated init
            self.__attrs_init__(**kwargs)  # type: ignore[attr-defined]

        else:

            self.__attrs_post_init__(**kwargs)

        validate(self)

    def __attrs_post_init__(self, **kwargs: Any) -> None:

        super().__attrs_post_init__()

        # set up log sink for histdata connector
        log_path = self.data_path / 'log' / 'histdata.log'

        handlers_to_remove = []
        for handler_id, handler in logger._core.handlers.items():
            if hasattr(handler, '_sink') and hasattr(handler._sink, '_path'):
                if str(handler._sink._path) == str(log_path):
                    handlers_to_remove.append(handler_id)

        for handler_id in handlers_to_remove:
            try:
                logger.remove(handler_id)
            except ValueError:
                # Handler already removed by another thread
                pass

        logger.add(log_path,
                   level="TRACE",
                   rotation="5 MB",
                   filter=lambda record: ('histdata' == record['extra'].get('target') and
                                          bool(record["extra"].get('target'))))

        self.connect()

    def connect(self) -> None:
        """Configure the requests session with SSL settings."""
        self._session = Session()
        self._session.verify = self.ssl_verify
        if not self.ssl_verify:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def check_connection(self) -> bool:
        """Test connectivity to histdata.com."""
        url = "https://www.histdata.com/download-free-forex-data/?/ascii/1-minute-bar-quotes"
        try:
            self._session.head(url, timeout=5)
            return True
        except Exception as e:
            logger.bind(target='histdata').error(
                f'Failed to connect to {url}: {e}')
            return False

    def get_available_tickers(self) -> List[str]:
        """
        Scrape available tickers from histdata.com.

        Results are cached after the first successful call on this instance.

        Returns
        -------
        List[str]
            Sorted, deduplicated list of ticker symbols
            (e.g. ['EURUSD', 'GBPUSD', ...]).
        """
        if self._tickers_cache:
            return self._tickers_cache

        url = "https://www.histdata.com/download-free-forex-data/?/ascii/1-minute-bar-quotes"

        try:
            self._session.head(url, timeout=5)
        except Exception as e:
            logger.bind(target='histdata').error(
                f'Failed to connect to {url}: {e}')
            return []

        try:
            response = self._session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            tickers = []
            # Tickers are typically in links that lead to the pair's specific page
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Pattern check based on the observed links
                if "/ascii/1-minute-bar-quotes/" in href:
                    parts = href.split('/')
                    ticker = parts[-1]
                    # Validate it's a valid ticker (usually 6 chars like EURUSD)
                    if ticker and len(ticker) >= 6:
                        tickers.append(ticker.upper())

            self._tickers_cache = sorted(list(set(tickers)))
            return self._tickers_cache

        except Exception as e:
            logger.bind(target='histdata').error(
                f'Failed to retrieve tickers from HistData: {e}')
            return []

    def download_month_raw(
        self,
        ticker: str,
        year: int,
        month_num: int,
        engine: str = 'polars_lazy'
    ) -> Union[PolarsDataFrame, PolarsLazyFrame]:
        """
        Download a single month of tick data from histdata.com and
        parse it into a DataFrame.

        Scrapes the download token from the histdata page, POSTs to
        the download endpoint to retrieve the ZIP archive, extracts
        the CSV content, and converts it to a DataFrame using the
        specified engine.

        Parameters
        ----------
        ticker : str
            Forex pair symbol (e.g. 'eurusd').
        year : int
            Data year.
        month_num : int
            Month number (1-12).
        engine : str
            DataFrame engine to use for parsing.
            One of 'pandas', 'pyarrow', 'polars', 'polars_lazy'.

        Returns
        -------
        Union[PolarsDataFrame, PolarsLazyFrame]
            Parsed tick data DataFrame with columns:
            timestamp, ask, bid, vol, p.

        Raises
        ------
        TickerNotFoundError
            If the download token cannot be scraped (ticker not supported).
        TickerDataBadTypeException
            If the downloaded content is not a valid ZIP file.
        TickerDataNotFoundError
            If the ZIP archive contents cannot be extracted.
        TickerDataInvalidException
            If the extracted file type is unexpected.
        """

        temp_filepath = str(
            self._temporary_data_path /
            (f'{ticker}_' +
             f'{year}_' +
             f'{MONTHS[month_num - 1]}_' +
             TEMP_CSV_FILE)
        )

        url = HISTDATA_URL_TICKDATA_TEMPLATE.format(
            ticker=ticker.lower(),
            year=year,
            month_num=month_num
        )
        r = self._session.get(url)

        token = None
        try:
            token = search('id="tk" value="(.*?)"', r.text).groups()[0]
        except AttributeError:
            logger.bind(target='histdata').critical(
                f'token value was not found scraping '
                f'url {url}: {ticker} not existing or'
                f'not supported by histdata.com: {ticker} - '
                f'{year}-{MONTHS[month_num - 1]}')

        # If exception was caught, token will still be None
        if token is None:
            raise TickerNotFoundError(
                f"Ticker {ticker} not found or not supported by histdata.com")

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
        logger.bind(target='histdata').trace(
            f'{ticker} - {year} - {MONTHS[month_num - 1]}: downloading')
        r = self._session.request(
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
            logger.bind(target='histdata').error(
                dedent(f'''Data {ticker} - {year} - {MONTHS[month_num - 1]}: {e}
                           url: {url}'''))
            raise TickerDataBadTypeException(
                dedent(f'''Data {ticker} - {year} - {MONTHS[month_num - 1]} BadZipFile error: {e}
                           url: {url}'''))

        else:

            # extract and parse zip file content
            try:
                ExtFile = zf.open(zf.namelist()[0])
            except Exception as e:
                logger.bind(target='histdata').error(
                    f'{ticker} - {year} - {MONTHS[month_num - 1]}: '
                    f'not found or invalid download: {e}')
                raise TickerDataNotFoundError(
                    f"Data {ticker} - {year} - {MONTHS[month_num - 1]} not found or not supported by histdata.com")

            else:
                if isinstance(ExtFile, ZipExtFile):
                    return self._raw_zipfile_to_df(
                        ExtFile, temp_filepath, engine=engine
                    )
                else:
                    logger.bind(target='histdata').error(
                        f'{ticker} - {year} - {MONTHS[month_num - 1]}: '
                        f'data type not expected')
                    raise TickerDataBadTypeException(
                        f"Data {ticker} - {year} - {MONTHS[month_num - 1]} type not expected")

    def _raw_zipfile_to_df(
        self,
        raw_file: ZipExtFile,
        temp_filepath: str,
        engine: str = 'polars'
    ) -> Union[PolarsDataFrame, PolarsLazyFrame]:
        """
        Convert a raw ZIP-extracted file to a DataFrame.

        Handles engine-specific parsing for pandas, pyarrow, polars,
        and polars_lazy engines. Computes the 'p' (mid-price) column
        and performs deduplication and business-day filtering.

        Parameters
        ----------
        raw_file : ZipExtFile
            The opened file from the downloaded ZIP archive.
        temp_filepath : str
            Path for temporary CSV file used during parsing.
        engine : str, optional
            DataFrame engine to use, by default 'polars'.

        Returns
        -------
        Union[PolarsDataFrame, PolarsLazyFrame]
            Parsed tick data DataFrame.
        """
        from polars import (
            String as polars_string,
            col
        )
        from polars.exceptions import ComputeError

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
            try:
                # Fast path: try reading directly as Float32
                raw_file_dtypes = POLARS_DTYPE_DICT.TIME_TICK_DTYPE.copy()
                raw_file_dtypes.pop('p')
                raw_file_dtypes[BASE_DATA_COLUMN_NAME.TIMESTAMP] = polars_string

                df = read_csv(
                    'polars',
                    temp_filepath,
                    separator=',',
                    has_header=False,
                    new_columns=DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                    schema_overrides=raw_file_dtypes,
                    use_pyarrow=True
                )
                df = df.with_columns(
                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).str.strptime(
                        PolarsDatetime('ms'),
                        format=DATE_FORMAT_HISTDATA_CSV
                    )
                )

            except Exception as e:

                logger.bind(target='histdata').warning(f'Occurred Exception: {type(e).__name__} - {e}.'
                                                       'Trying remove trailing spaces method')

                # Safe path: fallback to parsing all as String, stripping whitespace, and casting
                df = read_csv(
                    'polars',
                    temp_filepath,
                    separator=',',
                    has_header=False,
                    schema={
                        'column_1': polars_string,
                        'column_2': polars_string,
                        'column_3': polars_string,
                        'column_4': polars_string
                    }
                )
                df = df.rename({
                    'column_1': 'timestamp',
                    'column_2': 'ask',
                    'column_3': 'bid',
                    'column_4': 'vol'
                })
                df = df.with_columns([
                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).str.strptime(
                        PolarsDatetime('ms'),
                        format=DATE_FORMAT_HISTDATA_CSV
                    ),
                    col('ask').str.strip_chars().cast(PolarsFloat32),
                    col('bid').str.strip_chars().cast(PolarsFloat32),
                    col('vol').str.strip_chars().cast(PolarsFloat32)
                ])

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
            try:
                # Fast path: try reading directly as Float32
                raw_file_dtypes = POLARS_DTYPE_DICT.TIME_TICK_DTYPE.copy()
                raw_file_dtypes.pop('p')
                raw_file_dtypes[BASE_DATA_COLUMN_NAME.TIMESTAMP] = polars_string

                df = read_csv(
                    'polars_lazy',
                    temp_filepath,
                    separator=',',
                    has_header=False,
                    new_columns=DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                    schema_overrides=raw_file_dtypes
                )
                df = df.with_columns(
                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).str.strptime(
                        PolarsDatetime('ms'),
                        format=DATE_FORMAT_HISTDATA_CSV
                    )
                )
                # Eagerly collect to force schema/parsing validation on all rows.
                # If there are parsing errors anywhere in the file, this will raise ComputeError.
                # Then convert it back to a LazyFrame so it matches the expected return type.
                df = df.collect().lazy()

            except Exception as e:
                logger.bind(target='histdata').warning(f'Occurred Exception: {type(e).__name__}\n'
                                                       f' {e}. \n'
                                                       'Trying remove trailing spaces method')
                # Safe path: fallback to parsing all as String, stripping whitespace, and casting
                df = read_csv(
                    'polars_lazy',
                    temp_filepath,
                    separator=',',
                    has_header=False,
                    schema={
                        'column_1': polars_string,
                        'column_2': polars_string,
                        'column_3': polars_string,
                        'column_4': polars_string
                    }
                )
                df = df.rename({
                    'column_1': 'timestamp',
                    'column_2': 'ask',
                    'column_3': 'bid',
                    'column_4': 'vol'
                })
                df = df.with_columns([
                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).str.strptime(
                        PolarsDatetime('ms'),
                        format=DATE_FORMAT_HISTDATA_CSV
                    ),
                    col('ask').str.strip_chars().cast(PolarsFloat32),
                    col('bid').str.strip_chars().cast(PolarsFloat32),
                    col('vol').str.strip_chars().cast(PolarsFloat32)
                ])
                df = df.collect().lazy()

            # calculate 'p'
            df = df.with_columns(
                ((col('ask') + col('bid')) / 2).alias('p')
            )

            # final cast to standard dtypes
            df = df.select([
                col('timestamp').cast(PolarsDatetime('ms')),
                col('ask').cast(PolarsFloat32),
                col('bid').cast(PolarsFloat32),
                col('vol').cast(PolarsFloat32),
                col('p').cast(PolarsFloat32)
            ])

            # clean duplicated timestamps rows, keep first by default
            df = df.unique(subset=[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                           keep='first')

            # remove business days
            df = business_days_data(df)

        else:

            logger.bind(target='histdata').error(f'Engine {engine} is not supported')
            raise TypeError

        # return dataframe
        return df


@define(kw_only=True, slots=True)
class DukascopyConnector(RemoteConnector):
    """
    Connector class that encapsulates all HTTP interactions with Dukascopy's historical datafeed.

    Wraps connectivity checks, ticker registry, and tick data downloading via tick_vault library
    behind a single RemoteConnector-derived interface.
    """

    ssl_verify: bool = field(default=True, validator=validators.instance_of(bool))
    _session: Session = field(factory=Session)
    _tickers_cache: List[str] = field(factory=list,
                                      validator=validators.instance_of(list))

    def __init__(self, **kwargs: Any) -> None:

        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)

        if not _apply_config(
                self,
                kwargs,
                _class_attributes_name,
                _not_assigned_attrs_index_mask):

            # no config file is defined
            # call generated init
            self.__attrs_init__(**kwargs)  # type: ignore[attr-defined]

        else:

            self.__attrs_post_init__(**kwargs)

        validate(self)

    def __attrs_post_init__(self, **kwargs: Any) -> None:

        super().__attrs_post_init__()

        # set up log sink for dukascopy connector
        log_path = self.data_path / 'log' / 'dukascopy.log'

        handlers_to_remove = []
        for handler_id, handler in logger._core.handlers.items():
            if hasattr(handler, '_sink') and hasattr(handler._sink, '_path'):
                if str(handler._sink._path) == str(log_path):
                    handlers_to_remove.append(handler_id)

        for handler_id in handlers_to_remove:
            try:
                logger.remove(handler_id)
            except ValueError:
                pass

        logger.add(log_path,
                   level="TRACE",
                   rotation="5 MB",
                   filter=lambda record: ('dukascopy' == record['extra'].get('target') and
                                          bool(record["extra"].get('target'))))

        self.connect()

    def connect(self) -> None:
        """Configure session and tick_vault base directory."""
        self._session = Session()
        self._session.verify = self.ssl_verify
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })
        if not self.ssl_verify:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Configure tick_vault if it's imported
        try:
            from tick_vault import reload_config
            reload_config(
                base_directory=str(self._temporary_data_path),
                worker_per_proxy=3,
                fetch_max_retry_attempts=3,
            )
        except ImportError:
            logger.bind(target='dukascopy').warning("tick_vault is not installed. Please install it to use DukascopyConnector.")

    def check_connection(self) -> bool:
        """Test connectivity to dukascopy.com."""
        url = "https://www.dukascopy.com/swiss/english/marketwatch/historical/"
        try:
            response = self._session.head(url, timeout=30)
            if response.status_code == 200:
                return True
        except Exception:
            pass

        try:
            response = self._session.get(url, timeout=30)
            return response.status_code == 200
        except Exception as e:

            logger.bind(target='dukascopy').error(
                f'Failed to connect to {url}: {e}')
            return False

    def get_available_tickers(self) -> List[str]:
        """
        Get list of available tickers from tick_vault's registry and Dukascopy's tools page.

        Returns
        -------
        List[str]
            Sorted, deduplicated list of ticker symbols (e.g. ['BTCUSD', 'EURUSD', ...]).
        """
        if self._tickers_cache:
            return self._tickers_cache

        tickers_set = set()

        # 1. Add tickers from local tick_vault constants if available
        try:
            from tick_vault.constants import PIPET_SIZE_REGISTRY
            for k in PIPET_SIZE_REGISTRY.keys():
                tickers_set.add(k.upper())
        except ImportError:
            logger.bind(target='dukascopy').warning("Failed to import pipet size registry from tick_vault.")

        # 2. Scrape tickers from Dukascopy tools page
        scrape_url = "https://www.dukascopy.com/swiss/english/marketwatch/historical/"
        try:
            from bs4 import BeautifulSoup
            import re
            response = self._session.get(scrape_url, timeout=30)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                links = soup.find_all('a', href=re.compile(r'/charts/'))
                for link in links:
                    href = link.get('href', '')
                    match = re.search(r'/charts/([a-z]{3,6}-[a-z]{3,6})/?', href, re.IGNORECASE)
                    if match:
                        ticker = match.group(1).replace('-', '').upper()
                        tickers_set.add(ticker)
        except Exception as e:
            logger.bind(target='dukascopy').error(f"Failed to scrape tickers from Dukascopy website: {e}")

        self._tickers_cache = sorted(list(tickers_set))
        return self._tickers_cache

    def download_month_raw(
        self,
        ticker: str,
        year: int,
        month_num: int,
        temp_filepath: str = '',
        engine: str = 'polars_lazy'
    ) -> Union[PolarsDataFrame, PolarsLazyFrame]:
        """
        Downloads tick data for a specific year and month from Dukascopy using tick_vault.

        Parameters
        -------
        ticker: str
            The currency pair (e.g., 'EURUSD').
        year: int
            Year of the data to download.
        month_num: int
            Month of the data (1-12).
        engine: str
            Either 'polars' or 'polars_lazy'.

        Returns
        -------
        Union[PolarsDataFrame, PolarsLazyFrame]
            Polars DataFrame or LazyFrame with column names matching HistDataConnector.
        """
        # Input validation
        ticker_upper = ticker.upper()
        if ticker_upper not in self.get_available_tickers():
            raise TickerNotFoundError(f"Ticker {ticker_upper} is not supported by Dukascopy.")

        if not isinstance(year, int) or year < 2000 or year > datetime.now().year + 1:
            raise ValueError(f"Invalid year: {year}")

        if not isinstance(month_num, int) or month_num < 1 or month_num > 12:
            raise ValueError(f"Invalid month number: {month_num}")

        if engine not in ('polars', 'polars_lazy'):
            raise ValueError(f"Unsupported engine: {engine}. Only 'polars' and 'polars_lazy' are supported.")

        try:
            import asyncio
            from tick_vault import download_range, read_tick_data
        except ImportError:
            logger.bind(target='dukascopy').error("tick_vault is not installed. Cannot download data.")
            raise RuntimeError("tick_vault is not installed.")

        # Determine start and end date for the month
        start = datetime(year, month_num, 1)
        if month_num == 12:
            end = datetime(year + 1, 1, 1)
        else:
            end = datetime(year, month_num + 1, 1)

        # Run async download process in the event loop
        logger.bind(target='dukascopy').info(f"Downloading {ticker_upper} for {year}-{month_num:02d}...")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        loop.run_until_complete(download_range(symbol=ticker_upper, start=start, end=end))

        # Read downloaded tick data into Pandas DataFrame
        logger.bind(target='dukascopy').info(f"Reading downloaded tick data for {ticker_upper}...")
        pandas_df = read_tick_data(symbol=ticker_upper, start=start, end=end, strict=False)

        if pandas_df.empty:
            logger.bind(target='dukascopy').warning(f"No data returned for {ticker_upper} {year}-{month_num}")
            empty_df = PolarsDataFrame(schema=POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
            return empty_df.lazy() if engine == 'polars_lazy' else empty_df

        # Convert to Polars LazyFrame
        pl_df = pl.from_pandas(pandas_df).lazy()

        # Rename columns to match TIME_TICK_DTYPE
        pl_df = pl_df.rename({"time": "timestamp"})

        # Calculate vol (ask_volume + bid_volume) and p (mid price)
        pl_df = pl_df.with_columns([
            (pl.col("ask_volume") + pl.col("bid_volume")).cast(pl.Float32).alias("vol"),
            ((pl.col("ask") + pl.col("bid")) / 2).cast(pl.Float32).alias("p")
        ])

        # Cast to required TICK schema
        pl_df = pl_df.select(list(POLARS_DTYPE_DICT.TIME_TICK_DTYPE.keys())).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)

        # Deduplicate on timestamp and sort chronologically
        pl_df = pl_df.unique(subset=["timestamp"], keep='first', maintain_order=True).sort("timestamp")

        # Filter out business days/hours using standard helper
        pl_df = business_days_data(pl_df)

        return pl_df if engine == 'polars_lazy' else pl_df.collect()

    def get_recent_data(
        self,
        symbol: str,
        timeframe: str,
        interval_window: timedelta,
        engine: str = 'polars_lazy'
    ) -> Union[PolarsDataFrame, PolarsLazyFrame]:
        """
        Fetches recent data relative to the current time minus the interval_window.

        Parameters
        -------
        symbol: str
            The currency pair (e.g. 'EURUSD').
        timeframe: str
            The target timeframe (e.g. 'TICK', '1m', '5m').
        interval_window: timedelta
            The duration of data to fetch (e.g., timedelta(days=90)).
        engine: str
            Either 'polars' or 'polars_lazy'.

        Returns
        -------
        Union[PolarsDataFrame, PolarsLazyFrame]
            Polars DataFrame or LazyFrame containing recent data.
        """
        symbol_upper = symbol.upper()
        if symbol_upper not in self.get_available_tickers():
            raise TickerNotFoundError(f"Ticker {symbol_upper} is not supported by Dukascopy.")

        if engine not in ('polars', 'polars_lazy'):
            raise ValueError(f"Unsupported engine: {engine}. Only 'polars' and 'polars_lazy' are supported.")

        try:
            import asyncio
            from tick_vault import download_range, read_tick_data
        except ImportError:
            logger.bind(target='dukascopy').error("tick_vault is not installed. Cannot fetch recent data.")
            raise RuntimeError("tick_vault is not installed.")

        from datetime import timezone
        # Subtract 2 hours from current UTC time because Dukascopy CDN historical files
        # are uploaded with some latency (usually up to 1-2 hours).
        end = datetime.now(timezone.utc) - timedelta(hours=2)

        # Roll back to Friday 21:00 UTC if the end time falls on a weekend.
        # Weekday: 0=Monday, ..., 4=Friday, 5=Saturday, 6=Sunday.
        # Market close: Friday 22:00 UTC. Market open: Sunday 22:00 UTC.
        wd = end.weekday()
        if (wd == 4 and end.hour >= 22) or (wd == 5) or (wd == 6 and end.hour < 22):
            days_to_subtract = {4: 0, 5: 1, 6: 2}[wd]
            end = end - timedelta(days=days_to_subtract)
            end = end.replace(hour=21, minute=0, second=0, microsecond=0)

            # In case the input end date is greater than the available range in the metadata DB,
            # use the last available date in the DB if one exists.
            try:
                from tick_vault.metadata import MetadataDB
                with MetadataDB() as db:
                    db._ensure_table_exists(symbol_upper)
                    table_name = db._get_table_name(symbol_upper)
                    cursor = db.conn.execute(
                        f"SELECT MAX(timestamp) FROM {table_name} WHERE has_data = 1"
                    )
                    row = cursor.fetchone()
                    if row and row[0] is not None:
                        last_available = datetime.fromtimestamp(row[0], tz=timezone.utc)
                        last_available_end = last_available
                        if end > last_available_end:
                            end = last_available_end
            except Exception as e:
                logger.bind(target='dukascopy').debug(
                    f"Could not check metadata database for last available range: {e}"
                )

        start = end - interval_window

        # Run async download process
        logger.bind(target='dukascopy').info(f"Downloading recent data for {symbol_upper} from {start} to {end}...")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        loop.run_until_complete(download_range(symbol=symbol_upper, start=start, end=end))

        # Read downloaded tick data
        pandas_df = read_tick_data(symbol=symbol_upper, start=start, end=end, strict=False)

        if pandas_df.empty:
            logger.bind(target='dukascopy').warning(f"No recent data returned for {symbol_upper}")
            empty_df = PolarsDataFrame(schema=POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
            return empty_df.lazy() if engine == 'polars_lazy' else empty_df

        # Convert to Polars LazyFrame
        pl_df = pl.from_pandas(pandas_df).lazy()
        pl_df = pl_df.rename({"time": "timestamp"})
        pl_df = pl_df.with_columns([
            (pl.col("ask_volume") + pl.col("bid_volume")).cast(pl.Float32).alias("vol"),
            ((pl.col("ask") + pl.col("bid")) / 2).cast(pl.Float32).alias("p")
        ])
        pl_df = pl_df.select(list(POLARS_DTYPE_DICT.TIME_TICK_DTYPE.keys())).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
        pl_df = pl_df.unique(subset=["timestamp"], keep='first', maintain_order=True).sort("timestamp")
        pl_df = business_days_data(pl_df)

        # Reframe data if timeframe is not TICK
        if timeframe.lower() != TICK_TIMEFRAME.lower():
            # reframe_data
            pl_df = reframe_data(pl_df, timeframe)

        return pl_df if engine == 'polars_lazy' else pl_df.collect()


# REAL-TIME DATABASE CONNECTOR CLASS


@define(kw_only=True, slots=True)
class RealTimeDBConnectorTwelveData(RemoteConnector):
    """
    Class to read real-time data from the database using TwelveData API.
    """

    api_key: str = field(default='', validator=validators.instance_of(str))
    plan: str = field(
        default='',
        validator=validators.and_(
            validators.instance_of(str),
            validators.in_(TWELVEDATA_PROVIDER_PLAN_LIST)
        ),
        converter=str.lower
    )

    _chunk_size: int = field(
        default=TWELVE_DATA_CHUNK_SIZE,
        validator=validators.instance_of(int)
    )
    _max_requests_per_minute: int = field(
        default=8,
        validator=validators.instance_of(int)
    )
    _request_timestamps: List[float] = field(factory=list, init=False)
    _base_url: str = field(default="https://api.twelvedata.com", init=False)

    @property
    def tier(self) -> str:
        """Alias for plan to avoid AttributeError: self.tier."""
        return self.plan

    def __init__(self, **kwargs: Any) -> None:

        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)

        if not _apply_config(
                self,
                kwargs,
                _class_attributes_name,
                _not_assigned_attrs_index_mask):

            # no config file is defined
            # call generated init
            self.__attrs_init__(**kwargs)  # type: ignore[attr-defined]

        else:

            self.__attrs_post_init__(**kwargs)

        validate(self)

    def __attrs_post_init__(self, **kwargs: Any) -> None:

        super().__attrs_post_init__()

        # set up log sink for dukascopy connector
        log_path = Path(self.data_path) / 'log' / 'twelvedata.log'

        # apy key if not defined get it from env variable TWELVEDATA_API_KEY
        if not self.api_key:
            self.api_key = os.environ.get("TWELVE_DATA_API_KEY", "")

        if not self.api_key:
            raise ValueError("API key is required for RealTimeDBConnectorTwelveData")

        # set default twelve data chunk size
        self._chunk_size = TWELVE_DATA_CHUNK_SIZE

        # Configure rate limits and safety margins based on tier
        if self.tier == "free":
            self._max_requests_per_minute = TWELVE_DATA_FREE_TIER_MINUTE_RATE_LIMIT
        else:  # 'paid' / 'grow' plan specifications
            self._max_requests_per_minute = TWELVE_DATA_PRO_MINUTE_RATE_LIMIT

        logger.add(log_path,
                   level="TRACE",
                   rotation="5 MB",
                   filter=lambda record: ('twelvedata' == record['extra'].get('target') and
                                          bool(record["extra"].get('target'))))

    @property
    def chunk_size(self) -> int:
        """Max number of data points per request."""
        return self._max_output_size

    @property
    def max_requests_per_minute(self) -> int:
        """Max number of requests per minute."""
        return self._max_requests_per_minute

    def _enforce_rate_limit(self) -> None:
        """Tracks requests internally and blocks execution if exceeding the rate limit."""
        now = time.time()
        # Evict timestamps older than 60 seconds
        self._request_timestamps = [t for t in self._request_timestamps if now - t < 60]

        if len(self._request_timestamps) >= self._max_requests_per_minute:
            # Calculate sleep required to let the oldest request clear the 60s window
            sleep_time = 60 - (now - self._request_timestamps[0]) + 0.1
            print(f"[Rate Limiter] Approaching {self.tier} tier threshold. Pausing for {sleep_time:.2f} seconds...")
            time.sleep(max(sleep_time, 0.1))

        self._request_timestamps.append(time.time())

    def _execute_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handles HTTP mechanics, rate limiting, and standard headers."""
        self._enforce_rate_limit()

        url = f"{self._base_url}/{endpoint}"
        params["apikey"] = self.api_key

        headers = {"Accept": "application/json"}
        response = requests.get(url, params=params, headers=headers)

        if response.status_code != 200:
            logger.bind(target='twelvedata').error(f"API Error [{response.status_code}]: {response.text}")

        data = response.json()
        if "status" in data and data["status"] == "error":
            logger.bind(target='twelvedata').error(f"Twelve Data Error: {data.get('message')}")

        return data

    def get_realtime_price(self, symbol: str) -> PolarsLazyFrame:
        """
        Fetches the instantaneous real-time price and outputs as a 1-row LazyFrame.
        """
        data = self._execute_request("price", {"symbol": symbol})
        record = {}

        if "price" not in data:
            logger.bind(target='twelvedata').error(
                f"Twelve Data response did not contain 'price': {data}")
            return {}

        record = {
            "timestamp": datetime.now(),
            "ticker": symbol,
            "price": float(data["price"])
        }
        return PolarsDataFrame([record]).lazy()

    def get_data(self, symbol: str, timeframe: str, start_date: str, end_date: str) -> PolarsLazyFrame:
        """
        Fetches historical data for a specific date range.
        Automatically checks boundaries for maximum output limits per call.
        """

        # Sanity check - check if timeframe is supported by Twelve Data
        if timeframe not in TWELVE_DATA_TIMEFRAMES:
            logger.bind(target='twelvedata').warning(
                f"Timeframe {timeframe} "
                f"is not supported by Twelve Data. "
                f"Supported: {', '.join(TWELVE_DATA_TIMEFRAMES)}")
            return PolarsLazyFrame({})

        start_dt = any_date_to_datetime64(start_date)
        if start_dt.year < 2026:
            logger.bind(target='twelvedata').error(
                "For real time connectors start_date cannot be before January 2026")
            raise ValueError("For real time connectors start_date cannot be before January 2026")

        params = {
            "symbol": symbol,
            "interval": timeframe,
            "start_date": start_date,
            "end_date": end_date,
            "outputsize": self._chunk_size
        }

        data = self._execute_request("time_series", params)

        if "values" not in data:
            logger.bind(target='twelvedata').warning(
                f"Twelve Data response did not contain 'values': {data}")
            return PolarsLazyFrame({})

        lf = PolarsLazyFrame(data["values"])
        tf_schema = POLARS_DTYPE_DICT.TIME_TF_DTYPE
        # return data ordered by timestamp in ascending order
        return (
            lf.with_columns([
                pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("timestamp"),
            ])
            .select(list(tf_schema.keys()))
            .cast(tf_schema)
            .sort("timestamp")
        )

    def get_recent_data(self, symbol: str, timeframe: str, interval_window: timedelta) -> PolarsLazyFrame:
        """
        Fetches recent data relative to the current time minus the interval_window.
        Example: Pass timedelta(days=90) to get the most recent rolling 3 months.
        """

        # Sanity check - check if timeframe is supported by Twelve Data
        if timeframe not in TWELVE_DATA_TIMEFRAMES:
            logger.bind(target='twelvedata').warning(
                f"Timeframe {timeframe} "
                f"is not supported by Twelve Data. "
                f"Supported: {', '.join(TWELVE_DATA_TIMEFRAMES)}")
            return PolarsLazyFrame({})

        params = {
            "symbol": symbol,
            "interval": timeframe,
            "outputsize": self._chunk_size,
        }

        data = self._execute_request("time_series", params)

        if "values" not in data:
            logger.bind(target='twelvedata').warning(
                f"Twelve Data response did not contain 'values': {data}")
            return PolarsLazyFrame({})

        lf = PolarsLazyFrame(data["values"])
        tf_schema = POLARS_DTYPE_DICT.TIME_TF_DTYPE

        processed_lf = lf.with_columns([
            pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("timestamp"),
        ])

        # Set the cutoff to start from the most recent timestamp of the retrieved data
        max_ts_df = processed_lf.select(pl.col("timestamp").max()).collect()
        if max_ts_df.height > 0 and max_ts_df.item(0, 0) is not None:
            cutoff_dt = max_ts_df.item(0, 0) - interval_window
        else:
            cutoff_dt = datetime.now() - interval_window

        # return data ordered by timestamp in ascending order
        return (
            processed_lf
            .select(list(tf_schema.keys()))
            .cast(tf_schema)
            .filter(pl.col("timestamp") >= cutoff_dt)
            .sort("timestamp")
        )
