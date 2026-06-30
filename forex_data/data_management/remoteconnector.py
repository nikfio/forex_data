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
import socket
import ssl
import struct
from uuid import uuid4
import requests
from requests import Session
from io import BytesIO
from zipfile import ZipFile, ZipExtFile, BadZipFile
from textwrap import dedent
from bs4 import BeautifulSoup
import pandas as pd
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
    duration,
    timestamp as pyarrow_timestamp
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
    COLUMN_NAME,
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
    collect_lazyframe,
)

from ..config import _apply_config


@define(kw_only=True, slots=True)
class RemoteConnector:

    # interface parameters
    data_path: Union[str, Path] = field(default='', validator=validators.or_(
        validators.instance_of(str), validators.instance_of(Path)))
    data_type: str = field(default='parquet',
                           validator=validators.in_(SUPPORTED_DATA_FILES))
    engine: str = field(default='polars_lazy',
                        validator=validators.in_(SUPPORTED_DATA_ENGINES))
    polars_gpu_engine: bool = field(default=False,
                                    validator=validators.instance_of(bool))
    volume_data: bool = field(default=False,
                              validator=validators.instance_of(bool))

    # internal parameters
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

        # Clean up any orphaned temp directories from previous crashed sessions
        # that are older than 15 minutes.
        temp_root = self.data_path / TEMP_FOLDER
        if temp_root.exists() and temp_root.is_dir():
            import time
            now = time.time()
            for p in temp_root.iterdir():
                if p.is_dir() and p != self._temporary_data_path:
                    try:
                        # If the folder is older than 15 minutes, clean it up
                        if now - p.stat().st_mtime > 900:
                            shutil.rmtree(p)
                    except Exception:
                        pass

            try:
                # If the root temp directory is empty, remove it
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

    # interface parameters
    ssl_verify: bool = field(default=True, validator=validators.instance_of(bool))

    # internal parameters
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

        self._temporary_data_path.mkdir(parents=True, exist_ok=True)

        try:
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
        finally:
            self.clear_temporary_folder()

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

            df = read_csv(
                'pandas',
                raw_file,
                sep=',',
                names=[COLUMN_NAME.TIMESTAMP, COLUMN_NAME.ASK, COLUMN_NAME.BID, COLUMN_NAME.VOLUME],
                dtype={COLUMN_NAME.ASK: 'float32', COLUMN_NAME.BID: 'float32', COLUMN_NAME.VOLUME: 'float32'},
                parse_dates=[0],
                date_format=DATE_FORMAT_HISTDATA_CSV,
                engine='c'
            )

            # Localize and convert timezone from America/New_York (EST/EDT) to UTC
            df[COLUMN_NAME.TIMESTAMP] = (
                pd.to_datetime(df[COLUMN_NAME.TIMESTAMP])
                .dt.tz_localize('America/New_York', ambiguous='NaT', nonexistent='NaT')
                .dt.tz_convert('UTC')
                .dt.tz_localize(None)
            )
            df = df.dropna(subset=[COLUMN_NAME.TIMESTAMP])

            # calculate ask_volume, bid_volume, vwmp
            df[COLUMN_NAME.ASK_VOLUME] = 0.0
            df[COLUMN_NAME.BID_VOLUME] = 0.0
            df[COLUMN_NAME.VWMP] = (df[COLUMN_NAME.ASK] + df[COLUMN_NAME.BID]) / 2

            # select columns matching TICK_DATA schema
            df = df[DATA_COLUMN_NAMES.TICK_DATA]

        elif engine == 'pyarrow':

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
                column_names=[COLUMN_NAME.TIMESTAMP, COLUMN_NAME.ASK, COLUMN_NAME.BID, COLUMN_NAME.VOLUME]
            )

            parse_opts = arrow_csv.ParseOptions(
                delimiter=','
            )

            modtypes = {
                COLUMN_NAME.TIMESTAMP: pyarrow_string(),
                COLUMN_NAME.ASK: pyarrow_float32(),
                COLUMN_NAME.BID: pyarrow_float32(),
                COLUMN_NAME.VOLUME: pyarrow_float32()
            }

            convert_opts = arrow_csv.ConvertOptions(
                column_types=modtypes
            )

            # at first read file with timestamp as a string
            df = read_csv(
                'pyarrow',
                temp_filepath,
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=convert_opts
            )

            # convert timestamp string array to pyarrow timestamp('ms')
            mod_format = DATE_FORMAT_HISTDATA_CSV.removesuffix('%f')
            ts2 = pc.strptime(pc.utf8_slice_codeunits(
                df[COLUMN_NAME.TIMESTAMP], 0, 15), format=mod_format, unit="ms")
            d = pc.utf8_slice_codeunits(df[COLUMN_NAME.TIMESTAMP],
                                        15,
                                        99).cast(pyarrow_int64()).cast(duration("ms"))
            timecol = pc.add(ts2, d)

            # Localize and convert timezone from America/New_York (EST/EDT) to UTC
            timecol_aware = pc.assume_timezone(timecol, 'America/New_York', ambiguous='earliest', nonexistent='earliest')
            timecol_utc = pc.cast(timecol_aware, pyarrow_timestamp('ms', tz='UTC'))
            timecol = pc.cast(timecol_utc, pyarrow_timestamp('ms', tz=None))

            # calculate ask_volume, bid_volume, vwmp
            ask_volume = pc.cast(pc.multiply(df[COLUMN_NAME.VOLUME], 0.0), pyarrow_float32())
            bid_volume = pc.cast(pc.multiply(df[COLUMN_NAME.VOLUME], 0.0), pyarrow_float32())
            vwmp = pc.divide(
                pc.add_checked(df[COLUMN_NAME.ASK], df[COLUMN_NAME.BID]),
                2
            )

            # aggregate in a new table matching pyarrow schema
            df = Table.from_arrays(
                [
                    timecol,
                    df[COLUMN_NAME.ASK],
                    df[COLUMN_NAME.BID],
                    ask_volume,
                    bid_volume,
                    vwmp
                ],
                schema=schema(PYARROW_DTYPE_DICT.TIME_TICK_DTYPE.copy().items())
            )

        elif engine == 'polars':

            # download to temporary csv file
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

            try:
                # Fast path: try reading directly as Float32
                raw_file_dtypes = {
                    COLUMN_NAME.TIMESTAMP: polars_string,
                    COLUMN_NAME.ASK: PolarsFloat32,
                    COLUMN_NAME.BID: PolarsFloat32,
                    COLUMN_NAME.VOLUME: PolarsFloat32
                }

                df = read_csv(
                    'polars',
                    temp_filepath,
                    separator=',',
                    has_header=False,
                    new_columns=[COLUMN_NAME.TIMESTAMP, COLUMN_NAME.ASK, COLUMN_NAME.BID, COLUMN_NAME.VOLUME],
                    schema_overrides=raw_file_dtypes,
                    use_pyarrow=True
                )
                df = df.with_columns(
                    col(COLUMN_NAME.TIMESTAMP).str.strptime(
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
                    'column_1': COLUMN_NAME.TIMESTAMP,
                    'column_2': COLUMN_NAME.ASK,
                    'column_3': COLUMN_NAME.BID,
                    'column_4': COLUMN_NAME.VOLUME
                })
                df = df.with_columns([
                    col(COLUMN_NAME.TIMESTAMP).str.strptime(
                        PolarsDatetime('ms'),
                        format=DATE_FORMAT_HISTDATA_CSV
                    ),
                    col(COLUMN_NAME.ASK).str.strip_chars().cast(PolarsFloat32),
                    col(COLUMN_NAME.BID).str.strip_chars().cast(PolarsFloat32),
                    col(COLUMN_NAME.VOLUME).str.strip_chars().cast(PolarsFloat32)
                ])

            # Localize and convert timezone from America/New_York (EST/EDT) to UTC
            df = df.with_columns(
                col(COLUMN_NAME.TIMESTAMP)
                .dt.replace_time_zone('America/New_York', ambiguous='earliest', non_existent='null')
                .dt.convert_time_zone('UTC')
                .dt.replace_time_zone(None)
            ).filter(col(COLUMN_NAME.TIMESTAMP).is_not_null())

            # calculate ask_volume, bid_volume, vwmp
            df = df.with_columns([
                (col(COLUMN_NAME.VOLUME) * 0.0).alias(COLUMN_NAME.ASK_VOLUME),
                (col(COLUMN_NAME.VOLUME) * 0.0).alias(COLUMN_NAME.BID_VOLUME),
                ((col(COLUMN_NAME.ASK) + col(COLUMN_NAME.BID)) / 2).alias(COLUMN_NAME.VWMP)
            ])

            # final cast to standard dtypes
            df = df.select(DATA_COLUMN_NAMES.TICK_DATA).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)

            # clean duplicated timestamps rows, keep first by default
            df = df.unique(subset=[COLUMN_NAME.TIMESTAMP],
                           keep='first')

            # remove business days
            df = business_days_data(df)

        elif engine == 'polars_lazy':

            # download to temporary csv file
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

            try:
                # Fast path: try reading directly as Float32
                raw_file_dtypes = {
                    COLUMN_NAME.TIMESTAMP: polars_string,
                    COLUMN_NAME.ASK: PolarsFloat32,
                    COLUMN_NAME.BID: PolarsFloat32,
                    COLUMN_NAME.VOLUME: PolarsFloat32
                }

                df = read_csv(
                    'polars_lazy',
                    temp_filepath,
                    separator=',',
                    has_header=False,
                    new_columns=[COLUMN_NAME.TIMESTAMP, COLUMN_NAME.ASK, COLUMN_NAME.BID, COLUMN_NAME.VOLUME],
                    schema_overrides=raw_file_dtypes
                )
                df = df.with_columns(
                    col(COLUMN_NAME.TIMESTAMP).str.strptime(
                        PolarsDatetime('ms'),
                        format=DATE_FORMAT_HISTDATA_CSV
                    )
                )
                df = collect_lazyframe(df, self.polars_gpu_engine).lazy()

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
                    'column_1': COLUMN_NAME.TIMESTAMP,
                    'column_2': COLUMN_NAME.ASK,
                    'column_3': COLUMN_NAME.BID,
                    'column_4': COLUMN_NAME.VOLUME
                })
                df = df.with_columns([
                    col(COLUMN_NAME.TIMESTAMP).str.strptime(
                        PolarsDatetime('ms'),
                        format=DATE_FORMAT_HISTDATA_CSV
                    ),
                    col(COLUMN_NAME.ASK).str.strip_chars().cast(PolarsFloat32),
                    col(COLUMN_NAME.BID).str.strip_chars().cast(PolarsFloat32),
                    col(COLUMN_NAME.VOLUME).str.strip_chars().cast(PolarsFloat32)
                ])
                df = collect_lazyframe(df, self.polars_gpu_engine).lazy()

            # Localize and convert timezone from America/New_York (EST/EDT) to UTC
            df = df.with_columns(
                col(COLUMN_NAME.TIMESTAMP)
                .dt.replace_time_zone('America/New_York', ambiguous='earliest', non_existent='null')
                .dt.convert_time_zone('UTC')
                .dt.replace_time_zone(None)
            ).filter(col(COLUMN_NAME.TIMESTAMP).is_not_null())

            # calculate ask_volume, bid_volume, vwmp
            df = df.with_columns([
                (col(COLUMN_NAME.VOLUME) * 0.0).alias(COLUMN_NAME.ASK_VOLUME),
                (col(COLUMN_NAME.VOLUME) * 0.0).alias(COLUMN_NAME.BID_VOLUME),
                ((col(COLUMN_NAME.ASK) + col(COLUMN_NAME.BID)) / 2).alias(COLUMN_NAME.VWMP)
            ])

            # final cast to standard dtypes
            df = df.select(DATA_COLUMN_NAMES.TICK_DATA).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)

            # clean duplicated timestamps rows, keep first by default
            df = df.unique(subset=[COLUMN_NAME.TIMESTAMP],
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

    # interface parameters
    ssl_verify: bool = field(default=True, validator=validators.instance_of(bool))

    # internal parameters
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

        # Ensure temporary data path exists and reconfigure tick_vault to use it
        self._temporary_data_path.mkdir(parents=True, exist_ok=True)
        try:
            from tick_vault import reload_config
            reload_config(
                base_directory=str(self._temporary_data_path),
                worker_per_proxy=3,
                fetch_max_retry_attempts=3,
            )
        except ImportError:
            pass

        try:
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
            pl_df = pl_df.rename({"time": COLUMN_NAME.TIMESTAMP})

            # Calculate vwmp (volume weighted mid price)
            pl_df = pl_df.with_columns([
                pl.col(COLUMN_NAME.ASK_VOLUME).cast(pl.Float32),
                pl.col(COLUMN_NAME.BID_VOLUME).cast(pl.Float32),
                ((pl.col(COLUMN_NAME.BID) * pl.col(COLUMN_NAME.ASK_VOLUME) + pl.col(COLUMN_NAME.ASK) * pl.col(COLUMN_NAME.BID_VOLUME)) / 
                 (pl.col(COLUMN_NAME.ASK_VOLUME) + pl.col(COLUMN_NAME.BID_VOLUME))).cast(pl.Float32).alias(COLUMN_NAME.VWMP)
            ])

            # Cast to required TICK schema
            pl_df = pl_df.select(list(POLARS_DTYPE_DICT.TIME_TICK_DTYPE.keys())).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)

            # Deduplicate on timestamp and sort chronologically
            pl_df = pl_df.unique(subset=[COLUMN_NAME.TIMESTAMP], keep='first', maintain_order=True).sort(COLUMN_NAME.TIMESTAMP)

            # Filter out business days/hours using standard helper
            pl_df = business_days_data(pl_df)

            return pl_df if engine == 'polars_lazy' else collect_lazyframe(pl_df, self.polars_gpu_engine)
        finally:
            self.clear_temporary_folder()

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

        # Ensure temporary data path exists and reconfigure tick_vault to use it
        self._temporary_data_path.mkdir(parents=True, exist_ok=True)
        try:
            from tick_vault import reload_config
            reload_config(
                base_directory=str(self._temporary_data_path),
                worker_per_proxy=3,
                fetch_max_retry_attempts=3,
            )
        except ImportError:
            pass

        try:
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
            pl_df = pl_df.rename({"time": COLUMN_NAME.TIMESTAMP})
            pl_df = pl_df.with_columns([
                pl.col(COLUMN_NAME.ASK_VOLUME).cast(pl.Float32),
                pl.col(COLUMN_NAME.BID_VOLUME).cast(pl.Float32),
                ((pl.col(COLUMN_NAME.BID) * pl.col(COLUMN_NAME.ASK_VOLUME) + pl.col(COLUMN_NAME.ASK) * pl.col(COLUMN_NAME.BID_VOLUME)) / 
                 (pl.col(COLUMN_NAME.ASK_VOLUME) + pl.col(COLUMN_NAME.BID_VOLUME))).cast(pl.Float32).alias(COLUMN_NAME.VWMP)
            ])
            pl_df = pl_df.select(list(POLARS_DTYPE_DICT.TIME_TICK_DTYPE.keys())).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
            pl_df = pl_df.unique(subset=[COLUMN_NAME.TIMESTAMP], keep='first', maintain_order=True).sort(COLUMN_NAME.TIMESTAMP)
            pl_df = business_days_data(pl_df)

            # Reframe data if timeframe is not TICK
            if timeframe.lower() != TICK_TIMEFRAME.lower():
                # reframe_data
                pl_df = reframe_data(pl_df, timeframe)

            return pl_df if engine == 'polars_lazy' else collect_lazyframe(pl_df, self.polars_gpu_engine)
        finally:
            self.clear_temporary_folder()


# REAL-TIME DATABASE CONNECTOR CLASS


@define(kw_only=True, slots=True)
class TwelveDataConnector(RemoteConnector):
    """
    Class to read real-time data from the database using TwelveData API.
    """

    # interface parameters
    api_key: str = field(default='', validator=validators.instance_of(str))
    plan: str = field(
        default='',
        validator=validators.and_(
            validators.instance_of(str),
            validators.in_(TWELVEDATA_PROVIDER_PLAN_LIST)
        ),
        converter=str.lower
    )

    # internal parameters
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

        # set up log sink for twelvedata connector
        log_path = Path(self.data_path) / 'log' / 'twelvedata.log'

        # apy key if not defined get it from env variable TWELVEDATA_API_KEY
        if not self.api_key:
            self.api_key = os.environ.get("TWELVE_DATA_API_KEY", "")

        if not self.api_key:
            raise ValueError("API key is required for TwelveDataConnector")

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
        return self._chunk_size

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
            "price": float(data["price"]),
            "timezone": "UTC"
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
            "outputsize": self._chunk_size,
            "timezone": "UTC"
        }

        data = self._execute_request("time_series", params)

        if "values" not in data:
            logger.bind(target='twelvedata').warning(
                f"Twelve Data response did not contain 'values': {data}")
            return PolarsLazyFrame({})

        lf = PolarsLazyFrame(data["values"])
        tf_schema = POLARS_DTYPE_DICT.TIME_TF_DTYPE

        # Convert timestamp to America/New_York to filter weekends with DST
        # Market closes Friday 17:00 NY time and opens Sunday 17:00 NY time
        ny_time = (
            pl.col("timestamp")
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("America/New_York")
        )
        is_weekend = (
            (ny_time.dt.weekday() == 6)
            | ((ny_time.dt.weekday() == 5) & (ny_time.dt.hour() >= 17))
            | ((ny_time.dt.weekday() == 7) & (ny_time.dt.hour() < 17))
        )

        # return data ordered by timestamp in ascending order
        return (
            lf.with_columns([
                pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("timestamp"),
                pl.col("open").cast(pl.Float32),
                pl.col("high").cast(pl.Float32),
                pl.col("low").cast(pl.Float32),
                pl.col("close").cast(pl.Float32),
                pl.col("close").cast(pl.Float32).alias(COLUMN_NAME.ASK),
                pl.col("close").cast(pl.Float32).alias(COLUMN_NAME.BID),
                pl.lit(0.0).cast(pl.Float32).alias(COLUMN_NAME.ASK_VOLUME),
                pl.lit(0.0).cast(pl.Float32).alias(COLUMN_NAME.BID_VOLUME),
                pl.col("close").cast(pl.Float32).alias(COLUMN_NAME.VWMP),
                pl.col("close").cast(pl.Float32).alias(COLUMN_NAME.VWMP_AVG)
            ])
            .filter(~is_weekend)
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

        # Check if now is weekend in New York timezone
        import zoneinfo
        ny_tz = zoneinfo.ZoneInfo("America/New_York")
        now_ny = datetime.now(ny_tz)
        wkday = now_ny.weekday()  # Mon=0, ..., Sun=6
        is_wknd = (
            (wkday == 5)
            or (wkday == 4 and now_ny.hour >= 17)
            or (wkday == 6 and now_ny.hour < 17)
        )

        params = {
            "symbol": symbol,
            "interval": timeframe,
            "outputsize": self._chunk_size,
            "timezone": "UTC"
        }

        if is_wknd:
            # Set end_date to last active time (most recent Friday at 17:00 NY time)
            # wkday - 4 subtracts 0 days for Friday, 1 for Saturday, 2 for Sunday
            last_active_ny = (
                now_ny.replace(hour=17, minute=0, second=0, microsecond=0)
                - timedelta(days=wkday - 4)
            )
            # Convert to UTC as strings
            last_active_utc = last_active_ny.astimezone(zoneinfo.ZoneInfo("UTC"))
            start_dt = last_active_utc - interval_window

            params["end_date"] = last_active_utc.strftime("%Y-%m-%d %H:%M:%S")
            params["start_date"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")

        data = self._execute_request("time_series", params)

        if "values" not in data:
            logger.bind(target='twelvedata').warning(
                f"Twelve Data response did not contain 'values': {data}")
            return PolarsLazyFrame({})

        lf = PolarsLazyFrame(data["values"])
        tf_schema = POLARS_DTYPE_DICT.TIME_TF_DTYPE

        processed_lf = lf.with_columns([
            pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("timestamp"),
            pl.col("open").cast(pl.Float32),
            pl.col("high").cast(pl.Float32),
            pl.col("low").cast(pl.Float32),
            pl.col("close").cast(pl.Float32),
            pl.col("close").cast(pl.Float32).alias(COLUMN_NAME.ASK),
            pl.col("close").cast(pl.Float32).alias(COLUMN_NAME.BID),
            pl.lit(0.0).cast(pl.Float32).alias(COLUMN_NAME.ASK_VOLUME),
            pl.lit(0.0).cast(pl.Float32).alias(COLUMN_NAME.BID_VOLUME),
            pl.col("close").cast(pl.Float32).alias(COLUMN_NAME.VWMP),
            pl.col("close").cast(pl.Float32).alias(COLUMN_NAME.VWMP_AVG)
        ])

        # Convert timestamp to America/New_York to filter weekends with DST
        # Market closes Friday 17:00 NY time and opens Sunday 17:00 NY time
        ny_time = (
            pl.col("timestamp")
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("America/New_York")
        )
        is_weekend = (
            (ny_time.dt.weekday() == 6)
            | ((ny_time.dt.weekday() == 5) & (ny_time.dt.hour() >= 17))
            | ((ny_time.dt.weekday() == 7) & (ny_time.dt.hour() < 17))
        )

        # Filter out weekend data first
        processed_lf = processed_lf.filter(~is_weekend)

        # Set the cutoff to start from the most recent timestamp of the retrieved data
        max_ts_df = collect_lazyframe(processed_lf.select(pl.col("timestamp").max()), self.polars_gpu_engine)
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


@define(kw_only=True, slots=True)
class cTraderDataConnector(RemoteConnector):
    """
    Connector class to retrieve market data from cTrader OpenAPI.
    """
    client_id: str = field(default='', validator=validators.instance_of(str))
    client_secret: str = field(default='', validator=validators.instance_of(str))
    access_token: str = field(default='', validator=validators.instance_of(str))
    refresh_token: str = field(default='', validator=validators.instance_of(str))
    broker_account_id: str = field(default='', validator=validators.instance_of(str))
    broker_account_is_demo: bool = field(default=True, validator=validators.instance_of(bool))

    _socket: Any = field(init=False, default=None)
    _symbol_name_to_id: Dict[str, int] = field(init=False, factory=dict)
    _asset_id_to_name: Dict[int, str] = field(init=False, factory=dict)

    def __init__(self, **kwargs: Any) -> None:
        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)

        if not _apply_config(self, kwargs, _class_attributes_name, _not_assigned_attrs_index_mask):
            self.__attrs_init__(**kwargs)
        else:
            self.__attrs_post_init__(**kwargs)
        validate(self)

    def __attrs_post_init__(self, **kwargs: Any) -> None:
        super().__attrs_post_init__()
        
        # Log setup
        log_path = Path(self.data_path) / 'log' / 'ctrader.log'
        logger.add(
            log_path,
            level="TRACE",
            rotation="5 MB",
            filter=lambda r: r['extra'].get('target') == 'ctrader'
        )

        # Fallbacks to env
        if not self.client_id:
            self.client_id = os.environ.get("CTRADER_CLIENT_ID", "")
        if not self.client_secret:
            self.client_secret = os.environ.get("CTRADER_CLIENT_SECRET", "")
        if not self.access_token:
            self.access_token = os.environ.get("CTRADER_ACCESS_TOKEN", "")
        if not self.refresh_token:
            self.refresh_token = os.environ.get("CTRADER_REFRESH_ACCESS_TOKEN", "")
        if not self.broker_account_id:
            self.broker_account_id = os.environ.get("CTRADER_ACCOUNT_ID", "")

        if not self.client_id or not self.client_secret or not self.access_token or not self.broker_account_id:
            raise ValueError("cTrader credentials and account ID are required.")

        self.connect()

    def connect(self) -> None:
        if self._socket is not None:
            return
            
        host = "demo.ctraderapi.com" if self.broker_account_is_demo else "live.ctraderapi.com"
        port = 5035
        
        logger.bind(target='ctrader').info(f"Connecting to cTrader at {host}:{port}...")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(15.0)
            context = ssl.create_default_context()
            ssl_sock = context.wrap_socket(sock, server_hostname=host)
            ssl_sock.connect((host, port))
            self._socket = ssl_sock
        except Exception as e:
            logger.bind(target='ctrader').error(f"Failed to connect: {e}")
            raise ConnectionError(f"Failed to connect to cTrader: {e}") from e

        try:
            # 1. Application auth
            from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes
            app_req = ProtoOAApplicationAuthReq()
            app_req.clientId = self.client_id
            app_req.clientSecret = self.client_secret
            self._send_and_receive_raw(app_req, ProtoOAApplicationAuthRes)

            # 2. Account auth
            from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAAccountAuthReq, ProtoOAAccountAuthRes
            acc_req = ProtoOAAccountAuthReq()
            acc_req.ctidTraderAccountId = int(self.broker_account_id)
            acc_req.accessToken = self.access_token
            self._send_and_receive_raw(acc_req, ProtoOAAccountAuthRes)

            # 3. Symbols mapping
            from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOASymbolsListReq, ProtoOASymbolsListRes
            sym_req = ProtoOASymbolsListReq()
            sym_req.ctidTraderAccountId = int(self.broker_account_id)
            sym_res = self._send_and_receive_raw(sym_req, ProtoOASymbolsListRes)
            for s in sym_res.symbol:
                self._symbol_name_to_id[s.symbolName.upper()] = s.symbolId
        except Exception as e:
            self.close()
            raise e

    def check_connection(self) -> bool:
        try:
            self.connect()
            return self._socket is not None
        except Exception:
            return False

    def get_available_tickers(self) -> List[str]:
        return list(self._symbol_name_to_id.keys())

    def _send_and_receive_raw(self, request_msg: Any, expected_res_klass: Any) -> Any:
        ssl_sock = self._socket
        if ssl_sock is None:
            raise ConnectionError("Socket is not initialized.")
            
        from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoMessage
        
        proto_msg = ProtoMessage()
        proto_msg.payloadType = request_msg.payloadType
        proto_msg.payload = request_msg.SerializeToString()
        
        serialized = proto_msg.SerializeToString()
        length_header = struct.pack(">I", len(serialized))
        ssl_sock.sendall(length_header + serialized)
        
        while True:
            length_data = ssl_sock.recv(4)
            if not length_data or len(length_data) < 4:
                raise ConnectionError("Disconnected or incomplete header from cTrader.")
            length = struct.unpack(">I", length_data)[0]
            
            payload_data = b""
            while len(payload_data) < length:
                chunk = ssl_sock.recv(length - len(payload_data))
                if not chunk:
                    raise ConnectionError("Disconnected while reading payload.")
                payload_data += chunk
            
            res_proto_msg = ProtoMessage()
            res_proto_msg.ParseFromString(payload_data)
            
            from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAErrorRes
            if res_proto_msg.payloadType == expected_res_klass().payloadType:
                res_msg = expected_res_klass()
                res_msg.ParseFromString(res_proto_msg.payload)
                return res_msg
            elif res_proto_msg.payloadType == ProtoOAErrorRes().payloadType:
                err_msg = ProtoOAErrorRes()
                err_msg.ParseFromString(res_proto_msg.payload)
                raise ValueError(f"cTrader error: {err_msg.errorCode} - {err_msg.description}")

    def _send_and_receive(self, request_msg: Any, expected_res_klass: Any) -> Any:
        try:
            self.connect()
            return self._send_and_receive_raw(request_msg, expected_res_klass)
        except Exception:
            self.close()
            self.connect()
            return self._send_and_receive_raw(request_msg, expected_res_klass)

    def close(self) -> None:
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

    def get_data(self, symbol: str, timeframe: str, start_date: str, end_date: str) -> PolarsLazyFrame:
        if timeframe.upper() != "TICK":
            raise ValueError("cTraderDataConnector currently only supports TICK timeframe fetching.")

        symbol_norm = symbol.upper().replace("/", "")
        symbol_id = self._symbol_name_to_id.get(symbol_norm)
        if symbol_id is None:
            raise TickerNotFoundError(f"Symbol {symbol} not found in cTrader symbols.")

        start_dt = any_date_to_datetime64(start_date)
        end_dt = any_date_to_datetime64(end_date)

        # Break into 7-day windows to accommodate cTrader limits
        chunks = []
        chunk_start = start_dt
        while chunk_start < end_dt:
            chunk_end = min(chunk_start + timedelta(days=7), end_dt)
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end

        all_dfs = []
        for c_start, c_end in chunks:
            bid_df = self._fetch_ticks_type(symbol_id, 1, c_start, c_end)  # 1 = BID
            ask_df = self._fetch_ticks_type(symbol_id, 2, c_start, c_end)  # 2 = ASK
            
            if bid_df.height == 0 and ask_df.height == 0:
                continue

            merged = bid_df.join(ask_df, on="timestamp", how="full")
            merged = merged.with_columns(
                pl.coalesce(["timestamp", "timestamp_right"]).alias("timestamp")
            ).drop("timestamp_right").sort("timestamp")
            merged = merged.with_columns([
                pl.col("bid").forward_fill(),
                pl.col("ask").forward_fill()
            ]).drop_nulls(subset=["bid", "ask"])

            if merged.height == 0:
                continue

            # Populate required fields
            merged = merged.with_columns([
                pl.lit(0.0, dtype=pl.Float32).alias(COLUMN_NAME.ASK_VOLUME),
                pl.lit(0.0, dtype=pl.Float32).alias(COLUMN_NAME.BID_VOLUME),
                ((pl.col("ask") + pl.col("bid")) / 2.0).cast(pl.Float32).alias(COLUMN_NAME.VWMP)
            ])

            # Select and cast
            tick_schema = POLARS_DTYPE_DICT.TIME_TICK_DTYPE
            final_df = merged.select(list(tick_schema.keys())).cast(tick_schema)
            all_dfs.append(final_df)

        if not all_dfs:
            return PolarsLazyFrame(schema=POLARS_DTYPE_DICT.TIME_TICK_DTYPE)

        # Combine, sort, and deduplicate
        combined_df = pl.concat(all_dfs).unique(subset=[COLUMN_NAME.TIMESTAMP], keep='first', maintain_order=True).sort(COLUMN_NAME.TIMESTAMP)
        return business_days_data(combined_df.lazy())

    def _fetch_ticks_type(self, symbol_id: int, quote_type: int, start: datetime, end: datetime) -> pl.DataFrame:
        from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAGetTickDataReq, ProtoOAGetTickDataRes

        from_ts = int(start.timestamp() * 1000)
        to_ts = int(end.timestamp() * 1000)

        all_timestamps = []
        all_prices = []
        
        current_to = to_ts
        while current_to > from_ts:
            req = ProtoOAGetTickDataReq()
            req.ctidTraderAccountId = int(self.broker_account_id)
            req.symbolId = symbol_id
            req.type = quote_type
            req.fromTimestamp = from_ts
            req.toTimestamp = current_to

            res = self._send_and_receive(req, ProtoOAGetTickDataRes)
            ticks = res.tickData
            if not ticks:
                break

            # Reconstruct timestamps and prices
            current_time = ticks[0].timestamp
            chunk_timestamps = [current_time]
            chunk_prices = [ticks[0].tick / 100000.0]

            for tick in ticks[1:]:
                current_time -= tick.timestamp
                chunk_timestamps.append(current_time)
                chunk_prices.append(tick.tick / 100000.0)

            # Insert at the beginning (since we are traversing backwards newest-first)
            all_timestamps = chunk_timestamps + all_timestamps
            all_prices = chunk_prices + all_prices

            if res.hasMore:
                # Update current_to to the oldest tick timestamp in the chunk minus 1ms
                oldest_ts = chunk_timestamps[-1]
                current_to = oldest_ts - 1
            else:
                break

        col_name = "bid" if quote_type == 1 else "ask"
        if not all_timestamps:
            return pl.DataFrame(schema={"timestamp": pl.Datetime("ms"), col_name: pl.Float32})

        # Create localized datetime series from ms timestamps
        dt_series = pl.Series("timestamp", all_timestamps, dtype=pl.Int64).cast(pl.Datetime("ms"))
        return pl.DataFrame({
            "timestamp": dt_series,
            col_name: pl.Series(all_prices, dtype=pl.Float32)
        })

    def get_recent_data(self, symbol: str, timeframe: str, interval_window: timedelta) -> PolarsLazyFrame:
        end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_date = (datetime.now() - interval_window).strftime("%Y-%m-%d %H:%M:%S")
        return self.get_data(symbol, timeframe, start_date, end_date)
