# <span style="font-size:1.5em;">FOREX DATA</span>

The forex_data package offers ways to aggregate data from the Forex market into a dataframe having the the essential OHLC information, so the ouput will always have the columns:

* timestamp
* open
* high
* low
* close

The first purpose is to aggregate data in OHLC format and allow to have data in any timeframe specified in the most simple and efficient way.
The second purpose is to manage one or multiple sources, an interface layer will have primary functions with predefined name, inputs and ouput results: in order to ease the access and usage of multiple data sources.

At the moment, sources are divided in **historical sources** and **real-time sources**.

## SOURCES

### HISTORICAL SOURCE

A historical source is a source of data which makes data available but does not have a defined update policy for design reasons
On the contrary, it can provide a ton of history data, tipically from the first years of 2000s and the free tier is fine for the purposes of the package.

A perfect data source of this type is [histdata.com](http://www.histdata.com/), which work is really genuine and a lot appreciated.

We could see them as a model, maybe in the future a similar source can be found: that is why for generalization purpose this type of source is called *historical source*.

Summarizing, a historical source can provide tons of data even from many years ago and with no limits at the downside of a slow update rate. For example, *histdata* updates data on a montly basis.

### REAL-TIME SOURCE

A real-time source is what is more tipically known as a source for forex market or stock market data. It offers APIs in determined clients or even just a minimal documentation to establish the API call in HTTP request format.
A minimal free or trial offering is proposed, but they rely on premium subscriptions offers based on:

* real time performance 
* size of tickers list available
* how much history of a ticker 
* and many other parameters ...

As of now, just [alpha-vantage](https://www.alphavantage.co/documentation/) and [polygon-io](https://polygon.io/docs/forex/getting-started) are managed. The intention is to make the most out of them and their free tier access to data.

Even if free subscription is limitated for these providers, the reasons to include them in the package are to have closer real-time update than any historical source and also the module is designed to ease the work of studying a new provider API calls: a real time data manager uses at the same time all the remote sources available and provides access to their API through easier interface.

### Considerations

*What is the trade-off between historical and real-time source? And why a simultaneous usage of both is powerful?*

This question is the primary key of usefulness of the package.
An historical source like the one managed by the package, tipically updates data every month so you would have a delay of a month in retrieving the latest data, but on the upside you can have data from like 20 or more years ago to last month with a under a minute resolution.

A real-time source usually lets you get data limiting the number of candles of the output.
Also, tipically the source free subscription does not let to get data older than a month o few time more: especially if it requested with low resolution like 1-minute timeframe.
The real time source fills the gap of the month delay explained for the historical source.
And it is widely agreed that latest data have more influence on next trading positions to be set.

Concluding, the combination of historical and real-time source gives a 1-minute or lower resolution for data starting over 20 years ago approximately until yesterday or today data.


## INSTALLATION

The package is managed with Poetry, which is required to install the package.
Follow here for [installing Poetry](https://python-poetry.org/docs/).

Now, these steps should lead you to run the examples or any other package usage:

1. Open a shell (on Windows use [powershell7](https://learn.microsoft.com/it-it/powershell/scripting/install/installing-powershell-on-windows?view=powershell-7.4#msi)), clone the repository in a folder called `forex-data` (for example):
```
git clone https://github.com/nikfio/forex_data.git -b master forex-data
cd forex-data
```
2. Run poetry for package installation
```
poetry install
```
3. Run pytest to check everything is working fine
```
poetry run pytest
```

## CONFIGURATION FILE

A configuration file can be passed in order to group fixed parameters values.
In repository folder clone, look for [appconfig folder](appconfig) to see the [example template file](appconfig/appconfig_template.yaml).

At any run, the package looks for a file called `appconfig.yaml` and associates it to a variable called `APPCONFIG_FILE_YAML` so that it is simpler to use a default config file in package modules calls.

In data managers instantiation, you can pass the configuration file but any parameter value can be overridden by explicit assignment in object instantion.
The feature will be more clear following the [examples section](#examples).

#### ENGINE

Available options:

* pandas
* pyarrow
* polars

#### DATA_FILETYPE

Available options:

* csv
* parquet

*parquet* filetype is strongly suggested for read/write speed and disk space occupation.
Meanwhile, if you have any analysis application outside the Python environment, it would more likely accept csv files over parquet: so *csv* filetype could be a better choice for its broader acceptance.

#### DATA_PATH

The location where data downloaded or data reframed are cached.
Here you can pass a absolute folder path where the package will dump data downloaded with files type as assigned by the `DATA_FILETYPE` parameter.

The default locations used are:
* Historical source data cache path : `~/.database/HistoricalData`
* Real-time source data cache path : `~/.database/RealtimeData`

where `~` stands for the current user home folder location.
Beware that data caching is implemented for historical sources, next developments will cover also real-time sources data.

#### PROVIDERS_KEY

To use real-time sources you need to provide an API key.

Look here to register and create a key from Alpha-Vantage provider
[Alpha-Vantage free API registration](https://www.alphavantage.co/support/#api-key)

Look here to register and create a key from Polygon-IO provider
[Polygon-IO home page](https://polygon.io/)

## LOGGING

Logging feature is added via loguru library.
By construction log is dumped in a file which location is determined by pathlib.
A generic usage folder for the package named `.database` is created at the current user home folder.
Here log is dumped in a file called `forexdata.log`, the complete location of the log file will be:

`~/.database/forexdata.log`


## EXAMPLES

in [test folder](test) you can find examples showing the various modules or functionalities the package offers.

#### Historical data 

Let's walk through the [example for historical data source](test/test_hist_data_manager.py):

1. **data manager instance** 
    ```                            
    histmanager = historical_manager(
                    ticker='NZDUSD',
                    config_file=APPCONFIG_FILE_YAML
    )
    ```
    as mentioned in section [configuration](#configuration-file), you can see the feature of overriding the parameter `ticker` by direct assignment in object instantiation, meanwhile the remaining parameters value are assigned by the configuration file.
<br>

2. **get data**
    ```
    yeardata = histmanager.get_data(timeframe = '1h',
                                    start     = ex_start_date,
                                    end       = ex_end_date
    )
    ```
    the call returns a dataframe with data having timeframe, start and end specified by inputs assignment.
    The output data type is of type related to the engine selected.

    With `polars` as DATA_ENGINE option, the output dataframe logs
    ```
    ┌─────────────────────┬─────────┬─────────┬─────────┬─────────┐
    │ timestamp           ┆ open    ┆ high    ┆ low     ┆ close   │
    │ ---                 ┆ ---     ┆ ---     ┆ ---     ┆ ---     │
    │ datetime[ms]        ┆ f32     ┆ f32     ┆ f32     ┆ f32     │
    ╞═════════════════════╪═════════╪═════════╪═════════╪═════════╡
    │ 2009-10-04 17:00:00 ┆ 0.7172  ┆ 0.71735 ┆ 0.7165  ┆ 0.71665 │
    │ 2009-10-04 18:00:00 ┆ 0.71655 ┆ 0.7168  ┆ 0.71555 ┆ 0.71635 │
    │ 2009-10-04 19:00:00 ┆ 0.7164  ┆ 0.71665 ┆ 0.71385 ┆ 0.71445 │
    │ 2009-10-04 20:00:00 ┆ 0.7144  ┆ 0.7191  ┆ 0.71435 ┆ 0.71795 │
    │ 2009-10-04 21:00:00 ┆ 0.718   ┆ 0.72165 ┆ 0.71795 ┆ 0.72115 │
    │ …                   ┆ …       ┆ …       ┆ …       ┆ …       │
    │ 2009-12-03 06:00:00 ┆ 0.7248  ┆ 0.7256  ┆ 0.72435 ┆ 0.7247  │
    │ 2009-12-03 07:00:00 ┆ 0.72465 ┆ 0.72515 ┆ 0.72395 ┆ 0.725   │
    │ 2009-12-03 08:00:00 ┆ 0.72505 ┆ 0.72505 ┆ 0.7217  ┆ 0.7224  │
    │ 2009-12-03 09:00:00 ┆ 0.7225  ┆ 0.7241  ┆ 0.7214  ┆ 0.7225  │
    │ 2009-12-03 10:00:00 ┆ 0.72245 ┆ 0.72285 ┆ 0.7212  ┆ 0.72235 │
    └─────────────────────┴─────────┴─────────┴─────────┴─────────┘
    ```
<br>

3. **add a timeframe**
    ```
    histmanager.add_timeframe('1W', update_data=True)
    ```
    here a new timeframe is appended to existing ones.
    By assigning `update_data=True`, the data manager creates 
    and if not present dumps new timeframe data in its data path.
    Otherwise, the new timeframe is just appended to the instance internal list.
<br>

4. **plot data**
    ```
     histmanager.plot( timeframe   = '1D',
                      start_date  = '2013-02-02 18:00:00',
                      end_date    = '2013-06-23 23:00:00'
    )
    ```
    It performs a get_data function explained in point (2) and generates a classic candles chart

<br>

![output chart](doc/imgs/histdata_test_nzdusd.png)

<br>

#### Real-Time data

Let's walk through the [example for real-time data source](test/test_realtime_data_manager.py), outputs are with `polars` as DATA_ENGINE:

1. **data manager instance**
    ```
    realtimedata_manager = realtime_manager(
                            ticker = 'NZDUSD',
                            config_file = APPCONFIG_FILE_YAML
    )
    ```
2. **get last daily close**
    ```
    # input test request definition
    test_day_start   = '2024-03-10'
    test_day_end     = '2024-03-26'
    test_n_days      = 10
    dayclose_quote = realtimedata_manager.get_daily_close(last_close=True)

    logger.trace(f'Real time daily close quote {dayclose_quote}')
    ```
    
    Output:
    ```
    Real time daily close quote shape: (1, 5)
    ┌─────────────────────┬─────────┬─────────┬─────────┬────────┐
    │ timestamp           ┆ open    ┆ high    ┆ low     ┆ close  │
    │ ---                 ┆ ---     ┆ ---     ┆ ---     ┆ ---    │
    │ datetime[ms]        ┆ f32     ┆ f32     ┆ f32     ┆ f32    │
    ╞═════════════════════╪═════════╪═════════╪═════════╪════════╡
    │ 2024-04-19 00:00:00 ┆ 0.59022 ┆ 0.59062 ┆ 0.58516 ┆ 0.5886 │
    └─────────────────────┴─────────┴─────────┴─────────┴────────┘
    ```
3. **get daily close price with date interval argument by querying for the last N days**
    ```
    window_daily_ohlc = realtimedata_manager.get_daily_close(recent_days_window=test_n_days)

    logger.trace(f'Last {test_n_days} window data: {window_daily_ohlc}')
    ```

    Output 
    ```
    Last 10 window data: shape: (10, 5)
    ┌─────────────────────┬────────┬────────┬────────┬────────┐
    │ timestamp           ┆ open   ┆ high   ┆ low    ┆ close  │
    │ ---                 ┆ ---    ┆ ---    ┆ ---    ┆ ---    │
    │ datetime[ms]        ┆ f32    ┆ f32    ┆ f32    ┆ f32    │
    ╞═════════════════════╪════════╪════════╪════════╪════════╡
    │ 2024-04-19 00:00:00 ┆ 1.6935 ┆ 1.7078 ┆ 1.6932 ┆ 1.6971 │
    │ 2024-04-18 00:00:00 ┆ 1.6894 ┆ 1.6946 ┆ 1.6853 ┆ 1.6934 │
    │ 2024-04-17 00:00:00 ┆ 1.6999 ┆ 1.7012 ┆ 1.6873 ┆ 1.6897 │
    │ 2024-04-16 00:00:00 ┆ 1.6928 ┆ 1.7033 ┆ 1.6926 ┆ 1.7    │
    │ 2024-04-15 00:00:00 ┆ 1.6831 ┆ 1.6947 ┆ 1.6792 ┆ 1.6933 │
    │ 2024-04-12 00:00:00 ┆ 1.6668 ┆ 1.6846 ┆ 1.6635 ┆ 1.684  │
    │ 2024-04-11 00:00:00 ┆ 1.6731 ┆ 1.6746 ┆ 1.6624 ┆ 1.6667 │
    │ 2024-04-10 00:00:00 ┆ 1.6498 ┆ 1.6754 ┆ 1.6437 ┆ 1.6729 │
    │ 2024-04-09 00:00:00 ┆ 1.657  ┆ 1.6573 ┆ 1.6455 ┆ 1.65   │
    │ 2024-04-08 00:00:00 ┆ 1.6639 ┆ 1.6658 ┆ 1.6555 ┆ 1.6576 │
    └─────────────────────┴────────┴────────┴────────┴────────┘
    ```
4. **get daily close price with date interval argument by querying start and end date**
    ```
     window_limits_daily_ohlc = realtimedata_manager.get_daily_close(day_start=test_day_start,
      day_end=test_day_end)

    logger.trace(f'From {test_day_start} to {test_day_end} ' 
                 f'window data: {window_limits_daily_ohlc}')
    ```

    Output:
    ```
    ┌─────────────────────┬────────┬────────┬────────┬────────┐
    │ timestamp           ┆ open   ┆ high   ┆ low    ┆ close  │
    │ ---                 ┆ ---    ┆ ---    ┆ ---    ┆ ---    │
    │ datetime[ms]        ┆ f32    ┆ f32    ┆ f32    ┆ f32    │
    ╞═════════════════════╪════════╪════════╪════════╪════════╡
    │ 2024-03-26 00:00:00 ┆ 1.6651 ┆ 1.6672 ┆ 1.6578 ┆ 1.6647 │
    │ 2024-03-25 00:00:00 ┆ 1.6679 ┆ 1.6698 ┆ 1.6627 ┆ 1.665  │
    │ 2024-03-22 00:00:00 ┆ 1.6542 ┆ 1.669  ┆ 1.652  ┆ 1.6681 │
    │ 2024-03-21 00:00:00 ┆ 1.6474 ┆ 1.6554 ┆ 1.6372 ┆ 1.6539 │
    │ 2024-03-20 00:00:00 ┆ 1.6519 ┆ 1.6591 ┆ 1.643  ┆ 1.6479 │
    │ …                   ┆ …      ┆ …      ┆ …      ┆ …      │
    │ 2024-03-15 00:00:00 ┆ 1.6306 ┆ 1.6439 ┆ 1.6306 ┆ 1.6437 │
    │ 2024-03-14 00:00:00 ┆ 1.6229 ┆ 1.6329 ┆ 1.619  ┆ 1.6308 │
    │ 2024-03-13 00:00:00 ┆ 1.6252 ┆ 1.6267 ┆ 1.6203 ┆ 1.6236 │
    │ 2024-03-12 00:00:00 ┆ 1.6203 ┆ 1.6291 ┆ 1.617  ┆ 1.6256 │
    │ 2024-03-11 00:00:00 ┆ 1.6183 ┆ 1.6224 ┆ 1.617  ┆ 1.6205 │
    └─────────────────────┴────────┴────────┴────────┴────────┘
    ```
5. **get OHLC data with timeframe specific by querying start and end date interval**
    ```
    # input test request definition
    test_day_start   = '2024-04-10'
    test_day_end     = '2024-04-15'
    test_timeframe   = '1h'
    window_data_ohlc =  realtimedata_manager.get_data(  start     = test_day_start,
                                                        end       = test_day_end,
                                                        timeframe = test_timeframe)

    logger.trace(f'Real time {test_timeframe} window data: {window_data_ohlc}')
    ```

    Output:
    ```
    Real time 1h window data: shape: (72, 5)
    ┌─────────────────────┬─────────┬─────────┬─────────┬─────────┐
    │ timestamp           ┆ open    ┆ high    ┆ low     ┆ close   │
    │ ---                 ┆ ---     ┆ ---     ┆ ---     ┆ ---     │
    │ datetime[ms]        ┆ f32     ┆ f32     ┆ f32     ┆ f32     │
    ╞═════════════════════╪═════════╪═════════╪═════════╪═════════╡
    │ 2024-04-10 00:00:00 ┆ 0.60648 ┆ 0.6068  ┆ 0.6057  ┆ 0.606   │
    │ 2024-04-10 01:00:00 ┆ 0.60615 ┆ 0.60636 ┆ 0.6053  ┆ 0.60555 │
    │ 2024-04-10 02:00:00 ┆ 0.60554 ┆ 0.60772 ┆ 0.60344 ┆ 0.60707 │
    │ 2024-04-10 03:00:00 ┆ 0.60705 ┆ 0.60757 ┆ 0.6067  ┆ 0.60736 │
    │ 2024-04-10 04:00:00 ┆ 0.60737 ┆ 0.60762 ┆ 0.607   ┆ 0.60733 │
    │ …                   ┆ …       ┆ …       ┆ …       ┆ …       │
    │ 2024-04-12 20:00:00 ┆ 0.59352 ┆ 0.59374 ┆ 0.58252 ┆ 0.58252 │
    │ 2024-04-12 21:00:00 ┆ 0.54007 ┆ 0.5947  ┆ 0.54007 ┆ 0.594   │
    │ 2024-04-14 22:00:00 ┆ 0.59376 ┆ 0.59466 ┆ 0.59357 ┆ 0.59434 │
    │ 2024-04-14 23:00:00 ┆ 0.59439 ┆ 0.59493 ┆ 0.59378 ┆ 0.5941  │
    │ 2024-04-15 00:00:00 ┆ 0.59438 ┆ 0.59457 ┆ 0.5941  ┆ 0.59445 │
    └─────────────────────┴─────────┴─────────┴─────────┴─────────┘
    ```

## PYTEST and pipeline implementation



## Performance considerations

Pandas is way too slow compared to PyArrow and Polars, by far.
Especially working with csv files and performing in-dataframe operations like the groupby on along timestamp column.

*Soon time based benchmarks will be provided to explictly show how much polars and pyarrow overcome pandas*

The battle between polars and pyarrow is still on.
Any suggestion on why one should choose pyarrow over polars or viceversa is welcome.

Or, a further data engine suggested could be taken into consideration to be integrated in the package.


## Future developments

Here is a list of elements that could power or make the package more solid:

* scout for open source databases to have a persistent storage instead of simple local files in a folder
    1. by performance evaluations prefer components that fits with polars and pyarrow, and so components or databases tightened to [Apache Arrow](https://arrow.apache.org/docs/index.html) as much as possible
    2. use connectorx or ADBC for database driver
    3. for databases, 
        * here is a nice repo example by voltrondata [Arrow Flight SQL server - DuckDB / SQLite](https://github.com/voltrondata/flight-sql-server-example)
        * PostgreSQL, quite mentioned both in polars and pyarrow API, [Polars read database](https://docs.pola.rs/py-polars/html/reference/api/polars.read_database.html) and [Pyarrow PostgreSQL Recipes](https://arrow.apache.org/adbc/0.5.1/python/recipe/postgresql.html) 

* enhance charting part of the package

* enhance real time data manager to have a layer uniforming calls to different remote sources: 
    1. if a remote source API call is denied, iteratively search for another remote source type having similar API call and deliver the same results. So the final achievment is a cumulative sum of free API calls.
