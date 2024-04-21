# <span style="font-size:1.5em;">FOREX DATA</span>

The forex_data package offers ways to aggregate data from the Forex market into a dataframe having the the essential OHLC information, so the ouput will always have the columns:

* timestamp
* open
* high
* low
* close

The first purpose is to aggregate data in OHLC format and allow to have data in any timeframe specified in the most simple and efficient way.
The second purpose is to manage one or multiple sources, an interface layer will have primary functions with predefined name, inputs and ouput results.

At the moment, sources are divided in **historical sources** and **real-time sources**.

## SOURCES

### HISTORICAL SOURCE

A historical source is a source of data which makes data available but does not have a defined update policy for design reasons, but does have stored a ton of history data, tipically from the first years of 2000s and the free tier is fine for the purposes of the package.

Above I actually described what I found at [histdata.com](http://www.histdata.com/), which work is really genuine and a lot appreciated.

We could see them as a model, maybe in the future a similar source can be found: that is why for generalization purpose this type of source is called *historical source*.

Summarizing, a historical source can provide tons of data even from many years ago and with no limits at the downside of a slow update rate. For example, *histdata* updates data on a montly basis.

### REAL-TIME SOURCE

A real-time source is what is more tipically known as a source for forex market or stock market data. It offers APIs in determined clients or even just a minimal documentation to establish the API call in HTTP request format.
A minimal free or trial offering is proposed, but they rely on premium subscriptions offers based on:

* real time performance 
* size of tickers list available
* how much history of a ticker 
* and many other parameters ...

I liked [alpha-vantage](https://www.alphavantage.co/documentation/) and [polygon-io](https://polygon.io/docs/forex/getting-started).
As of now, I am managing just these two and trying to make the most out of them and their free tier access to data.

Even if free subscription is limitated for these providers, the reasons to include them in the package are to have closer real-time update than any historical source and also the module is designed to ease the work of studying a new provider API calls: a real time data manager uses at the same time all the remote sources available and provides access to their API through easier interface.

## INSTALLATION

The package is managed via Poetry, which is required to install the package.
Follow here for installing Poetry: [Poetry documentation](https://python-poetry.org/docs/).

With poetry installed, these steps should lead you to run the examples or any other package usage:

1. Open a shell (on Windows use [powershell7](https://learn.microsoft.com/it-it/powershell/scripting/install/installing-powershell-on-windows?view=powershell-7.4#msi)), clone the repository in a folder called `forex-data` (for example):
```
git clone -b master forex-data
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
In repository folder clone, look for `\appconfig` folder to see the [example template file](appconfig\appconfig_template.yaml).

At any run, the package looks for a file called `appconfig.yaml` and associates it to a variable called `APPCONFIG_YAML` so that it is simpler to use a default config file in package modules calls.

In data managers instantiation, you can pass the configuration file but any parameter value can be overridden by explicit assignment in object instantion.
The feature will be more clear following the [examples section](#examples)

#### ENGINE

Available options:

* pandas
* pyarrow
* polars

#### DATA_FILETYPE

Available options:

* csv
* parquet

#### DATA_PATH

Here you can pass a absolute folder path where the package will dump data downloaded with files type as assigned by the `DATA_FILETYPE` parameter.

#### PROVIDERS_KEY

To use real-time sources you need to provide an API key.

Look here to register and create a key from Alpha-Vantage provider
[Alpha-Vantage free API registration](https://www.alphavantage.co/support/#api-key)

Look here to register and create a key from Polygon-IO provider
[Polygon-IO home page](https://polygon.io/)

## EXAMPLES

in `\test` folder you can find working examples showing the various modules or functionalities the package offers.

#### Historical data 

Let's walk through the [example for historical data source](test\test_hist_data_manager.py):

1. data manager instance 
    ```                            
    histmanager = historical_manager(
                    ticker='NZDUSD',
                    config_file=APPCONFIG_YAML
    )
    ```
    as mentioned in section [configuration](#configuration-file), you can see the feature of overriding the parameter `ticker` by direct assignment in object instantiation, meanwhile the remaining parameters value are assigned by the configuration file.
<br>
2. get data
    ```
    yeardata = histmanager.get_data(timeframe = '1h',
                                    start     = ex_start_date,
                                    end       = ex_end_date
    )
    ```
    the call returns a dataframe with data having timeframe, start and end specified by inputs assignment.
    The output data type is of type related to the engine selected.
<br>
3. add a timeframe
    ```
    histmanager.add_timeframe('1W', update_data=True)
    ```
    here a new timeframe is appended to existing ones.
    By assigning `update_data=True`, the data manager creates 
    and if not present dumps new timeframe data in its data path.
    Otherwise, the new timeframe is just appended to the instance internal list.
<br>
4. plot data
    ```
     histmanager.plot( timeframe   = '1D',
                      start_date  = '2013-02-02 18:00:00',
                      end_date    = '2013-06-23 23:00:00'
    )
    ```
    It performs a get_data function explained in point (2) and generates a calssic candles chart.



#### Real-Time data

Let's walk through the [example for real-time data source](test\test_realtime_data_manager.py):

1. data manager instance call
    ```
    realtimedata_manager = realtime_manager(
                            ticker = 'NZDUSD',
                            config_file = APPCONFIG_YAML
    )
    ```
2. get last daily close
    ```
    dayclose_quote = realtimedata_manager.get_daily_close(last_close=True)
    ```
    
3. get daily close price with date interval argument by querying for the last N days
    ```
    window_daily_ohlc = realtimedata_manager.get_daily_close(recent_days_window=test_n_days)
    ```

4. get daily close price with date interval argument by querying start and end date
    ```
     window_limits_daily_ohlc = realtimedata_manager.get_daily_close(day_start=test_day_start,
                                                                     day_end=test_day_end)
    ```

5. get OHLC data with timeframe specific by querying start and end date interval
    ```
    window_data_ohlc =  realtimedata_manager.get_data(  start     = test_day_start,
                                                        end       = test_day_end,
                                                        timeframe = test_timeframe)
    ```

## Performance considerations

Pandas is way too slow compared to PyArrow and Polars, by far.
Especially working with csv files and performing in-dataframe operations like the groupby on along timestamp column.


## Future developments

Here is a list of elements that could power or make the package more solid:

* scout for open source databases to have a persistent storage instead of simple local files in a folder
    1. use components or databases tightened to [Apache Arrow](https://arrow.apache.org/docs/index.html) as much as possible
    2. use connectorx or ADBC for database driver

* enhance charting part of the package

* enhance real time data manager to have a layer uniforming calls to different remote sources: 
    1. if a remote source API call is denied, iteratively search for another remote source type having similar API call and deliver the same results. So the final achievment is a cumulative sum of free API calls.
