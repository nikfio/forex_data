# implement charts

- delete existing plot() function in HistoricalManagerDB
- I am looking to have a derived class DataCharts that inherits from HistoricalManagerDB and has a realtime_connector as input argument. This connector will be used to plot realtime data.
- new related code will be under a new dedicated folder forex_data/data_management/charts
- I want to have charts to plot prices ohlc or ask/bid with a defined specified as input start and end date.
- Hovering over a OHLC candle should give volume info/values, or other useful info.
- the base plot is OHLC but evaluate other important sets with features available and create a SUPPORTED_CHARTS_TYPE list
- Evaluate Dash plotly is better or others.
- realtime plotting shall be available, it will be specified:
    * start date
    * timeframe (resolution interval of time to update data)
    * the chart will be kept on and updated every timeframe timeput expires
    * plotting will be done in a separate thread to prevent blocking the main thread.
    * shall support the same SUPPORTED_CHARTS_TYPE as in historical charts

    
    