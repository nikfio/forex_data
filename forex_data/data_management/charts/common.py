from enum import Enum


class SupportedChartType(str, Enum):
    """
    Enum defining the types of charts supported by the DataCharts module.
    """
    OHLC = "ohlc"
    LINE = "line"  # Simple line chart for close prices or single values
    SCATTER = "scatter"  # Scatter plot for tick data or specific points


SUPPORTED_CHARTS_TYPE = [e.value for e in SupportedChartType]
