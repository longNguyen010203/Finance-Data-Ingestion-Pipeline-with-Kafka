import logging
from typing import Union, Any

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, 
    StructField, 
    TimestampType,
    FloatType,
    IntegerType,
    StringType
)



schema = StructType([
    StructField("Datetime", TimestampType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Adj Close", FloatType(), True),
    StructField("Volume", IntegerType(), True),
    StructField("Dividends", FloatType(), True),
    StructField("Stock Splits", FloatType(), True),
    StructField("ticker", StringType(), True)
])


def process_stocktrade_data_realtime(data: DataFrame) -> DataFrame:
    pass