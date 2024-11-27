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
    StructField("datetime", TimestampType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("adj_close", FloatType(), True),
    StructField("volume", IntegerType(), True),
    StructField("dividends", FloatType(), True),
    StructField("stock_splits", FloatType(), True),
    StructField("ticker", StringType(), True)
])


def process_OHLCV_data_realtime(data: DataFrame) -> DataFrame:
    pass