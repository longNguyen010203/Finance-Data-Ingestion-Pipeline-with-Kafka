# from __future__ import annotations

import os
import logging
from datetime import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from airflow.models.baseoperator import BaseOperator



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


kafka_input_config = {
    "kafka.bootstrap.servers": "kafka:9092",
    "subscribe": "input",
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}

kafka_output_config = {
    "kafka.bootstrap.servers": "kafka:9092",
    "topic": "finnhub_stocktrade",
    "checkpointLocation": "./check.txt"
}


class SparkProcessOperator(BaseOperator):
    """ 
    Defines target class SpotifyApiOperator in conjunction 
    with Spotify API, inheriting from class BaseOperator.
    
    :param access_key 
    :param secret_key
    :param bucket_name
    :param endpoint
    :param secure
    """
    
    def __init__(self, config: dict, appName: str, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.appName = appName
        self._config = config
        
        
    def create_spark_session(self) -> SparkSession:
        s_conn = None
        
        try:
            if self._config is not None:
                s_conn: SparkSession = SparkSession.builder \
                        .appName("{}-{}".format(self.appName, datetime.today())) \
                        .config("spark.hadoop.fs.s3a.endpoint", "http://" + str(self._config["endpoint_url"])) \
                        .config("spark.hadoop.fs.s3a.access.key", str(self._config["aws_access_key_id"])) \
                        .config("spark.hadoop.fs.s3a.secret.key", str(self._config["aws_secret_access_key"])) \
                        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                                                       "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0") \
                        .config("spark.driver.extraJavaOptions", "-Djava.library.path=/opt/bitnami/spark/lib") \
                        .config("spark.executor.extraJavaOptions", "-Djava.library.path=/opt/bitnami/spark/lib") \
                        .getOrCreate()
                        
                s_conn.sparkContext.setLogLevel("ERROR")
                logger.info(f"Create {self.appName} SparkSession success.")
                return s_conn
            
        except Exception as e:
            logger.exception(f"Create {self.appName} SparkSession failed.")
            raise e
        
        
    def connect_to_kafka(self, schema: T.StructType, topic_name: str) -> DataFrame:
        streaming_df = None
        
        try:
            spark: SparkSession = self.create_spark_session()
            if spark is not None:
                if schema is not None:
                    streaming_df = spark.readStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", "kafka:9092") \
                        .option("subscribe", topic_name) \
                        .option("startingOffsets", "earliest").load() \
                        .select(F.from_json(F.col("value").cast("string"), schema).alias("json_data")) \
                        .select("json_data.*")                                
                                    
                    logger.info("kafka dataframe created successfully")
                    return streaming_df
        
        except Exception as e:
            logger.warning(f"kafka dataframe could not be created because: {e}")
            
            
    def streaming_OHLCV_data_process(self) -> DataFrame:
        pass