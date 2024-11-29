from __future__ import annotations

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
            # SPARK_HOME = '/opt/bitnami/spark'
            # spark_submit_path = os.path.join(SPARK_HOME, 'bin', 'spark-submit')
            
            # Kiểm tra nếu spark-submit tồn tại
            # if not os.path.isfile(spark_submit_path):
            #     raise FileNotFoundError(f"Spark submit script not found at {spark_submit_path}")

            # print(f"Using spark-submit from: {spark_submit_path}")
            
            if self._config is not None:
                s_conn: SparkSession = SparkSession.builder \
                        .appName("{}-{}".format(self.appName, datetime.today())) \
                        .master("spark://spark-master:7077") \
                        .config("spark.sql.streaming.checkpointLocation.criticalErrorPolicy", "FAIL") \
                        .config("spark.sql.streaming.checkpointLocation.enable", True) \
                        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
                                                "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
                        .config('spark.cassandra.connection.host', 'cassandra') \
                        .config("spark.cassandra.connection.port", "9042") \
                        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
                        .config("spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog") \
                        .config("spark.cassandra.auth.username", "cassandra") \
                        .config("spark.cassandra.auth.password", "cassandra") \
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
                        .option("startingOffsets", "earliest") \
                        .load()                             
                                    
                    logger.info("kafka dataframe created successfully")
                    return streaming_df
        
        except Exception as e:
            logger.warning(f"kafka dataframe could not be created because: {e}")