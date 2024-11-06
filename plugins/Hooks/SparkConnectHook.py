from __future__ import annotations

import os
import logging
import warnings
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from airflow.hooks.base import BaseHook



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



class SparkConnectHook(BaseHook):
    """
    Abstract base class for hooks.

    Hooks are meant as an interface to
    interact with external systems. MySqlHook, HiveHook, PigHook return
    object that can handle the connection and interaction to specific
    instances of these systems, and expose consistent methods to interact
    with them.

    :param logger_name: Name of the logger used by the Hook to emit logs.
    If set to `None` (default), the logger name will fall back to
    `airflow.task.hooks.{class.__module__}.{class.__name__}` (e.g. DbApiHook will have
    *airflow.task.hooks.airflow.providers.common.sql.hooks.sql.DbApiHook* as logger).
    """
    
    def __init__(self, config: dict, logger_name: str | None = None, *args, **kwargs):
        super().__init__(logger_name, *args, **kwargs)
        self._config = config
        
        
    def create_spark_session(self, appName: str = None) -> SparkSession:
        try:
            spark = SparkSession.builder.appName(appName) \
                    .master("local[*]") \
                    .config("spark.hadoop.fs.s3a.endpoint", "http://" + self._config["endpoint_url"]) \
                    .config("spark.hadoop.fs.s3a.access.key", str(self._config["aws_access_key_id"])) \
                    .config("spark.hadoop.fs.s3a.secret.key", str(self._config["aws_secret_access_key"])) \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .getOrCreate()
            logger.info(f"Create {appName} SparkSession success.")
            return spark
            
        except Exception as e:
            logger.exception(f"Create {appName} SparkSession failed.")
            raise e