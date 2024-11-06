from __future__ import annotations

import os
import logging
import pandas as pd
from typing import Tuple
from minio import Minio
from pathlib import Path
from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.Hooks.MinioConnectHook import MinioConnectHook



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



class MinioReaderWriterOperator(BaseOperator):
    """ 
    Defines target class SpotifyApiOperator in conjunction 
    with Spotify API, inheriting from class BaseOperator.
    
    :param access_key 
    :param secret_key
    :param bucket_name
    :param endpoint
    :param secure
    """
    
    @apply_defaults
    def __init__(self, bucket_name: str = "lakehouse",*args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name

        
    def _get_file_path(self, ticker: str) -> Tuple[str, str]:
        """ 
        Get the data file name and temporary address
        """
        
        today = datetime.today()
        year, month, day = today.year, today.month, today.day
        key = "/".join([ticker, year, month, f"day_{day}"])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"), 
            "-".join([ticker, f"day_{day}"])
        )
        
        return f"{key}.parquet", tmp_file_path
        
    
    def writer_to_minio(self, ticker: str) -> None:
        """ 
        Write the collected data file to the bucket created in minio
        """
        
        key_name: str = self._get_file_path(ticker)[0]
        tmp_file_path: str = self._get_file_path(ticker)[1]
        
        minio = MinioConnectHook(
            endpoint=os.getenv("ENDPOINT_URL"),
            access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            secure=False
        )
        
        try:
            bucket_name: str = self.bucket_name
            with minio.get_connect_minio() as client:
                # Make bucket if not exist.
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                    logger.info(f"create {bucket_name} bucket success.")
                else:
                    logger.info(f"Bucket {bucket_name} already exists.")
                
                # Write data file to bucket in minio.
                client.fput_object(bucket_name, key_name, tmp_file_path)
                
                logger.info(
                    f"""
                        path: {key_name}, 
                        data_collection: {datetime.today()}
                        tmp: {tmp_file_path},
                    """
                )
                
                # clean up tmp file
                os.remove(tmp_file_path)
                
        except Exception as e:
            raise e
        
    
    def reader_from_minio(self) -> None:
        pass