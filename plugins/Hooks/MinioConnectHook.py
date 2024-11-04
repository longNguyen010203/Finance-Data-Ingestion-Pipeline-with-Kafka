from __future__ import annotations

import os
import logging
import warnings
from minio import Minio
from typing import Any

from airflow.hooks.base import BaseHook
from dotenv import load_dotenv
load_dotenv()



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MinioConnectHook(BaseHook):
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
    
    def __init__(
        self, 
        endpoint: str, 
        access_key: str, 
        secret_key: str, 
        secure: bool, 
        logger_name: str | None = None
    ) -> None:
        
        super().__init__(logger_name)
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.get_connect_minio()
        
        
    def get_connect_minio(self) -> Minio:
        try:
            client = Minio(
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            return client
        except Exception as e:
            logger.exception("Connect to Minio failed.")
            raise e