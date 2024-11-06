from __future__ import annotations

import os
import json
import logging
import pandas as pd
from typing import Tuple, Union
from pathlib import Path
from datetime import datetime
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



class KafkaConsumeProduceOperator(BaseOperator):
    """ 
    Defines target class SpotifyApiOperator in conjunction 
    with Spotify API, inheriting from class BaseOperator.
    
    :param kafka_nodes 
    :param topic_name
    """
    
    @apply_defaults
    def __init__(self, kafka_nodes: str, topic_name: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.kafka_nodes = kafka_nodes
        self.topic_name = topic_name
        
        
    def send_data_to_topic(self, record_data: dict[str, Union[str, float]]) -> None:
        """
        Create the producer object and send data to the topic.
        """
        
        try:
            producer = KafkaProducer(bootstrap_servers=[self.kafka_nodes],
                value_serializer=lambda m: json.dumps(m).encode("utf-8")
            )
            logger.info("Create producer object success.")
            
            producer.send(topic=self.topic_name, value=record_data)
            producer.flush()
        
        except Exception as e:
            logger.info(f"{e}")
            raise e
        
        
    def read_data_from_topic(self) -> None:
        """
        Create the producer object and send data to the topic.
        """
        
        try:
            consumer = KafkaConsumer(topic=self.topic_name,
                bootstrap_servers=[self.kafka_nodes],
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            for message in consumer:
                data = message.value
                
        
        except Exception as e:
            logger.info(f"{e}")
            raise e
        
    
    def send_data_to_spark_streaming(self) -> None:
        pass