from __future__ import annotations

import functools
import os
import sys
import time
import json
import kafka
import logging
import kafka.errors
import pandas as pd
from typing import Any
from cassandra.cluster import Cluster
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from datetime import datetime, timedelta
from kafka.admin import KafkaAdminClient, NewTopic
from cassandra.auth import PlainTextAuthProvider


dags_dir = os.path.dirname(os.path.abspath(__file__))
streaming_procesing_path = os.path.abspath(os.path.join(
    dags_dir, "../scripts/spark_streaming_processing"))
sys.path.append(streaming_procesing_path)
import yfinance_processing


from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from Operators.SparkProcessOperator import SparkProcessOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)




# Define the DAG
default_args = {
    'owner': 'longdata',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['longdata.010203@gmail.com']
}


def load_connections() -> None:
    # Connections needed for this dag to finish
    from airflow.models import Connection
    from airflow.utils import db
    
    db.merge_conn(
        Connection(
            conn_id="t1-3",
            conn_type="kafka",
            extra=json.dumps({"socket.timeout.ms": 10, "bootstrap.servers": "kafka:9092"}),
        )
    )
    db.merge_conn(
        Connection(
            conn_id="t5",
            conn_type="kafka",
            extra=json.dumps(
                {
                    "bootstrap.servers": "kafka:9092",
                    "group.id": "t5",
                    "enable.auto.commit": False,
                    "auto.offset.reset": "earliest",
                    "security.protocol": "PLAINTEXT"
                }
            ),
        )
    )
    
    
def await_financial(message) -> bool:
    import json
    try:
        # Parse the message
        data = json.loads(message.value())
        
        if data.get("Volume", 0) > 0 and data.get("ticker"):
            logger.info(f"Received valid message for ticker {data['ticker']} with volume {data['Volume']}")
            return True
        else:
            logger.warning(f"Invalid message received: {data}")
            return False
    except json.JSONDecodeError:
        logger.error(f"Error decoding message: {message.value()}")
        return False
    
    
def connect_to_cassandra():
    cluster = Cluster(["cassandra"], port=9042)
    session = cluster.connect()
    return session


def create_keyspace(**kwargs) -> None:
    session = connect_to_cassandra()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS finance 
        WITH replication = {
            'class': 'SimpleStrategy', 
            'replication_factor': 1
        }; """)
    logger.info("Keyspace 'finance' created.")
    
    
def create_table(**kwargs):
    session = connect_to_cassandra()
    session.set_keyspace('finance')
    session.execute("""
        CREATE TABLE IF NOT EXISTS stock_data (
            datetime timestamp,
            open float,
            high float,
            low float,
            close float,
            adj_close float,
            volume int,
            dividends float,
            stock_splits float,
            ticker text,
            PRIMARY KEY (datetime)
        );
    """)
    logger.info("Table 'stock_data' created in keyspace 'finance'.")

    

@dag(
    dag_id="YahooFinanceApi_to_Minio_v2019",
    default_args=default_args,
    start_date=datetime(2024, 11, 15),
    schedule_interval="0 23 * * Mon,Wed,Fri",
    tags=["YahooFinanceApi", "ETL", "Data Engineer", "Minio"],
    catchup=False
)
def etl_pipeline_YahooFinanceApi_to_Minio() -> None:
    
    #---------------------------------------------------------#
    # Data ingestion pipeline from Yahoo Finance API to Minio #
    #---------------------------------------------------------#
    
    #------------------#
    # Connection Kafka #
    #------------------#
    connect_kafka = PythonOperator(task_id="connect_kafka", python_callable=load_connections)
    
    #--------------#
    # Create Topic #
    #--------------#
    @task(multiple_outputs=True)
    def create_topic_yahoo_financial_kafka(**context) -> dict[str, Any]:
        """ 
        Create topics for data, financial indices, stocks such as Open, Volume, Close, 
        High, Low,... and a few other indicators from Yahoo Finance API. 
        """
        
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=["kafka:9092"])
            topic_name = "yfinance_stock"
            
            # Check if topic already exists
            if admin_client is not None:
                existing_topics = admin_client.list_topics()
                if topic_name not in existing_topics:
                    topic = NewTopic(name="yfinance_stock", num_partitions=1, replication_factor=1)
                    admin_client.create_topics(new_topics=[topic], validate_only=False)
                    logger.info(f"Create success {topic} topic.")
                else: 
                    logger.info(f"Topic {topic_name} already exists.")
            else: 
                logger.warning(f"admin client error")
            
            admin_client.close()
            
            # Push topic name into XCom
            context["ti"].xcom_push(key="topic_name", value=topic_name)
            logger.info(f"Push {topic_name} topic into Xcom success.")
            
            return {
                "bootstrap_servers": "kafka:9092",
                "topic_name": topic_name,
                "num_partitions": 1,
                "replication_factor": 1
            } 
        
        except kafka.errors.TopicAlreadyExistsError as e:
            logger.info(f"topic {topic_name} already exists.")
        except Exception as e:
            logger.warning(f"Error create topic: {e}", exc_info=True)
            raise
        
        
    #-----------------#
    # Wait for signal #
    #-----------------#
    wait_financial_message_sensor = AwaitMessageSensor(
        kafka_config_id="t5",
        task_id="wait_financial_message_sensor",
        topics=["{{ ti.xcom_pull(key='topic_name') }}"],
        apply_function="ingestion_yfinance_data_to_minio.await_financial",
        poll_timeout=360,
        poll_interval=5,
        xcom_push_key="retrieved_message",
    )
    
    
    #----------------------------------------#
    # Streaming Processing with Apache Spark #
    #----------------------------------------#
    @task(
        templates_dict={
            "received": "{{ ti.xcom_pull(key='topic_name') }}"
        },
    )
    def processing_with_spark_streaming(**context) -> None:
        """ 
        Create and connect to the Spark Session object, then connect 
        to kafka to read data from the topic and perform processing. 
        """
        
        # Provide configurations
        spark = SparkProcessOperator(
            task_id="processing_with_spark_streaming",
            appName="spark-streaming-{}".format(datetime.today()),
            config={
                "endpoint_url": os.getenv("MINIO_ENDPOINT"),
                "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
                "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            }
        )
        
        # Perform Spark Session object creation
        # sparkSession: SparkSession = spark.create_spark_session()
        spark_streaming_df: DataFrame = spark.connect_to_kafka(
            schema=yfinance_processing.schema,
            topic_name=context["templates_dict"]["received"]
        )
        logger.info("Create Spark Session and connect Kafka Success.")
        
        select_df = spark_streaming_df.selectExpr("CAST(value AS STRING)") \
            .select(F.from_json(F.col('value'), yfinance_processing.schema).alias('data')).select("data.*")
            
        
        logging.info("Streaming is being started...")            
        streaming_query = select_df.writeStream \
            .foreachBatch(lambda batch_df, _: batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="stock_data", keyspace="finance") \
                .mode("append") \
                .save()) \
            .outputMode("update") \
            .start()
            
        select_df.explain(True)
        streaming_query.awaitTermination()
        
        
    #----------------------------------#
    # Create Keyspace for Cassandra DB #
    #----------------------------------#
    create_keyspace_cassandra = PythonOperator(
        task_id="create_keyspace",
        python_callable=create_keyspace
    )
    
    
    #-------------------------------#
    # Create Table for Cassandra DB #
    #-------------------------------#
    create_table_cassandra = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )
    

    processing_with_spark_task = processing_with_spark_streaming()
    create_keyspace_cassandra >> create_table_cassandra
    create_table_cassandra >> processing_with_spark_task
    connect_kafka >> create_topic_yahoo_financial_kafka() >> wait_financial_message_sensor
    wait_financial_message_sensor >> processing_with_spark_task

etl_pipeline_YahooFinanceApi_to_Minio()