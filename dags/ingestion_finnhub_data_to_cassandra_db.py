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
import finnhub_processing


from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from Operators.SparkProcessOperator import SparkProcessOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



CASSANDRA_KEYSPACE="finance"
CASSANDRA_TABLE_NAME="stock_trade"
KAFKA_TOPIC_NAME="finnhub_stock"

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
        
        if data.get("v", 0) > 0 and data.get("s"):
            logger.info(f"Received valid message for ticker {data['s']} with volume {data['v']}")
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
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{
            'class': 'SimpleStrategy', 
            'replication_factor': 1
        }}; """.format(CASSANDRA_KEYSPACE))
    logger.info(f"Keyspace {CASSANDRA_KEYSPACE} created.")
    
    
def create_table(**kwargs):
    session = connect_to_cassandra()
    session.set_keyspace(f"{CASSANDRA_KEYSPACE}")
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE_NAME} (
            id UUID, 
            trade_conditions list<text>,
            last_price float,
            symbol text,
            datetime timestamp,
            volume int,
            PRIMARY KEY (id)
        );
    """)
    logger.info(f"Table {CASSANDRA_TABLE_NAME} created in keyspace {CASSANDRA_KEYSPACE}.")
    
    

@dag(
    dag_id="FinnhubApi_to_Cassandra_v104",
    default_args=default_args,
    start_date=datetime(2024, 11, 15),
    schedule_interval="0 23 * * Mon,Wed,Fri",
    tags=["FinnhubApi", "ETL", "Data Engineer", "Cassandra"],
    catchup=False
)
def etl_pipeline_FinnhubApi_to_Cassandra() -> None:
    
    #-------------------------------------------------------#
    # Data ingestion pipeline from Finnhub API to Cassandra #
    #-------------------------------------------------------#
    
    #------------------#
    # Connection Kafka #
    #------------------#
    connect_kafka = PythonOperator(task_id="connect_kafka", python_callable=load_connections)
    
    #--------------#
    # Create Topic #
    #--------------#
    @task(multiple_outputs=True)
    def create_topic_finnhub_financial_kafka(**context) -> dict[str, Any]:
        """ 
        Create topics for data, financial indices, stocks such as trade_conditions, Volume, 
        last_price, symbol, datetime,... and a few other indicators from Finnhub API. 
        """
        
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=["kafka:9092"])
            topic_name = KAFKA_TOPIC_NAME
            
            # Check if topic already exists
            if admin_client is not None:
                existing_topics = admin_client.list_topics()
                if topic_name not in existing_topics:
                    topic = NewTopic(name=KAFKA_TOPIC_NAME, num_partitions=1, replication_factor=1)
                    admin_client.create_topics(new_topics=[topic], validate_only=False)
                    logger.info(f"Create success {topic} topic.")
                else: 
                    logger.info(f"Topic {KAFKA_TOPIC_NAME} already exists.")
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
        apply_function="ingestion_finnhub_data_to_cassandra_db.await_financial",
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
            schema=finnhub_processing.schema,
            topic_name=context["templates_dict"]["received"]
        )
        logger.info("Create Spark Session and connect Kafka Success.")
        
        select_df = spark_streaming_df.selectExpr("CAST(value AS STRING)") \
            .select(F.from_json(F.col('value'), finnhub_processing.schema) \
            .alias('data')).select("data.*") \
            .withColumn("timestamp", F.from_unixtime(F.col("datetime") / 1000)) \
            .select(
                F.col('c').alias('trade_conditions'),
                F.col('p').alias('last_price'),
                F.col('s').alias('symbol'),
                F.col('t').alias('datetime'),
                F.col('v').alias('volume')
            ) \
            .withColumn("id", F.expr("uuid()")) 
            
        logging.info("Streaming is being started...")            
        streaming_query = select_df.writeStream \
            .foreachBatch(lambda batch_df, _: batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=f"{CASSANDRA_TABLE_NAME}", keyspace=f"{CASSANDRA_KEYSPACE}") \
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
    connect_kafka >> create_topic_finnhub_financial_kafka() >> wait_financial_message_sensor
    wait_financial_message_sensor >> processing_with_spark_task

etl_pipeline_FinnhubApi_to_Cassandra()