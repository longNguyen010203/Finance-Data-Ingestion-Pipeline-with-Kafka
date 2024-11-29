import os
import json
import time
import logging
import finnhub
import websocket
import pandas as pd
from kafka.producer import KafkaProducer
from ..constant import stock_code_constant
from typing import Union, Any
from dotenv import load_dotenv
load_dotenv()



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



KAFKA_NODES = os.getenv("KAFKA_NODES")
producer = KafkaProducer(bootstrap_servers=[KAFKA_NODES],
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)


class StockFinnhubMetrics:
    """ 
    :param api_key:
    :param topic_name:
    """
    
    topic_name: str = ""
    seen_records = set()
    
    def __init__(self, api_key: str, topic_name: str) -> None:
        self.api_key = api_key
        StockFinnhubMetrics.topic_name = topic_name
        
        
    def finnhub_websocket_collect(self) -> None:
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        
        try:
            if self.api_key is not None:
                websocket.enableTrace(True)
                ws = websocket.WebSocketApp(
                    f"wss://ws.finnhub.io?token={self.api_key}",
                    on_message=StockFinnhubMetrics.on_message,
                    on_error=StockFinnhubMetrics.on_error,
                    on_close=StockFinnhubMetrics.on_close,
                    on_ping=StockFinnhubMetrics.on_ping,
                    on_pong=StockFinnhubMetrics.on_pong
                )
                ws.on_open = StockFinnhubMetrics.on_open
                ws.run_forever(ping_interval=30)
                
        except (
                ConnectionRefusedError,
                KeyboardInterrupt,
                SystemExit,
                Exception,
            ) as e:
            logger.exception(f"Connect failed: {str(e)}")   
            
            
    @staticmethod
    def on_message(ws, message) -> None:
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        
        try:
            data = json.loads(message)
            
            if data is not None or "data" in data:
                for record_data in data["data"]:
                    record_key = (
                        str(record_data["c"]), 
                        record_data["p"], 
                        record_data["s"],
                        record_data["t"],
                        record_data["v"],
                    )
                    if record_key not in StockFinnhubMetrics.seen_records:
                        StockFinnhubMetrics.seen_records.add(record_key)
                    
                        producer.send(
                            topic=StockFinnhubMetrics.topic_name,
                            value=record_data
                        )
                        # producer.flush()
                        logger.info(f"Collect success {record_data}")
                        
                    else: logger.warning(f"Duplicate record skipped: {record_data}")
                    
        except Exception as e:
            logger.error(f"Error in on_message: {str(e)}")
        
        
    @staticmethod
    def on_error(ws, error) -> None: 
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        logger.error(f"WebSocket Error: {error}")
        
        
    @staticmethod
    def on_close(ws) -> None: 
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        logger.warning("WebSocket closed.")
    
    
    @staticmethod
    def on_open(ws) -> None:
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        
        for stock_code in stock_code_constant.STOCKCODE:
            message = f'{{"type":"subscribe","symbol":"{stock_code}"}}'
            ws.send(message)
            logger.info(f"Subscribed to {stock_code}.")
            
            
    @staticmethod
    def on_ping(ws, message):
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        logger.info("Ping sent")
        
        
    @staticmethod
    def on_pong(ws, message):
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        logger.info("Pong received")

                
                
                
if __name__ == "__main__":
    finnhubApi = StockFinnhubMetrics(
        api_key=os.getenv("API_KEY"),
        topic_name="finnhub_stock"
    )
    finnhubApi.finnhub_websocket_collect()