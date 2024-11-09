import os
import json
import time
import logging
import argparse
import pandas as pd
import yfinance as YahooFinanceAPI
from kafka.producer import KafkaProducer
from ..constant import stock_code_constant
from typing import Union, Any
from dotenv import load_dotenv
load_dotenv()



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



class StockFinanceMetrics:
    """
    :param tickerSymbols
    """
    
    def __init__(self, nameSource: str = "Yahoo Finance API") -> None:
        self.nameSource: str = nameSource
        
        
    def yahoo_finance_connect(self, stock_code: str) -> YahooFinanceAPI.Ticker:
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        
        try:
            if stock_code in stock_code_constant.STOCKCODE:
                # logger.info(f"Connect success to {self.nameSource}.")
                return YahooFinanceAPI.Ticker(stock_code)
            else: logger.info(f"Connect failed to {self.nameSource}.")
        except Exception as e: raise e
        
        
    def get_OHLCV_data_realtime(
        self, stock_code: str, period: str = "1d", interval: str = "1m", auto_adjust: bool = False
    ) -> dict[str, Union[str, float]]:
        """ 
        Basic and important indicators when analyzing the price of a stock 
        or asset, including: Open, High, Low, Close, Adj Close, Volume,...
        
        :param stock_code: Stock code for which you want to get data
        :param period: Specify the time range for which you want to retrieve data.
        :param interval: Determines the data frequency, 
        i.e. the time interval between data points.
        :param auto_adjust: Is a boolean parameter (True/False) that allows automatic 
        adjustment of the stock price to account for events such as dividends, 
        stock splits, or other adjustments.
        """
        
        try:
            if stock_code in stock_code_constant.STOCKCODE:
                yahooFinanceConnect = self.yahoo_finance_connect(stock_code)
                # logger.info(f"Connect success to {self.nameSource}")
            else: logger.info(f"{stock_code} is not exists.")
                
            if period == "1d" and interval == "1m":
                getStockInformationDF = yahooFinanceConnect.history(
                    period=period,
                    interval=interval,
                    auto_adjust=auto_adjust
                )
            else:
                logger.warning(f"period:{period} and interval:{interval} is not exists.")
                return
            
            getStockInformationDF = getStockInformationDF.reset_index()
            getStockInformationDF["ticker"] = stock_code
            pd.set_option('display.max_rows', None)
            
            record_data = getStockInformationDF.tail(2).head(1).to_json(
                date_format='iso',
                orient="records"            
            )
            # time.sleep(seconds=0.5)
            return json.loads(str(record_data)[1:-1])
        
        except Exception as e: raise e
    
    
    def get_OHLCVs_data_realtime(
        self, topic: str, period: str = "1d", interval: str = "1m", auto_adjust: bool = False
    ) -> None:
        """ 
        Basic and important indicators when analyzing stock or asset prices, 
        including: Open, High, Low, Close, Close Adjustment, Volume,.... 
        Taken with many stock codes.
        """
        
        try:
            KAFKA_NODES = os.getenv("KAFKA_NODES")
            producer = KafkaProducer(bootstrap_servers=[KAFKA_NODES],
                value_serializer=lambda m: json.dumps(m).encode("utf-8")
            )
            
            while True:
                for stock_code in stock_code_constant.STOCKCODE:
                    stock_json_record = self.get_OHLCV_data_realtime(
                        stock_code=stock_code,
                        period=period,
                        interval=interval,
                        auto_adjust=auto_adjust
                    )
                    print(stock_json_record)
                    producer.send(topic=topic, value=stock_json_record)
                    producer.flush()
                    time.sleep(0.2)
                    
                time.sleep(20)
                
        except Exception as e: raise e
            
            
    def get_stock_trade_realtime(self) -> Any:
        pass
        
        
        
if __name__ == "__main__":
    stock = StockFinanceMetrics()
    # print(stock.get_OHLCV_data_realtime("AAPL"))
    stock.get_OHLCVs_data_realtime()