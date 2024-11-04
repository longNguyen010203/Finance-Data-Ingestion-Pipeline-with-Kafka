from __future__ import annotations

import json
import time
import logging
import warnings
import pandas as pd
from typing import Any, Union
import yfinance as YahooFinanceAPI
from constant import stock_code_constant

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



class YahooFinanceOperator(BaseOperator):
    """ 
    Defines target class SpotifyApiOperator in conjunction 
    with Spotify API, inheriting from class BaseOperator.
    
    :param spotify_client_id 
    :param spotify_client_secret
    """
    
    @apply_defaults
    def __init__(self, nameSource: str = "Yahoo Finance API", *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.nameSource: str = nameSource
        
        
    def yahoo_finance_connect(self, stock_code: str) -> YahooFinanceAPI.Ticker:
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        
        if stock_code in stock_code_constant.STOCKCODE:
            logger.info(f"Connect success to {self.nameSource}.")
            return YahooFinanceAPI.Ticker(stock_code)
        else: logger.info(f"Connect failed to {self.nameSource}.")
        
        
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
        
        if stock_code in stock_code_constant.STOCKCODE:
            yahooFinanceConnect = self.yahoo_finance_connect(stock_code)
            logger.info(f"Connect success to {self.nameSource}")
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
        time.sleep(seconds=0.5)
        
        return json.loads(record_data[1:-1])
    
    
    def get_OHLCVs_data_realtime(
        self, period: str = "1d", interval: str = "1m", auto_adjust: bool = False
    ) -> None:
        """ 
        Basic and important indicators when analyzing stock or asset prices, 
        including: Open, High, Low, Close, Close Adjustment, Volume,.... 
        Taken with many stock codes.
        """
        
        while True:
            for stock_code in stock_code_constant.STOCKCODE:
                self.get_OHLCV_data_realtime(
                    stock_code=stock_code,
                    period=period,
                    interval=interval,
                    auto_adjust=auto_adjust
                )
                
            time.sleep(40)
            
            
    def get_finance_metrics_realtime(self) -> Any:
        pass