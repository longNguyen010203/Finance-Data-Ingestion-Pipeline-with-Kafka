from airflow.plugins_manager import AirflowPlugin
from Operators.YahooFinanceOperator import YahooFinanceOperator



class YahooFinanceOperator(AirflowPlugin):
    name = "YahooFinance_API_Plugin"
    operators = [YahooFinanceOperator]