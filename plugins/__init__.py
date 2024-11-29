from airflow.plugins_manager import AirflowPlugin
# from Operators.YahooFinanceOperator import YahooFinanceOperator
# from Operators.MinioReaderWriterOperator import MinioReaderWriterOperator
# from Operators.KafkaConsumeOperator import KafkaConsumeOperator
from Operators.SparkProcessOperator import SparkProcessOperator





class CustomOperatorsPlugin(AirflowPlugin):
    name = "custom_operators_plugin"
    operators = [
        # YahooFinanceOperator,
        # MinioReaderWriterOperator,
        # KafkaConsumeOperator,
        SparkProcessOperator
    ]