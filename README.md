# 🌄 Finance Data Ingestion Pipeline with Kafka

In this project, I built a real-time data ingestion pipeline with Apache Kafka and Spark Streaming to collect and process financial data from Yahoo Finance and Finnhub, analyze it in Jupyter Notebook, and generate financial reports using Power BI.

## 🔦 About Project

<img src="./images/about_project.png" style="width: 100%;">

 - **Data Source**: This project uses two main `data sources`: [Yahoo Finance API](https://finance.yahoo.com/) and [Finnhub Stock API](https://finnhub.io/)
   - `Yahoo Finance API`: Data is collected from `Yahoo Finance's API` using the `yfinance` library, collected in `real time` with an interval between data points of `1 minute`, collected data includes indicators such as `Open`, `Volume`, `Close`, `Datetime`,...
   - `Finnhub Stock API`: Data is collected from `Finnhub's API` in `real time`, collected data includes `transaction` indicators such as `v (volume)`, `p (last price)`, `t (time)`,...
 - **Extract Data**: After being collected, data will be written to `Kafka (Kafka Producer)` with different `topics` for each different `data source`.
 - **Transform Data**: After data is sent to `Kafka Topic`, it will be read and retrieved using `Spark Streaming (Kafka Consumer)` and performed `real-time processing`. `Spark` is set up with `3 worker nodes`, applying `Spark's` distributed nature in large data processing.
 - **Load Data**: At the same time, when the data is processed, it will be loaded directly into the `Cassandra` Database using `spark`.
 - **Serving**: Provide detailed insights, create `financial reports` with `Power BI`, and `analyze` investment performance to guide strategic decision-making and optimize portfolio management.
 - **package and orchestration**: Components are packaged using `Docker` and orchestrated using `Apache Airflow`.

## 🚀 Workflow

<img src="./images/workflow_financial.png" style="width: 100%;">

