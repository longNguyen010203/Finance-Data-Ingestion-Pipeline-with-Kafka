# Use the Bitnami Spark base image
FROM bitnami/spark:3.3.2

WORKDIR /opt/bitnami/spark

USER root


ENV SPARK_HOME=/opt/bitnami/spark
ENV JAVA_HOME=/opt/bitnami/java
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PATH=$JAVA_HOME/bin:$PATH



RUN apt-get update && apt-get install -y curl


RUN curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.13-3.5.0.jar \
    && curl -L -o /opt/bitnami/spark/jars/commons-logging-1.1.3.jar \
    https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar

RUN chmod -R 777 /opt/bitnami/spark/jars/