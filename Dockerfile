FROM apache/airflow:2.10.3

# ENV SPARK_HOME=/opt/bitnami/spark
# ENV JAVA_HOME=/opt/bitnami/java
# ENV PATH=$SPARK_HOME/bin:$PATH
# ENV PATH=$JAVA_HOME/bin:$PATH
# ENV SPARK_SUBMIT_BINARY=/opt/bitnami/spark/bin/spark-submit
USER airflow

COPY requirements.txt /requirements.txt

USER root
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean

USER airflow
RUN pip install --upgrade pip
RUN pip install -r /requirements.txt

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME