FROM apache/airflow:3.0.4
USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/
RUN export JAVA_HOME

USER airflow
RUN pip install "apache-airflow-providers-apache-spark" pyspark==3.5.0

