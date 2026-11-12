FROM apache/spark:3.5.3 AS spark

FROM apache/airflow:2.10.5

USER root
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark /opt/spark
ENV JAVA_HOME=/opt/java/openjdk SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:/opt/java/openjdk/bin:${PATH}"

RUN curl -fsSL --retry 3 https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar -o /opt/spark/jars/delta-spark_2.12-3.2.1.jar \
    && curl -fsSL --retry 3 https://repo.maven.apache.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar -o /opt/spark/jars/delta-storage-3.2.1.jar \
    && install -d -o airflow -g 0 /opt/airflow/pls-storage

COPY --chown=airflow:0 constraints.txt /opt/airflow/constraints.txt

USER airflow
RUN python -m pip install --no-cache-dir --constraint /opt/airflow/constraints.txt \
    "apache-airflow==2.10.5" \
    "apache-airflow-providers-apache-spark==4.11.3" \
    "delta-spark==3.2.1" \
    "pyspark==3.5.3" \
    && python -m pip check
