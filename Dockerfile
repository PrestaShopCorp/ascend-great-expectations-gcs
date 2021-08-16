FROM quay.io/ascendio/pyspark:production-io.ascend-spark_2.12-3.1.0-SNAPSHOT

ENV DATABRICKS_RUNTIME_VERSION=ASCEND

RUN pip install ascend-great-expectations-gcs
