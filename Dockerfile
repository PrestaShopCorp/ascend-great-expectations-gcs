FROM quay.io/ascendio/pyspark:production-io.ascend-spark_2.12-3.1.0-SNAPSHOT
ARG PACKAGE_VERSION
ENV PACKAGE_VERSION=${PACKAGE_VERSION}
ENV DATABRICKS_RUNTIME_VERSION=ASCEND

RUN pip install ascend-great-expectations-gcs==$PACKAGE_VERSION