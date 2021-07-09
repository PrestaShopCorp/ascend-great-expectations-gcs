FROM quay.io/ascendio/pyspark:production-io.ascend-spark_2.12-3.1.0-SNAPSHOT
ENV DATABRICKS_RUNTIME_VERSION=ASCEND
COPY .  /tmp/pypkg/ascend
RUN pip install /tmp/pypkg/ascend


