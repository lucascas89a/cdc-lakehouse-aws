# Airflow on top of the Spark image: tasks run the Spark jobs in-process (local mode), so Airflow
# never needs the Docker socket, which would be equivalent to root on the host.
# The base image is local-only, so build Spark first:
#   docker compose --profile spark build spark-bronze
ARG SPARK_IMAGE=cdc-lakehouse/spark:local
FROM ${SPARK_IMAGE}

USER root
COPY constraints-3.3.2-py3.13.txt /tmp/constraints.txt
RUN pip install --no-cache-dir "apache-airflow[postgres]==3.3.2" --constraint /tmp/constraints.txt \
 && rm /tmp/constraints.txt

ENV AIRFLOW_HOME=/opt/airflow
RUN mkdir -p /opt/airflow && chown spark:spark /opt/airflow
USER spark
# `standalone` runs scheduler, API server, dag processor and triggerer in one process tree:
# single-node demo mode, not a production deployment. compose.yaml overrides this command to
# stage the UI password file first.
CMD ["airflow", "standalone"]
