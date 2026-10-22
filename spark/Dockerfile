FROM python:3.13.16-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Resolve the Kafka, Delta and S3A jars at build time and bake them into Spark's classpath,
# so containers start without reaching Maven Central.
COPY fetch_jars.py /tmp/fetch_jars.py
RUN python /tmp/fetch_jars.py && rm -rf /tmp/ivy /tmp/fetch_jars.py

RUN useradd --uid 10001 --create-home spark
WORKDIR /app
COPY jobs/ jobs/
USER spark
