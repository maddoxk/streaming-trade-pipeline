# Spark runs in local mode inside this one container: the point of the project is the
# streaming logic, not cluster operations (see README "Limitations").
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.3 psycopg2-binary==2.9.9

# Resolve the Kafka connector at build time so the container starts without network access.
RUN python -c "from pyspark.sql import SparkSession; \
s = SparkSession.builder.master('local[1]') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
    .config('spark.jars.ivy', '/opt/ivy').getOrCreate(); s.stop()"

WORKDIR /app
COPY src /app/src
ENV PYTHONPATH=/app/src
ENTRYPOINT ["python", "-m", "pipeline.spark_job"]
