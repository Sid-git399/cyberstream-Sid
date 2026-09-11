FROM bitnami/spark:3.5.1
USER root
WORKDIR /app
COPY streaming/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared /app/shared
COPY streaming /app/streaming
ENV KAFKA_BROKERS=kafka:9092 \
    KAFKA_TOPIC=security-events \
    CHECKPOINT_DIR=/data/checkpoints \
    PROCESSED_PATH=/data/processed \
    ALERTS_PATH=/data/alerts \
    DLQ_PATH=/data/dlq \
    INCIDENTS_PATH=/data/incidents
ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.6.1", "/app/streaming/spark_job.py"]
