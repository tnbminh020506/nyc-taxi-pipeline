# Bước 2.1–2.3 | Mạnh | Môi trường chạy Spark Batch, Structured Streaming và test xử lý.
#
# Đầu vào
# - Image Spark 3.5.3 chính thức (Java 11, Python 3.8, Scala 2.12).
#
# Đầu ra
# - Image có PySpark trên PYTHONPATH, connector Kafka cho Structured Streaming,
#   psycopg2 để ghi PostgreSQL và pytest để chạy tests/test_processing.py.
#
# Cách dùng (chạy tại thư mục gốc repo)
#   docker build -t nyc-taxi-spark:dev docker/spark
#   docker run --rm -v "$PWD":/workspace nyc-taxi-spark:dev python3 -m pytest tests/test_processing.py
FROM apache/spark:3.5.3-python3

USER root

# Connector Kafka khớp Spark 3.5.3 / Scala 2.12, tải sẵn để không cần mạng lúc chạy stream.
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && wget -q ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
 && wget -q ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
 && wget -q ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && wget -q ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN pip3 install --no-cache-dir "pytest==8.3.5" "psycopg2-binary==2.9.10"

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/workspace/src \
    PYSPARK_PYTHON=python3 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /workspace
USER spark
