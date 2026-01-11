FROM flink:2.1.1-java17

# Python installieren
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && apt-get clean

# PyFlink und Abhängigkeiten
RUN pip3 install --no-cache-dir \
    apache-flink==2.1.1 \
    kafka-python-ng

# Kafka Connector JAR herunterladen
RUN curl -o /opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar


WORKDIR /opt/flink-job

COPY aggregation_job.py .

CMD ["python", "-u", "aggregation_job.py"]