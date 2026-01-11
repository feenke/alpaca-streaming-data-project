FROM flink:2.1.1-java17

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && apt-get clean

RUN pip3 install --no-cache-dir \
    apache-flink==2.1.1 \
    kafka-python-ng

RUN curl -o /opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar

WORKDIR /opt/flink-job

COPY aggregation_job.py .
COPY submit_job.sh .
RUN chmod +x submit_job.sh

# bash script to submit the pyflink job via Flink CLI
CMD ["./submit_job.sh"]