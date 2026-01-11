FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    confluent-kafka

COPY ml_consumer.py .

CMD ["python", "-u", "ml_consumer.py"]