FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    alpaca-py \
    confluent-kafka

COPY alpaca_data_pull.py .

CMD ["python", "-u", "alpaca_data_pull.py"]