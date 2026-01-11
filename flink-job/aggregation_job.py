import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable, UnknownTopicOrPartitionError
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

def wait_for_kafka_and_topic(topic="raw", max_retries=30, retry_delay=5):
    """waiting till kafka and the topic are online"""
    
    print(f"wait for topic '{topic}'...")
    
    for attempt in range(max_retries):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            topics = admin.list_topics()
            admin.close()
            
            if topic in topics:
                print(f"Kafka online and topic '{topic}' exists!")
                return True
            else:
                print(f"topic '{topic}' not existing yet (try {attempt + 1}/{max_retries})")
                
        except NoBrokersAvailable:
            print(f"Kafka not reachable (try {attempt + 1}/{max_retries})")
        except Exception as e:
            print(f"error: {e} (try {attempt + 1}/{max_retries})")
        
        time.sleep(retry_delay)
    
    raise Exception(f"Topic '{topic}' not reachable after {max_retries} tries")


def run_aggregation_job():
    """ Executes the aggregation """
    wait_for_kafka_and_topic("raw")
    
    # set up Flink
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # Kafka Source Table (raw topic)
    table_env.execute_sql(f"""
        CREATE TABLE raw_trades (
            symbol STRING,
            price DOUBLE,
            size DOUBLE,
            exchange STRING,
            `timestamp` STRING,
            ingestion_time STRING,
            event_time AS TO_TIMESTAMP(REPLACE(SUBSTR(`timestamp`, 1, 23), 'T', ' ')),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-aggregation',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)
    
    # Kafka Sink Table (processed topic)
    table_env.execute_sql(f"""
        CREATE TABLE ohlcv_output (
            symbol STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            volume DOUBLE,
            trade_count BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'processed',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)
    
    # OHLCV Aggregation with 1 min tumbling Window
    table_env.execute_sql("""
        INSERT INTO ohlcv_output
        SELECT 
            symbol,
            window_start,
            window_end,
            FIRST_VALUE(price) as open_price,
            MAX(price) as high_price,
            MIN(price) as low_price,
            LAST_VALUE(price) as close_price,
            SUM(size) as volume,
            COUNT(*) as trade_count
        FROM TABLE(
            TUMBLE(TABLE raw_trades, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        GROUP BY symbol, window_start, window_end
    """).wait()


if __name__ == "__main__":
    print("Starting PyFlink OHLCV Aggregation Job...")
    run_aggregation_job()
