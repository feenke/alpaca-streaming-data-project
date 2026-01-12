import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")


def run_aggregation_job():
    print("Initializing Flink environment...")
    
    # Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(60000)  # every 60 seconds
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    print("Creating raw_trades table...")
    
    table_env.execute_sql(f"""
        CREATE TABLE raw_trades (
            symbol STRING,
            price DOUBLE,
            size DOUBLE,
            exchange STRING,
            `timestamp` STRING,
            ingestion_time STRING,
            event_time AS TO_TIMESTAMP(REPLACE(SUBSTR(`timestamp`, 1, 23), 'T', ' ')),
            WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
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
    
    print("Creating ohlcv_output table...")
    
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
    
    print("Starting OHLCV aggregation...")
    
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
            TUMBLE(TABLE raw_trades, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
        )
        GROUP BY symbol, window_start, window_end
    """).wait()


if __name__ == "__main__":
    print("Starting PyFlink OHLCV Aggregation Job...")
    run_aggregation_job()