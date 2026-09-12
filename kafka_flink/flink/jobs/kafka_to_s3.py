from pyflink.table import EnvironmentSettings, TableEnvironment


env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)


# Credentials come from environment variables in docker-compose

# Read from Kafka - bounded to what's currently in the topic, so the job
# finishes cleanly and commits the S3 file exactly once at the end
t_env.execute_sql("""
    CREATE TEMPORARY TABLE kafka_in (
        sensor_id STRING,
        temperature DOUBLE,
        humidity DOUBLE,
        event_time BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'input-events',
        'properties.bootstrap.servers' = 'broker:29092',
        'properties.group.id' = 'kafka-to-s3',
        'scan.startup.mode' = 'earliest-offset',
        'scan.bounded.mode' = 'latest-offset',
        'format' = 'avro-confluent',
        'avro-confluent.url' = 'http://schema-registry:8081'
    )
""")

# Write to MinIO S3
t_env.execute_sql("""
    CREATE TEMPORARY TABLE s3_out (
        sensor_id STRING,
        temperature DOUBLE,
        humidity DOUBLE,
        event_time BIGINT
    ) WITH (
        'connector' = 'filesystem',
        'path' = 's3://gaurav-hudi-data/flink-output/kafka-output/',
        'format' = 'json'
    )
""")

print("Starting: Kafka -> MinIO S3")
t_env.execute_sql("INSERT INTO s3_out SELECT * FROM kafka_in").wait()
print("Done!")
