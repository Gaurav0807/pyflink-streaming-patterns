"""
Sample PyFlink streaming job:

    input-events (Kafka, Avro/Schema Registry)
        -> transform (C -> F, threshold classification)
        -> output-events (Kafka, Avro/Schema Registry)

Submit with the cluster running (see README.md):

    docker exec -it jobmanager ./bin/flink run -py /opt/flink/jobs/kafka_pyflink_job.py

Watch it in the Flink Web UI at http://localhost:8082
"""
from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.execute_sql(f"""
        CREATE TABLE input_events (
            sensor_id STRING,
            temperature DOUBLE,
            humidity DOUBLE,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'input-events',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'pyflink-sensor-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'avro-confluent',
            'avro-confluent.url' = '{SCHEMA_REGISTRY_URL}'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE output_events (
            sensor_id STRING,
            temperature_f DOUBLE,
            humidity DOUBLE,
            status STRING,
            event_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output-events',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'avro-confluent',
            'avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
            'avro-confluent.subject' = 'output-events-value'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO output_events
        SELECT
            sensor_id,
            temperature * 9 / 5 + 32 AS temperature_f,
            humidity,
            CASE
                WHEN temperature > 30 THEN 'HOT'
                WHEN temperature < 18 THEN 'COLD'
                ELSE 'NORMAL'
            END AS status,
            event_time
        FROM input_events
    """).wait()


if __name__ == "__main__":
    main()
