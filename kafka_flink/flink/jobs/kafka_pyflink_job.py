"""

docker exec -it jobmanager ./bin/flink run -py /opt/flink/jobs/kafka_pyflink_job.py

Watch it in the Flink Web UI at http://localhost:8082
"""
from pyflink.table import EnvironmentSettings, TableEnvironment, StreamTableEnvironment
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
    ExternalizedCheckpointCleanup,
)


KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"


#   'earliest-offset' - read the entire topic from the beginning
#   'latest-offset'   - skip all existing data, only see new messages from now on
#   'timestamp'       - start at the first message at/after SCAN_STARTUP_TIMESTAMP_MILLIS
SCAN_STARTUP_MODE = "earliest-offset"
SCAN_STARTUP_TIMESTAMP_MILLIS = 1789236113617  # only used when SCAN_STARTUP_MODE == 'timestamp'


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.disable_operator_chaining()

    # Take a consistent snapshot (Kafka offsets + any operator state) every
    # 10s. On failure, Flink resumes from the last one instead of restarting
    # from scan.startup.mode.
    env.enable_checkpointing(10000)  # interval in ms

    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_checkpoint_timeout(60000)  # fail a stuck checkpoint after 60s
    checkpoint_config.set_min_pause_between_checkpoints(5000)  # breathing room between attempts
    checkpoint_config.set_max_concurrent_checkpoints(1)
    # Without this, cancelling the job deletes its checkpoints -- this keeps
    # the last one so a manually-stopped job can still be resumed from it.
    checkpoint_config.enable_externalized_checkpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    # state.checkpoints.dir is already set globally in docker-compose.yml
    # (file:///opt/flink/checkpoints), so no need to repeat it here.

    # env_settings = EnvironmentSettings.in_streaming_mode()
    # t_env = TableEnvironment.create(env_settings)

    t_env = StreamTableEnvironment.create(env)
    # state.checkpoints.num-retained is NOT a per-job setting -- Flink reads it
    # from the JobManager process's own flink-conf.yaml, not from anything
    # shipped with the JobGraph. It's set once, cluster-wide, in
    # docker-compose.yml alongside state.checkpoints.dir.


    startup_option = f"'scan.startup.mode' = '{SCAN_STARTUP_MODE}'"
    if SCAN_STARTUP_MODE == "timestamp":
        startup_option += f",\n            'scan.startup.timestamp-millis' = '{SCAN_STARTUP_TIMESTAMP_MILLIS}'"

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
            {startup_option},
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
