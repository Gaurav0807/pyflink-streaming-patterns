"""
ReducingState: like ValueState, but you never read-modify-write it yourself.

Computes the running MAX temperature per sensor. You call .add(x) and Flink
folds x into the existing stored value using the ReduceFunction you supply --
Flink owns the update logic, not your process_element code.

Run:
  docker exec jobmanager ./bin/flink run -py /opt/flink/jobs/pyflink_reducingstate_example.py
"""
import io

import fastavro
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    MapFunction,
    ReduceFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ReducingStateDescriptor

BOOTSTRAP = "broker:29092"
TOPIC = "input-events"

# latin-1 maps bytes 0..255 to codepoints 0..255 one-to-one, so the raw Avro
# survives the round trip through Flink's string schema. UTF-8 would corrupt it.
CHARSET = "ISO-8859-1"

# This demo's data always uses this one schema (schema/sensor_event.avsc), so
# it's hardcoded rather than fetched from Schema Registry by id -- simpler,
# but only correct as long as the schema never changes.
SENSOR_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "SensorEvent",
    "fields": [
        {"name": "sensor_id", "type": "string"},
        {"name": "temperature", "type": "double"},
        {"name": "humidity", "type": "double"},
        {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
})


class DecodeAvro(MapFunction):
    def map(self, value):
        raw = value.encode(CHARSET)
        # First 5 bytes are the Confluent wire-format header (magic byte +
        # schema id) -- skip them, since we already know the schema.
        return fastavro.schemaless_reader(io.BytesIO(raw[5:]), SENSOR_SCHEMA)


class MaxTemperature(ReduceFunction):
    def reduce(self, value1, value2):
        return value1 if value1 > value2 else value2


class RunningMax(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        # One running-max value per key (per sensor_id), updated by MaxTemperature.
        self._max_temp = ctx.get_reducing_state(
            ReducingStateDescriptor("max_temp", MaxTemperature(), Types.DOUBLE())
        )

    def process_element(self, record, ctx):
        self._max_temp.add(record["temperature"])
        yield f"{record['sensor_id']}: running_max={self._max_temp.get():.2f}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # State only survives a crash if it's checkpointed -- without this, a
    # failure would lose every sensor's running max.
    env.enable_checkpointing(10000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("pyflink-reducingstate-demo")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema(CHARSET))
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
        .map(DecodeAvro(), output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda record: record["sensor_id"])  # scopes the state per sensor
        .process(RunningMax(), output_type=Types.STRING())
        .print()
    )

    env.execute("ReducingState Example")


if __name__ == "__main__":
    main()
