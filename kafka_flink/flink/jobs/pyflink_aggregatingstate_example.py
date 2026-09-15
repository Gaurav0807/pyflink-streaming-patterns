"""
AggregatingState: like ReducingState, but the accumulator's type can differ
from both the input and the output type.

Computes the running AVERAGE temperature per sensor. The input is a single
temperature (double), the accumulator is a (sum, count) tuple, and the
output is sum/count (double) -- three different types, which ReducingState
can't do (its accumulator must match the input/output type).

This computes the exact same thing as pyflink_stateful_example.py, which
does it by hand with a single ValueState -- this file is the built-in way to
do the same job, with Flink managing the accumulator instead of you.

Run:
  docker exec jobmanager ./bin/flink run -py /opt/flink/jobs/pyflink_aggregatingstate_example.py
"""
import io

import fastavro
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    MapFunction,
    RuntimeContext,
)
from pyflink.datastream.state import AggregatingStateDescriptor

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


class AverageAggregate(AggregateFunction):
    def create_accumulator(self):
        return (0.0, 0)  # (sum, count)

    def add(self, value, accumulator):
        total, count = accumulator
        return (total + value, count + 1)

    def get_result(self, accumulator):
        total, count = accumulator
        return total / count if count else 0.0

    def merge(self, acc_a, acc_b):
        return (acc_a[0] + acc_b[0], acc_a[1] + acc_b[1])


class RunningAverageAgg(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        # One (sum, count) accumulator per key (per sensor_id), managed by
        # AverageAggregate -- .get() applies get_result() for you.
        self._avg_temp = ctx.get_aggregating_state(
            AggregatingStateDescriptor(
                "avg_temp", AverageAggregate(), Types.TUPLE([Types.DOUBLE(), Types.LONG()])
            )
        )

    def process_element(self, record, ctx):
        self._avg_temp.add(record["temperature"])
        yield f"{record['sensor_id']}: running_avg={self._avg_temp.get():.2f}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # State only survives a crash if it's checkpointed -- without this, a
    # failure would lose every sensor's running average.
    env.enable_checkpointing(10000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("pyflink-aggregatingstate-demo")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema(CHARSET))
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
        .map(DecodeAvro(), output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda record: record["sensor_id"])  # scopes the state per sensor
        .process(RunningAverageAgg(), output_type=Types.STRING())
        .print()
    )

    env.execute("AggregatingState Example")


if __name__ == "__main__":
    main()
