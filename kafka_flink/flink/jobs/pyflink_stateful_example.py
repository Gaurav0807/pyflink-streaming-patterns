
import io

import fastavro
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

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


class RunningAverage(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        # One (sum, count) pair per key (per sensor_id), managed by Flink.
        # This single state value is the whole point of the file.
        self._state = ctx.get_state(
            ValueStateDescriptor("running_totals", Types.TUPLE([Types.DOUBLE(), Types.LONG()]))
        )

    def process_element(self, record, ctx):
        total, count = self._state.value() or (0.0, 0)
        total += record["temperature"]
        count += 1
        self._state.update((total, count))
        yield f"{record['sensor_id']}: reading #{count}, running_avg={total / count:.2f}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # State only survives a crash if it's checkpointed -- without this, a
    # failure would reset every sensor's running average back to zero.
    env.enable_checkpointing(10000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("pyflink-stateful-demo")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema(CHARSET))
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
        .map(DecodeAvro(), output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda record: record["sensor_id"])  # scopes the state per sensor
        .process(RunningAverage(), output_type=Types.STRING())
        .print()
    )

    env.execute("Stateful Example")


if __name__ == "__main__":
    main()
