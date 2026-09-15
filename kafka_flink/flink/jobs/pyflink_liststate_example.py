"""
ListState: a list of values per key.

Keeps the last 3 temperature readings per sensor and reports a small
sliding-window average. A single ValueState can only hold one number --
ListState is what you reach for when a key needs to remember several values.

Run:
  docker exec jobmanager ./bin/flink run -py /opt/flink/jobs/pyflink_liststate_example.py
"""
import io

import fastavro
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

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


class RecentReadingsWindow(KeyedProcessFunction):
    MAX_READINGS = 3

    def open(self, ctx: RuntimeContext):
        # One growing (but trimmed) list of readings per key (per sensor_id).
        self._recent = ctx.get_list_state(
            ListStateDescriptor("recent_temps", Types.DOUBLE())
        )

    def process_element(self, record, ctx):
        # .get() returns an iterable snapshot -- materialize it to mutate/trim.
        readings = list(self._recent.get() or []) + [record["temperature"]]
        readings = readings[-self.MAX_READINGS:]
        self._recent.update(readings)

        window_avg = sum(readings) / len(readings)
        formatted = [f"{t:.1f}" for t in readings]
        yield (
            f"{record['sensor_id']}: last {len(readings)} readings={formatted}, "
            f"window_avg={window_avg:.2f}"
        )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # State only survives a crash if it's checkpointed -- without this, a
    # failure would lose every sensor's recent-readings window.
    env.enable_checkpointing(10000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("pyflink-liststate-demo")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema(CHARSET))
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
        .map(DecodeAvro(), output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda record: record["sensor_id"])  # scopes the state per sensor
        .process(RecentReadingsWindow(), output_type=Types.STRING())
        .print()
    )

    env.execute("ListState Example")


if __name__ == "__main__":
    main()



# ListState is Flink state that stores multiple values for each key.
# ValueState = one value per key
# ListState = a collection/list of values per key


# RecentReadingsWindow,inheriting from Flink's KeyedProcessFunction  :- 
# Because of this, Flink knows that your class can process each record in a keyed stream.

# Existing state
# [30, 32, 35]
#        +
# New reading
# [33]
#        ↓
# [30, 32, 35, 33]
#        ↓
# readings[-3:]
#        ↓
# [32, 35, 33]    ← 30 is removed here
#        ↓
# update state
#        ↓
# [32, 35, 33]
#        ↓
# calculate average
#        ↓
# 33.33