"""
MapState: a key->value map per key -- state nested inside state.

Per sensor, keeps a running count of readings in each temperature band
(HOT / NORMAL / COLD). ValueState could hold one number, ListState a list of
numbers -- MapState is for when each key needs its own little dictionary.

Run:
  docker exec jobmanager ./bin/flink run -py /opt/flink/jobs/pyflink_mapstate_example.py
"""
import io

import fastavro
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor

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


class CategoryCounts(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        # One {category: count} map per key (per sensor_id).
        self._counts = ctx.get_map_state(
            MapStateDescriptor("category_counts", Types.STRING(), Types.LONG())
        )

    def process_element(self, record, ctx):
        temp = record["temperature"]
        category = "HOT" if temp > 30 else "COLD" if temp < 18 else "NORMAL"

        current = self._counts.get(category) or 0
        self._counts.put(category, current + 1)

        totals = ", ".join(f"{k}={v}" for k, v in self._counts.items())
        yield f"{record['sensor_id']}: {category} reading (totals: {totals})"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # State only survives a crash if it's checkpointed -- without this, a
    # failure would lose every sensor's category counts.
    env.enable_checkpointing(10000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("pyflink-mapstate-demo")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema(CHARSET))
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
        .map(DecodeAvro(), output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda record: record["sensor_id"])  # scopes the state per sensor
        .process(CategoryCounts(), output_type=Types.STRING())
        .print()
    )

    env.execute("MapState Example")


if __name__ == "__main__":
    main()

#MapState is basically a Flink-managed dictionary for each key.