"""
ValueState: one value per key.

Remembers each sensor's previous temperature and reports whether the new
reading went UP, DOWN, or stayed the SAME. Impossible without state -- a
stateless map only ever sees the current record, never the one before it.

Run:
  docker exec jobmanager ./bin/flink run -py /opt/flink/jobs/pyflink_valuestate_example.py
"""
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


class TemperatureTrend(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        # One previous-temperature value per key (per sensor_id).
        self._last_temp = ctx.get_state(ValueStateDescriptor("last_temp", Types.DOUBLE()))

    def process_element(self, record, ctx): #ctx is Flink's Context.
        previous = self._last_temp.value()
        current = record["temperature"]
        self._last_temp.update(current)

        if previous is None:
            trend = "first reading"
        elif current > previous:
            trend = "UP"
        elif current < previous:
            trend = "DOWN"
        else:
            trend = "SAME"
        yield f"{record['sensor_id']}: {current:.2f} ({trend})"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # State only survives a crash if it's checkpointed -- without this, a
    # failure would lose every sensor's last-seen temperature.
    env.disable_operator_chaining()

    env.enable_checkpointing(10000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("pyflink-valuestate-demo")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema(CHARSET))
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
        .map(DecodeAvro(), output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda record: record["sensor_id"])  # scopes the state per sensor
        .process(TemperatureTrend(), output_type=Types.STRING())
        .print()
    )

    env.execute("ValueState Example")


if __name__ == "__main__":
    main()


#ValueState is Flink's way of remembering ONE value for each key.

#When the TemperatureTrend operator starts, Flink calls: it call open() functions automatically,
#open() is basically an initialization method.

# sensor-2: 22.83 (first reading)
# sensor-2: 16.00 (DOWN)
# sensor-2: 31.37 (UP)
# sensor-5: 17.50 (first reading)
# sensor-2: 30.67 (DOWN)
# sensor-2: 22.82 (DOWN)
# sensor-5: 30.15 (UP)
# sensor-3: 25.29 (first reading)
