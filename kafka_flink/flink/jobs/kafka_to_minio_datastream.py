"""Kafka -> MinIO with the PyFlink DataStream API (no SQL).

Decodes Confluent-framed Avro using schemas fetched from Schema Registry and
writes newline-delimited JSON to MinIO with boto3 -- Flink's S3 file sink
needs a recoverable writer, which MinIO doesn't support.

Run:
  docker exec jobmanager ./bin/flink run -py /opt/flink/jobs/kafka_to_minio_datastream.py
"""
import io
import json
import struct

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import FlatMapFunction, MapFunction, RuntimeContext

BOOTSTRAP = "broker:29092"
TOPIC = "input-events"
REGISTRY = "http://schema-registry:8081"
BUCKET = "sensor-data"
PREFIX = "kafka-output"
FLUSH_EVERY = 500

# latin-1 maps bytes 0..255 to codepoints 0..255 one-to-one, so the raw Avro
# survives the round trip through Flink's string schema. UTF-8 would corrupt it.
CHARSET = "ISO-8859-1"


class DecodeAvro(MapFunction):
    def open(self, ctx: RuntimeContext):
        import requests

        self._http = requests.Session()
        self._schemas = {}

    def map(self, value):
        import fastavro

        raw = value.encode(CHARSET)
        # Confluent wire format: magic byte, 4-byte schema id, Avro payload.
        schema_id = struct.unpack(">I", raw[1:5])[0]
        if schema_id not in self._schemas:
            resp = self._http.get(f"{REGISTRY}/schemas/ids/{schema_id}", timeout=10)
            resp.raise_for_status()
            self._schemas[schema_id] = fastavro.parse_schema(
                json.loads(resp.json()["schema"])
            )

        record = fastavro.schemaless_reader(io.BytesIO(raw[5:]), self._schemas[schema_id])
        # timestamp-millis fields decode to datetime, which json can't handle.
        return json.dumps(record, default=lambda o: o.isoformat())


class MinioSink(FlatMapFunction):
    """Buffers JSON lines, uploading one object per FLUSH_EVERY records."""

    def open(self, ctx: RuntimeContext):
        import boto3

        self._s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )
        self._subtask = ctx.get_index_of_this_subtask()
        self._buffer = []
        self._part = 0

    def _flush(self):
        if not self._buffer:
            return []

        key = f"{PREFIX}/subtask-{self._subtask}-part-{self._part}.json"
        self._s3.put_object(
            Bucket=BUCKET, Key=key, Body=("\n".join(self._buffer) + "\n").encode()
        )
        message = f"wrote s3://{BUCKET}/{key} ({len(self._buffer)} records)"
        self._buffer, self._part = [], self._part + 1
        return [message]

    def flat_map(self, value):
        self._buffer.append(value)
        # Emit only on flush, so the print sink reports files, not every record.
        return self._flush() if len(self._buffer) >= FLUSH_EVERY else []

    def close(self):
        # Job is bounded, so close() commits whatever is left over.
        for message in self._flush():
            print(message)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("pyflink-kafka-to-minio")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        # Bounded: stop at the current end of the topic so close() runs.
        .set_bounded(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema(CHARSET))
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka")
        .map(DecodeAvro(), output_type=Types.STRING())
        .flat_map(MinioSink(), output_type=Types.STRING())
        .print()
    )

    env.execute("Kafka to MinIO")


if __name__ == "__main__":
    main()
