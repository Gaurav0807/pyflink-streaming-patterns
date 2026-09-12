"""
Simple: Read Kafka → Decode Avro → Upload to S3 (no Flink)
"""
import io
import json
import struct
import boto3
import requests
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.schema_registry import SchemaRegistryClient
import fastavro

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "input-events"
KAFKA_GROUP = "simple-s3-uploader"

REGISTRY_URL = "http://localhost:8081"
SCHEMA_CACHE = {}

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
AWS_SESSION_TOKEN = ""

S3_BUCKET = "gaurav-hudi-data"
S3_PREFIX = "flink-output/kafka-output"


def get_schema(schema_id):
    """Fetch schema from Schema Registry and cache it."""
    if schema_id in SCHEMA_CACHE:
        return SCHEMA_CACHE[schema_id]

    resp = requests.get(f"{REGISTRY_URL}/schemas/ids/{schema_id}")
    resp.raise_for_status()
    schema = fastavro.parse_schema(json.loads(resp.json()["schema"]))
    SCHEMA_CACHE[schema_id] = schema
    return schema


def decode_message(raw_bytes):
    """Decode Confluent-framed Avro message."""
    magic, schema_id = struct.unpack(">bI", raw_bytes[:5])
    if magic != 0:
        raise ValueError(f"Invalid magic byte: {magic}")

    schema = get_schema(schema_id)
    record = fastavro.schemaless_reader(io.BytesIO(raw_bytes[5:]), schema)
    return record


def main():
    # Kafka consumer
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': KAFKA_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([KAFKA_TOPIC])

    # S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name='us-east-1'
    )

    records = []
    count = 0

    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
    print(f"Uploading to S3: s3://{S3_BUCKET}/{S3_PREFIX}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                # No more messages
                if records:
                    upload_to_s3(s3, records)
                break

            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            # Decode and collect
            try:
                record = decode_message(msg.value())
                records.append(record)
                count += 1

                if count % 100 == 0:
                    print(f"  Decoded {count} messages...")

                # Upload in batches of 500
                if len(records) >= 500:
                    upload_to_s3(s3, records)
                    records = []

            except Exception as e:
                print(f"Error decoding message: {e}")
                continue

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        consumer.close()


def upload_to_s3(s3, records):
    """Upload batch of records to S3 as JSON lines."""
    if not records:
        return

    # Create JSON lines
    body = "\n".join(json.dumps(r, default=lambda o: o.isoformat()) for r in records)
    body += "\n"

    # Upload with timestamp
    import time
    timestamp = int(time.time() * 1000)
    key = f"{S3_PREFIX}/batch-{timestamp}.json"

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode())
        print(f"✓ Uploaded {len(records)} records to s3://{S3_BUCKET}/{key}")
    except Exception as e:
        print(f"✗ Upload failed: {e}")


if __name__ == "__main__":
    main()
