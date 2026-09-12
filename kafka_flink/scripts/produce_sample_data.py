"""
Produces sample Avro-encoded SensorEvent messages to the `input-events`
topic, registering the schema with Confluent Schema Registry.

Run from the host machine (venv with requirements.txt installed):

    python scripts/produce_sample_data.py                # runs forever, Ctrl+C to stop
    python scripts/produce_sample_data.py --count 20      # produce exactly 20 then exit

Assumes the stack from docker-compose.yml is up and its ports are
published to localhost (broker -> 9092, schema-registry -> 8081).
"""
import argparse
import itertools
import pathlib
import random
import time

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)
from confluent_kafka import Producer

TOPIC = "input-events"
SCHEMA_PATH = pathlib.Path(__file__).resolve().parent.parent / "schema" / "sensor_event.avsc"


def make_event(sensor_id: str) -> dict:
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "event_time": int(time.time() * 1000),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--schema-registry-url", default="http://localhost:8081")
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of messages to produce, then exit. Omit to run forever (Ctrl+C to stop).",
    )
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    args = parser.parse_args()

    schema_str = SCHEMA_PATH.read_text()

    schema_registry_client = SchemaRegistryClient({"url": args.schema_registry_url})
    key_serializer = StringSerializer("utf_8")
    value_serializer = AvroSerializer(schema_registry_client, schema_str)

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    sensor_ids = [f"sensor-{i}" for i in range(1, 6)]

    iterator = range(args.count) if args.count is not None else itertools.count()
    produced = 0

    try:
        for _ in iterator:
            sensor_id = random.choice(sensor_ids)
            event = make_event(sensor_id)
            producer.produce(
                topic=TOPIC,
                key=key_serializer(sensor_id),
                value=value_serializer(
                    event, SerializationContext(TOPIC, MessageField.VALUE)
                ),
                on_delivery=delivery_report,
            )
            produced += 1
            producer.poll(0)
            time.sleep(args.sleep_seconds)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush()
        print(f"Produced {produced} messages to '{TOPIC}'.")


if __name__ == "__main__":
    main()
