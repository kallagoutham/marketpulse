"""Consume Kafka stock events and upload them to Amazon S3 as JSONL batches."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


DEFAULT_GROUP_ID = "marketpulse-s3-consumer"
DEFAULT_BATCH_SIZE = 500
DEFAULT_FLUSH_INTERVAL_SECONDS = 10

stop_requested = False


def request_stop(signum: int, frame: object) -> None:
    del signum, frame
    global stop_requested
    stop_requested = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume Kafka messages and upload JSONL batches to S3."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        help="Kafka bootstrap servers. Defaults to KAFKA_BOOTSTRAP_SERVERS from .env.",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC"),
        help="Kafka topic to consume. Defaults to KAFKA_TOPIC from .env.",
    )
    parser.add_argument(
        "--group-id",
        default=os.environ.get("KAFKA_CONSUMER_GROUP", DEFAULT_GROUP_ID),
        help="Kafka consumer group id.",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("S3_BUCKET_NAME"),
        help="S3 bucket name. Defaults to S3_BUCKET_NAME from .env.",
    )
    parser.add_argument(
        "--prefix",
        default=os.environ.get("S3_OUTPUT_PREFIX", "stock-market-events"),
        help="S3 key prefix for uploaded JSONL files.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", "us-east-1"),
        help="AWS region for S3.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("CONSUMER_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))),
        help="Number of messages per S3 object.",
    )
    parser.add_argument(
        "--flush-interval",
        type=float,
        default=float(
            os.environ.get(
                "CONSUMER_FLUSH_INTERVAL_SECONDS",
                str(DEFAULT_FLUSH_INTERVAL_SECONDS),
            )
        ),
        help="Maximum seconds to wait before flushing a partial batch.",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=float(os.environ.get("CONSUMER_IDLE_TIMEOUT_SECONDS", "30")),
        help="Stop if no Kafka messages arrive for this many seconds. Use 0 to disable.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=int(os.environ.get("CONSUMER_MAX_MESSAGES", "0")),
        help="Stop after this many messages. Use 0 to run continuously.",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        default=os.environ.get("CONSUMER_FROM_BEGINNING", "False").lower() == "true",
        help="Read from earliest offsets when this consumer group has no committed offset.",
    )
    return parser.parse_args()


def require_value(value: str | None, name: str) -> str:
    if not value:
        raise SystemExit(f"{name} is required in .env or as a CLI argument.")
    return value


def build_consumer(
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    from_beginning: bool,
):
    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: kafka-python. Install dependencies with "
            "`pip install -r requirements.txt`."
        ) from exc

    consumer = KafkaConsumer(
        bootstrap_servers=[
            server.strip() for server in bootstrap_servers.split(",") if server.strip()
        ],
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest" if from_beginning else "latest",
        consumer_timeout_ms=1000,
    )

    if from_beginning:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(
                f"Kafka topic does not exist or has no partitions: {topic}"
            )
        topic_partitions = [
            TopicPartition(topic, partition) for partition in partitions
        ]
        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)
        consumer.assign(topic_partitions)
        print(f"Assigned partitions from beginning: {topic_partitions}")
        for topic_partition in topic_partitions:
            consumer.seek(topic_partition, beginning_offsets[topic_partition])
            print(
                f"Seeked {topic_partition}: "
                f"beginning={beginning_offsets[topic_partition]} "
                f"end={end_offsets[topic_partition]} "
                f"position={consumer.position(topic_partition)}"
            )
    else:
        consumer.subscribe([topic])

    return consumer


def print_cluster_metadata(consumer) -> None:
    cluster = consumer._client.cluster
    brokers = sorted(
        f"{broker.node_id}@{broker.host}:{broker.port}" for broker in cluster.brokers()
    )
    print(f"Kafka metadata brokers: {', '.join(brokers) if brokers else 'none'}")


def get_topic_offsets(*, bootstrap_servers: str, topic: str) -> dict[str, int]:
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: confluent-kafka. Install dependencies with "
            "`pip install -r requirements.txt`."
        ) from exc

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"marketpulse-offset-check-{uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            return {"partitions": 0, "beginning_offset": 0, "end_offset": 0}

        beginning_offset = 0
        end_offset = 0
        for partition_id in topic_metadata.partitions.keys():
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition_id),
                timeout=10,
                cached=False,
            )
            beginning_offset += low
            end_offset += high

        return {
            "partitions": len(topic_metadata.partitions),
            "beginning_offset": beginning_offset,
            "end_offset": end_offset,
        }
    finally:
        consumer.close()


def build_s3_client(region: str):
    try:
        import boto3
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: boto3. Install dependencies with "
            "`pip install -r requirements.txt`."
        ) from exc

    return boto3.client("s3", region_name=region)


def decode_message(raw_value: bytes) -> dict[str, Any]:
    try:
        return json.loads(raw_value.decode("utf-8"))
    except json.JSONDecodeError:
        return {"raw_message": raw_value.decode("utf-8", errors="replace")}


def build_s3_key(prefix: str, topic: str) -> str:
    now = datetime.now(timezone.utc)
    return (
        f"{prefix.rstrip('/')}/topic={topic}/"
        f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
        f"batch-{now:%Y%m%dT%H%M%S}-{uuid4().hex}.jsonl"
    )


def upload_batch(
    *,
    s3_client,
    bucket: str,
    prefix: str,
    topic: str,
    records: list[dict[str, Any]],
) -> str:
    body = "\n".join(json.dumps(record, default=str) for record in records) + "\n"
    key = build_s3_key(prefix=prefix, topic=topic)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return key


def make_record(message) -> dict[str, Any]:
    value = (
        decode_message(message.value)
        if isinstance(message.value, bytes)
        else message.value
    )
    return {
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
        "key": message.key.decode("utf-8") if message.key else None,
        "consumed_at": datetime.now(timezone.utc).isoformat(),
        "value": value,
    }


def flush_batch(
    *,
    consumer,
    s3_client,
    bucket: str,
    prefix: str,
    topic: str,
    batch: list[dict[str, Any]],
) -> tuple[int, str]:
    if not batch:
        return 0, ""

    key = upload_batch(
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
        topic=topic,
        records=batch,
    )
    consumer.commit()
    print(f"Uploaded {len(batch)} messages to s3://{bucket}/{key}")
    return len(batch), key


def make_confluent_record(message) -> dict[str, Any]:
    key = message.key()
    value = message.value()
    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "key": key.decode("utf-8") if key else None,
        "consumed_at": datetime.now(timezone.utc).isoformat(),
        "value": decode_message(value) if isinstance(value, bytes) else value,
    }


def run_confluent_consumer_to_s3(
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    bucket: str,
    prefix: str,
    region: str,
    batch_size: int,
    flush_interval: float,
    idle_timeout: float,
    max_messages: int,
    from_beginning: bool,
    progress_callback=None,
) -> int:
    try:
        from confluent_kafka import Consumer, KafkaError, TopicPartition
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: confluent-kafka. Install dependencies with "
            "`pip install -r requirements.txt`."
        ) from exc

    s3_client = build_s3_client(region)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
        }
    )

    batch: list[dict[str, Any]] = []
    consumed_count = 0
    last_flush_time = time.monotonic()
    last_message_time = time.monotonic()

    try:
        if from_beginning:
            metadata = consumer.list_topics(topic, timeout=10)
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(f"Kafka topic metadata is unavailable: {topic}")

            partitions = [
                TopicPartition(topic, partition_id, 0)
                for partition_id in topic_metadata.partitions.keys()
            ]
            consumer.assign(partitions)
            print(f"Assigned confluent partitions from beginning: {partitions}")
        else:
            consumer.subscribe([topic])

        while True:
            message = consumer.poll(1.0)
            if message is None:
                if (
                    idle_timeout
                    and time.monotonic() - last_message_time >= idle_timeout
                ):
                    print(
                        f"No Kafka messages received for {idle_timeout:g} seconds; stopping."
                    )
                    break
            elif message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError(message.error())
            else:
                last_message_time = time.monotonic()
                batch.append(make_confluent_record(message))
                consumed_count += 1

                if len(batch) >= batch_size:
                    uploaded, s3_key = flush_batch(
                        consumer=consumer,
                        s3_client=s3_client,
                        bucket=bucket,
                        prefix=prefix,
                        topic=topic,
                        batch=batch,
                    )
                    if progress_callback:
                        progress_callback(uploaded, s3_key)
                    batch.clear()
                    last_flush_time = time.monotonic()

                if max_messages and consumed_count >= max_messages:
                    break

            if batch and time.monotonic() - last_flush_time >= flush_interval:
                uploaded, s3_key = flush_batch(
                    consumer=consumer,
                    s3_client=s3_client,
                    bucket=bucket,
                    prefix=prefix,
                    topic=topic,
                    batch=batch,
                )
                if progress_callback:
                    progress_callback(uploaded, s3_key)
                batch.clear()
                last_flush_time = time.monotonic()

        if batch:
            uploaded, s3_key = flush_batch(
                consumer=consumer,
                s3_client=s3_client,
                bucket=bucket,
                prefix=prefix,
                topic=topic,
                batch=batch,
            )
            if progress_callback:
                progress_callback(uploaded, s3_key)
            batch.clear()
    finally:
        consumer.close()

    return consumed_count


def main() -> int:
    global stop_requested

    if load_dotenv:
        load_dotenv(".env", override=True)

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    args = parse_args()
    bootstrap_servers = require_value(args.bootstrap_servers, "KAFKA_BOOTSTRAP_SERVERS")
    topic = require_value(args.topic, "KAFKA_TOPIC")
    bucket = require_value(args.bucket, "S3_BUCKET_NAME")

    print(f"Consuming Kafka topic: {topic}")
    print(f"Kafka broker: {bootstrap_servers}")
    print(f"S3 destination: s3://{bucket}/{args.prefix.strip('/')}/")
    print(f"Consumer group: {args.group_id}")

    offsets = get_topic_offsets(bootstrap_servers=bootstrap_servers, topic=topic)
    print(
        "Topic offsets: "
        f"partitions={offsets['partitions']} "
        f"beginning={offsets['beginning_offset']} "
        f"end={offsets['end_offset']}"
    )
    if offsets["partitions"] == 0:
        print(
            f"Kafka topic does not exist or has no partitions: {topic}", file=sys.stderr
        )
        return 1
    if offsets["end_offset"] == 0:
        print(f"Kafka topic has 0 messages available: {topic}", file=sys.stderr)
        return 1

    consumed_count = run_confluent_consumer_to_s3(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=args.group_id,
        bucket=bucket,
        prefix=args.prefix,
        region=args.region,
        batch_size=args.batch_size,
        flush_interval=args.flush_interval,
        idle_timeout=args.idle_timeout,
        max_messages=args.max_messages,
        from_beginning=args.from_beginning,
    )

    print(f"Consumer stopped. Total messages uploaded: {consumed_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
