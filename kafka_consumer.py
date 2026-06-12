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
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: kafka-python. Install dependencies with "
            "`pip install -r requirements.txt`."
        ) from exc

    return KafkaConsumer(
        topic,
        bootstrap_servers=[
            server.strip() for server in bootstrap_servers.split(",") if server.strip()
        ],
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest" if from_beginning else "latest",
        value_deserializer=decode_message,
        consumer_timeout_ms=1000,
    )


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
    return {
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
        "key": message.key.decode("utf-8") if message.key else None,
        "consumed_at": datetime.now(timezone.utc).isoformat(),
        "value": message.value,
    }


def flush_batch(
    *,
    consumer,
    s3_client,
    bucket: str,
    prefix: str,
    topic: str,
    batch: list[dict[str, Any]],
) -> int:
    if not batch:
        return 0

    key = upload_batch(
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
        topic=topic,
        records=batch,
    )
    consumer.commit()
    print(f"Uploaded {len(batch)} messages to s3://{bucket}/{key}")
    return len(batch)


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

    consumer = build_consumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=args.group_id,
        from_beginning=args.from_beginning,
    )
    s3_client = build_s3_client(args.region)

    batch: list[dict[str, Any]] = []
    consumed_count = 0
    last_flush_time = time.monotonic()

    try:
        while not stop_requested:
            for message in consumer:
                batch.append(make_record(message))
                consumed_count += 1

                if len(batch) >= args.batch_size:
                    flush_batch(
                        consumer=consumer,
                        s3_client=s3_client,
                        bucket=bucket,
                        prefix=args.prefix,
                        topic=topic,
                        batch=batch,
                    )
                    batch.clear()
                    last_flush_time = time.monotonic()

                if args.max_messages and consumed_count >= args.max_messages:
                    stop_requested = True
                    break

            if batch and time.monotonic() - last_flush_time >= args.flush_interval:
                flush_batch(
                    consumer=consumer,
                    s3_client=s3_client,
                    bucket=bucket,
                    prefix=args.prefix,
                    topic=topic,
                    batch=batch,
                )
                batch.clear()
                last_flush_time = time.monotonic()

        if batch:
            flush_batch(
                consumer=consumer,
                s3_client=s3_client,
                bucket=bucket,
                prefix=args.prefix,
                topic=topic,
                batch=batch,
            )
            batch.clear()
    finally:
        consumer.close()

    print(f"Consumer stopped. Total messages uploaded: {consumed_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
