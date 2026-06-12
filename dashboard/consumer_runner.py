from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from kafka_consumer import get_topic_offsets, run_confluent_consumer_to_s3

CONSUMER_RUNS: dict[str, dict[str, Any]] = {}


def start_consumer_run(
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
) -> str:
    run_id = uuid.uuid4().hex
    CONSUMER_RUNS[run_id] = {
        "id": run_id,
        "bootstrap_servers": bootstrap_servers,
        "topic": topic,
        "group_id": group_id,
        "bucket": bucket,
        "prefix": prefix,
        "region": region,
        "status": "queued",
        "messages_uploaded": 0,
        "s3_keys": [],
        "topic_end_offset": None,
        "batch_size": batch_size,
        "idle_timeout": idle_timeout,
        "max_messages": max_messages,
        "error": "",
        "started_at": None,
        "finished_at": None,
    }

    thread = threading.Thread(
        target=_run_consumer,
        kwargs={
            "run_id": run_id,
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "group_id": group_id,
            "bucket": bucket,
            "prefix": prefix,
            "region": region,
            "batch_size": batch_size,
            "flush_interval": flush_interval,
            "idle_timeout": idle_timeout,
            "max_messages": max_messages,
            "from_beginning": from_beginning,
        },
        daemon=True,
    )
    thread.start()
    return run_id


def _run_consumer(
    *,
    run_id: str,
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
) -> None:
    run = CONSUMER_RUNS[run_id]
    run["status"] = "running"
    run["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

    try:
        offsets = get_topic_offsets(bootstrap_servers=bootstrap_servers, topic=topic)
        run["topic_end_offset"] = offsets["end_offset"]
        if offsets["partitions"] == 0:
            run["status"] = "failed"
            run["error"] = f"Kafka topic does not exist or has no partitions: {topic}"
            return

        if offsets["end_offset"] == 0:
            run["status"] = "failed"
            run["error"] = f"Kafka topic has 0 messages available: {topic}"
            return

        def update_progress(uploaded: int, s3_key: str) -> None:
            run["messages_uploaded"] += uploaded
            if uploaded:
                run["s3_keys"].append(f"s3://{bucket}/{s3_key}")

        consumed_count = run_confluent_consumer_to_s3(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            bucket=bucket,
            prefix=prefix,
            region=region,
            batch_size=batch_size,
            flush_interval=flush_interval,
            idle_timeout=idle_timeout,
            max_messages=max_messages,
            from_beginning=from_beginning,
            progress_callback=update_progress,
        )

        if consumed_count == 0:
            run["status"] = "failed"
            run["error"] = (
                "No Kafka messages were uploaded to S3 for this run. "
                f"Topic end offset is {run['topic_end_offset']}. "
                "Use a fresh group with from-beginning enabled."
            )
        else:
            run["status"] = "completed"
    except Exception as exc:
        run["status"] = "failed"
        run["error"] = str(exc)
        print(f"S3 consumer run {run_id} failed: {exc}")
    finally:
        run["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
