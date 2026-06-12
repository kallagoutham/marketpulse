from __future__ import annotations

import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from django.conf import settings

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from kafka_producer import (
    build_producer,
    check_bootstrap_servers,
    ensure_topic,
    publish_events,
)

RUNS: dict[str, dict[str, Any]] = {}


def get_bootstrap_servers() -> str:
    if load_dotenv:
        load_dotenv(settings.BASE_DIR / ".env", override=True)
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")


def save_uploaded_dataset(uploaded_file) -> Path:
    upload_dir = Path(settings.MEDIA_ROOT) / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    target_path = upload_dir / f"{uuid.uuid4().hex}_{uploaded_file.name}"

    with target_path.open("wb") as target:
        for chunk in uploaded_file.chunks():
            target.write(chunk)

    return target_path


def start_producer_run(
    *,
    bootstrap_servers: str,
    topic: str,
    dataset_path: Path,
    delay: float,
    limit: int,
    create_topic: bool,
) -> str:
    run_id = uuid.uuid4().hex
    RUNS[run_id] = {
        "id": run_id,
        "bootstrap_servers": bootstrap_servers,
        "topic": topic,
        "dataset": dataset_path.name,
        "status": "queued",
        "messages_sent": 0,
        "limit": limit,
        "delay": delay,
        "error": "",
        "started_at": None,
        "finished_at": None,
    }

    thread = threading.Thread(
        target=_run_producer,
        kwargs={
            "run_id": run_id,
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "dataset_path": dataset_path,
            "delay": delay,
            "limit": limit,
            "create_topic": create_topic,
        },
        daemon=True,
    )
    thread.start()
    return run_id


def _run_producer(
    *,
    run_id: str,
    bootstrap_servers: str,
    topic: str,
    dataset_path: Path,
    delay: float,
    limit: int,
    create_topic: bool,
) -> None:
    run = RUNS[run_id]
    run["status"] = "running"
    run["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

    run["bootstrap_servers"] = bootstrap_servers
    if not bootstrap_servers:
        run["status"] = "failed"
        run["error"] = "KAFKA_BOOTSTRAP_SERVERS is missing from .env."
        run["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return

    producer = None
    try:
        if not check_bootstrap_servers(bootstrap_servers):
            raise RuntimeError("Kafka broker is not reachable from this Django app.")

        if create_topic:
            ensure_topic(
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                partitions=int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "1")),
                replication_factor=int(
                    os.environ.get("KAFKA_TOPIC_REPLICATION_FACTOR", "1")
                ),
            )

        print(
            f"Starting Kafka publish run {run_id}: topic={topic}, "
            f"dataset={dataset_path}, limit={limit}, delay={delay}"
        )
        producer = build_producer(bootstrap_servers)
        sent = publish_events(
            producer=producer,
            topic=topic,
            dataset_path=dataset_path,
            delay_seconds=delay,
            limit=limit,
            progress_callback=lambda count: run.update({"messages_sent": count}),
        )
        run["messages_sent"] = sent
        if sent == 0:
            run["status"] = "failed"
            run["error"] = (
                "No data rows were published. Upload a file with rows below the header."
            )
        else:
            run["status"] = "completed"
        print(
            f"Finished Kafka publish run {run_id}: sent={sent}, status={run['status']}"
        )
    except Exception as exc:
        run["status"] = "failed"
        run["error"] = str(exc)
        print(f"Kafka publish run {run_id} failed: {exc}")
    finally:
        if producer:
            producer.close()
        run["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
