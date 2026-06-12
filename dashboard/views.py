import os
import time

from django.contrib import messages
from django.http import HttpResponse
from django.shortcuts import redirect, render

from kafka_producer import REQUIRED_FIELDS

from .consumer_runner import CONSUMER_RUNS, start_consumer_run
from .forms import ConsumerRunForm, ProducerRunForm
from .producer_runner import (
    RUNS,
    get_bootstrap_servers,
    save_uploaded_dataset,
    start_producer_run,
)

PIPELINE_STAGES = [
    {
        "name": "CSV Dataset",
        "description": "Historical stock market rows used as the simulation source.",
        "status": "Source",
    },
    {
        "name": "Python Producer",
        "description": "Reads stock rows and publishes live-style events to Kafka.",
        "status": "Planned",
    },
    {
        "name": "Kafka on EC2",
        "description": "Streams market events through a durable message broker.",
        "status": "Planned",
    },
    {
        "name": "Consumer",
        "description": "Consumes Kafka records and writes them into S3.",
        "status": "Planned",
    },
    {
        "name": "Amazon S3",
        "description": "Stores raw stock market event files for analytics.",
        "status": "Planned",
    },
    {
        "name": "AWS Glue",
        "description": "Crawls S3 data and publishes metadata to the Data Catalog.",
        "status": "Planned",
    },
    {
        "name": "Amazon Athena",
        "description": "Runs SQL queries over the cataloged stock market data.",
        "status": "Planned",
    },
]


def home(request):
    return render(request, "dashboard/home.html", {"stages": PIPELINE_STAGES})


def architecture(request):
    return render(request, "dashboard/architecture.html", {"stages": PIPELINE_STAGES})


def status(request):
    return render(request, "dashboard/status.html", {"stages": PIPELINE_STAGES})


def producer(request):
    bootstrap_servers = get_bootstrap_servers()
    initial_topic = os.environ.get("KAFKA_TOPIC", "")
    form = ProducerRunForm(
        initial={
            "bootstrap_servers": bootstrap_servers,
            "topic": initial_topic,
            "delay": 0,
            "limit": 0,
        }
    )

    if request.method == "POST":
        form = ProducerRunForm(request.POST, request.FILES)
        if form.is_valid():
            dataset_path = save_uploaded_dataset(form.cleaned_data["dataset"])
            run_id = start_producer_run(
                bootstrap_servers=form.cleaned_data["bootstrap_servers"],
                topic=form.cleaned_data["topic"],
                dataset_path=dataset_path,
                delay=form.cleaned_data.get("delay") or 0,
                limit=form.cleaned_data.get("limit") or 0,
                create_topic=form.cleaned_data.get("create_topic", False),
            )
            messages.success(request, f"Producer run started: {run_id}")
            return redirect("dashboard:producer")

    runs = sorted(RUNS.values(), key=lambda item: item["id"], reverse=True)
    has_active_runs = any(run["status"] in {"queued", "running"} for run in runs)
    return render(
        request,
        "dashboard/producer.html",
        {
            "form": form,
            "runs": runs,
            "has_active_runs": has_active_runs,
            "bootstrap_servers": bootstrap_servers,
            "bootstrap_servers_configured": bool(bootstrap_servers),
        },
    )


def consumer(request):
    bootstrap_servers = get_bootstrap_servers()
    initial = {
        "bootstrap_servers": bootstrap_servers,
        "topic": os.environ.get("KAFKA_TOPIC", ""),
        "group_id": (
            f"{os.environ.get('KAFKA_CONSUMER_GROUP', 'marketpulse-s3-consumer')}-"
            f"ui-{int(time.time())}"
        ),
        "bucket": os.environ.get("S3_BUCKET_NAME", ""),
        "prefix": os.environ.get("S3_OUTPUT_PREFIX", "stock-market-events"),
        "region": os.environ.get("AWS_REGION", "us-east-1"),
        "batch_size": int(os.environ.get("CONSUMER_BATCH_SIZE", "500")),
        "flush_interval": float(
            os.environ.get("CONSUMER_FLUSH_INTERVAL_SECONDS", "10")
        ),
        "idle_timeout": float(os.environ.get("CONSUMER_IDLE_TIMEOUT_SECONDS", "30")),
        "max_messages": int(os.environ.get("CONSUMER_UI_MAX_MESSAGES", "10")),
        "from_beginning": True,
    }
    form = ConsumerRunForm(initial=initial)

    if request.method == "POST":
        form = ConsumerRunForm(request.POST)
        if form.is_valid():
            run_id = start_consumer_run(
                bootstrap_servers=form.cleaned_data["bootstrap_servers"],
                topic=form.cleaned_data["topic"],
                group_id=form.cleaned_data["group_id"],
                bucket=form.cleaned_data["bucket"],
                prefix=form.cleaned_data["prefix"],
                region=form.cleaned_data["region"],
                batch_size=form.cleaned_data["batch_size"],
                flush_interval=form.cleaned_data["flush_interval"],
                idle_timeout=form.cleaned_data["idle_timeout"],
                max_messages=form.cleaned_data["max_messages"],
                from_beginning=form.cleaned_data.get("from_beginning", False),
            )
            messages.success(request, f"S3 consumer run started: {run_id}")
            return redirect("dashboard:consumer")

    runs = sorted(CONSUMER_RUNS.values(), key=lambda item: item["id"], reverse=True)
    has_active_runs = any(run["status"] in {"queued", "running"} for run in runs)
    return render(
        request,
        "dashboard/consumer.html",
        {
            "form": form,
            "runs": runs,
            "has_active_runs": has_active_runs,
        },
    )


def sample_dataset_template(request):
    ordered_headers = [
        "Index",
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
        "CloseUSD",
    ]
    missing_headers = set(ordered_headers) - REQUIRED_FIELDS
    if missing_headers:
        return HttpResponse(
            "Template headers do not match producer validation.", status=500
        )

    response = HttpResponse(",".join(ordered_headers) + "\n", content_type="text/csv")
    response["Content-Disposition"] = (
        'attachment; filename="marketpulse_sample_template.csv"'
    )
    return response
