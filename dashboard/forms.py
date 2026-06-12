from django import forms


def validate_bootstrap_servers(bootstrap_servers: str) -> str:
    bootstrap_servers = bootstrap_servers.strip()
    for server in [
        item.strip() for item in bootstrap_servers.split(",") if item.strip()
    ]:
        host, separator, port = server.rpartition(":")
        if not separator or not host or not port.isdigit():
            raise forms.ValidationError(
                "Use host:port format, for example 18.226.240.111:9092."
            )
    return bootstrap_servers


class ProducerRunForm(forms.Form):
    bootstrap_servers = forms.CharField(
        label="Kafka broker",
        max_length=500,
        help_text="Kafka bootstrap server, for example 18.226.240.111:9092.",
    )
    topic = forms.CharField(
        max_length=249,
        help_text="Kafka topic to create/use for this upload.",
    )
    dataset = forms.FileField(
        help_text="Upload a .csv, .xlsx, or .xlsm file with the stock market template columns.",
    )
    delay = forms.FloatField(
        initial=0,
        min_value=0,
        required=False,
        help_text="Seconds to wait between messages.",
    )
    limit = forms.IntegerField(
        initial=0,
        min_value=0,
        required=False,
        help_text="Maximum rows to publish. Use 0 for all rows.",
    )
    create_topic = forms.BooleanField(
        initial=True,
        required=False,
        help_text="Create the topic if it does not already exist.",
    )

    def clean_dataset(self):
        dataset = self.cleaned_data["dataset"]
        allowed_extensions = {".csv", ".xlsx", ".xlsm"}
        filename = dataset.name.lower()
        if not any(filename.endswith(extension) for extension in allowed_extensions):
            raise forms.ValidationError("Upload a .csv, .xlsx, or .xlsm file.")
        return dataset

    def clean_bootstrap_servers(self):
        return validate_bootstrap_servers(self.cleaned_data["bootstrap_servers"])


class ConsumerRunForm(forms.Form):
    bootstrap_servers = forms.CharField(
        label="Kafka broker",
        max_length=500,
        help_text="Kafka bootstrap server, for example 18.226.240.111:9092.",
    )
    topic = forms.CharField(
        max_length=249,
        help_text="Kafka topic to consume from.",
    )
    group_id = forms.CharField(
        max_length=249,
        help_text="Consumer group used for offset tracking.",
    )
    bucket = forms.CharField(
        label="S3 bucket",
        max_length=255,
        help_text="Destination S3 bucket name.",
    )
    prefix = forms.CharField(
        label="S3 prefix",
        max_length=500,
        initial="stock-market-events",
        help_text="Folder-style S3 prefix for uploaded JSONL batches.",
    )
    region = forms.CharField(
        label="AWS region",
        max_length=64,
        initial="us-east-1",
    )
    batch_size = forms.IntegerField(
        initial=500,
        min_value=1,
        help_text="Messages per S3 object.",
    )
    flush_interval = forms.FloatField(
        initial=10,
        min_value=1,
        help_text="Seconds before flushing a partial batch.",
    )
    idle_timeout = forms.FloatField(
        initial=30,
        min_value=0,
        help_text="Stop if no Kafka messages arrive for this many seconds. Use 0 to disable.",
    )
    max_messages = forms.IntegerField(
        initial=10,
        min_value=0,
        help_text="Stop after this many messages. Use 0 to run continuously.",
    )
    from_beginning = forms.BooleanField(
        initial=False,
        required=False,
        help_text="Read from earliest offsets when this group has no committed offset.",
    )

    def clean_bootstrap_servers(self):
        return validate_bootstrap_servers(self.cleaned_data["bootstrap_servers"])
