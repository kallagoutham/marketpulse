from django import forms


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
        bootstrap_servers = self.cleaned_data["bootstrap_servers"].strip()
        for server in [
            item.strip() for item in bootstrap_servers.split(",") if item.strip()
        ]:
            host, separator, port = server.rpartition(":")
            if not separator or not host or not port.isdigit():
                raise forms.ValidationError(
                    "Use host:port format, for example 18.226.240.111:9092."
                )
        return bootstrap_servers
