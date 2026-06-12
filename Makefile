# Kafka Docker Makefile
# Usage examples:
#   make create-topic TOPIC=test-topic
#   make list-topics
#   make producer TOPIC=test-topic
#   make consumer TOPIC=test-topic
#   make describe-topic TOPIC=test-topic

KAFKA_CONTAINER ?= kafka
KAFKA_BIN ?= /opt/kafka/bin
PARTITIONS ?= 1
REPLICATION_FACTOR ?= 1
GROUP ?= test-group
MAX_MESSAGES ?= 10
TIMEOUT_MS ?= 10000
S3_BUCKET ?= $(S3_BUCKET_NAME)
S3_PREFIX ?= $(S3_OUTPUT_PREFIX)
FROM_BEGINNING ?=

-include .env

BOOTSTRAP_SERVER ?= $(KAFKA_BOOTSTRAP_SERVERS)
TOPIC ?= $(KAFKA_TOPIC)
CONSUMER_MAX_MESSAGES ?= 0

.PHONY: help shell create-topic list-topics describe-topic delete-topic producer consumer consumer-from-beginning consume-sample dataset-producer s3-consumer groups describe-group offsets

help:
	@echo "Kafka Makefile commands:"
	@echo ""
	@echo "  make shell"
	@echo "      Open shell inside Kafka container"
	@echo ""
	@echo "  make create-topic TOPIC=test-topic"
	@echo "      Create a Kafka topic"
	@echo ""
	@echo "  make list-topics"
	@echo "      List all Kafka topics"
	@echo ""
	@echo "  make describe-topic TOPIC=test-topic"
	@echo "      Describe a topic"
	@echo ""
	@echo "  make delete-topic TOPIC=test-topic"
	@echo "      Delete a topic"
	@echo ""
	@echo "  make producer TOPIC=test-topic"
	@echo "      Start Kafka console producer"
	@echo ""
	@echo "  make consumer TOPIC=test-topic"
	@echo "      Start Kafka console consumer"
	@echo ""
	@echo "  make consumer-from-beginning TOPIC=test-topic"
	@echo "      Consume topic messages from beginning"
	@echo ""
	@echo "  make consume-sample TOPIC=test-topic MAX_MESSAGES=10"
	@echo "      Read a sample of existing topic messages from beginning"
	@echo ""
	@echo "  make dataset-producer"
	@echo "      Publish dataset/indexProcessed.csv to Kafka using kafka_producer.py"
	@echo ""
	@echo "  make s3-consumer"
	@echo "      Consume Kafka messages and upload JSONL batches to S3"
	@echo ""
	@echo "  make groups"
	@echo "      List consumer groups"
	@echo ""
	@echo "  make describe-group GROUP=test-group"
	@echo "      Describe a consumer group"
	@echo ""

shell:
	docker exec -it $(KAFKA_CONTAINER) /bin/bash

create-topic:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-topics.sh \
		--create \
		--if-not-exists \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--partitions $(PARTITIONS) \
		--replication-factor $(REPLICATION_FACTOR)

list-topics:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-topics.sh \
		--list \
		--bootstrap-server $(BOOTSTRAP_SERVER)

describe-topic:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-topics.sh \
		--describe \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP_SERVER)

delete-topic:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-topics.sh \
		--delete \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP_SERVER)

producer:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-console-producer.sh \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP_SERVER)

consumer:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-console-consumer.sh \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--group $(GROUP)

consumer-from-beginning:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-console-consumer.sh \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--from-beginning \
		--group $(GROUP)

consume-sample:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-console-consumer.sh \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--from-beginning \
		--max-messages $(MAX_MESSAGES) \
		--timeout-ms $(TIMEOUT_MS) \
		--group $(GROUP)

dataset-producer:
	python3 kafka_producer.py \
		--bootstrap-servers $(BOOTSTRAP_SERVER) \
		--topic $(TOPIC)

s3-consumer:
	python3 kafka_consumer.py \
		--bootstrap-servers $(BOOTSTRAP_SERVER) \
		--topic $(TOPIC) \
		--bucket $(S3_BUCKET) \
		--prefix $(S3_PREFIX) \
		--max-messages $(CONSUMER_MAX_MESSAGES) \
		$(FROM_BEGINNING)

groups:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-consumer-groups.sh \
		--list \
		--bootstrap-server $(BOOTSTRAP_SERVER)

describe-group:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-consumer-groups.sh \
		--describe \
		--group $(GROUP) \
		--bootstrap-server $(BOOTSTRAP_SERVER)

offsets:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-run-class.sh kafka.tools.GetOffsetShell \
		--broker-list $(BOOTSTRAP_SERVER) \
		--topic $(TOPIC)
