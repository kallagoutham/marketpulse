# Kafka Docker Makefile
# Usage examples:
#   make create-topic TOPIC=test-topic
#   make list-topics
#   make producer TOPIC=test-topic
#   make consumer TOPIC=test-topic
#   make describe-topic TOPIC=test-topic

KAFKA_CONTAINER ?= kafka
KAFKA_BIN ?= /opt/kafka/bin
BOOTSTRAP_SERVER ?= localhost:9092
TOPIC ?= test-topic
PARTITIONS ?= 1
REPLICATION_FACTOR ?= 1
GROUP ?= test-group

.PHONY: help shell create-topic list-topics describe-topic delete-topic producer consumer consumer-from-beginning groups describe-group offsets

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