# What Happens After an Event Occurs? Building a Real-Time Streaming Pipeline with Kafka and AWS

## A practical journey from fragile point-to-point systems to replayable event streams, durable storage, and analytics

Imagine that a customer places an order, a payment is approved, a delivery vehicle changes location, or a connected device reports an abnormal temperature.

To the person using the application, each action appears simple. A confirmation appears, a status changes, or a dashboard displays a new value. Behind that small interface update, the event may need to travel through an entire organization.

Consider an order being placed. That single event may need to reach many systems at almost the same time.

- Inventory must reserve the products.
- Payment processing must confirm the transaction.
- A warehouse must begin fulfillment.
- A notification service must update the customer.
- A fraud engine may need to evaluate the activity.
- A data platform must retain the event for reporting and analysis.

Now multiply that single action by thousands or millions of users, devices, and services producing updates continuously.

The difficult part is no longer updating the screen. It is delivering each event reliably to multiple systems without allowing one slow or unavailable application to disrupt everything else.

This is the broader real-time streaming problem I wanted to explain. 

---

## The Hidden Complexity Behind a Real-Time Event

Suppose an application sends each new event directly to every downstream service.

At first, the design looks reasonable:

```text
Application -> Operational service
Application -> Notification service
Application -> Analytics service
Application -> Data warehouse
```

But this architecture becomes fragile as the system grows.

What happens if the data warehouse is temporarily unavailable? Should the source application stop and wait?

What happens if one consumer processes only 100 events per second while the source produces 1,000?

What happens when a new fraud-detection service needs the same historical events? Does the source application need to resend everything?

What happens if a downstream service processes an event successfully but crashes before recording that progress?

Direct connections create tight coupling. The producer needs to understand every consumer, every failure mode, and often every retry policy. Adding another consumer increases the producer's responsibility.

This is dangerous in any event-driven system. A missing order, payment, device alert, audit record, or status update can create incorrect business state and leave teams without a reliable history of what happened.

The system needs an intermediate layer that can accept events quickly, preserve their order within a partition, retain them for replay, and allow consumers to operate independently.

That layer can be Apache Kafka.

---

## Kafka as the Event Backbone

Kafka changes the relationship between the source and downstream applications.

Instead of sending an update directly to five services, the source publishes the event once to a Kafka topic. Each downstream application consumes that topic independently.

```text
                              -> Operational consumer
Application -> Kafka          -> Notification consumer
                              -> Analytics consumer
                              -> Data-lake consumer
```

The producer no longer needs to wait for the data lake or know that a new analytics service was deployed.

Kafka is useful here for several reasons.

### Decoupling

Producers and consumers evolve independently. A consumer can be restarted, replaced, or scaled without changing the producer.

### Buffering and backpressure

If a consumer becomes temporarily slow, Kafka retains the events while the consumer catches up. The producer does not need to stop at the speed of the slowest downstream system.

### Replay

Kafka retains events for a configured period. A new consumer group can read from the beginning, while an existing consumer can resume from its last committed offset.

Replay is powerful in the real world. A team can deploy a new surveillance rule today and test it against yesterday's events without asking the source platform to recreate them.

### Independent consumer groups

The risk engine and data-lake writer can read the same topic without interfering with each other. Each group maintains its own progress.

### Ordering within a partition

Events assigned to the same partition retain their order. This matters when reconstructing the sequence of changes for an order, account, device, shipment, or other business entity.

Kafka does not solve every problem automatically. It does not decide the correct schema, eliminate duplicates, or guarantee that a downstream business action completed. Those responsibilities still require careful design. But Kafka provides a durable boundary around which those decisions can be made.

---

## Real-World Applications of Kafka

The same pattern appears across many industries.

### E-commerce

When a customer places an order, the event may trigger inventory reservation, payment processing, shipping, recommendations, analytics, and customer notifications. Kafka allows each workflow to react without forcing the checkout service to coordinate every downstream action synchronously.

### Banking and payments

Transaction events can feed fraud detection, account balances, notification systems, regulatory reporting, and analytical storage. Different consumers can apply different latency and retention requirements to the same event stream.

### Logistics

Package scans, vehicle locations, warehouse updates, and delivery-status changes can be published as events. Tracking applications, routing systems, and operational dashboards can consume the same stream independently.

### Manufacturing and IoT

Machines continuously emit temperature, pressure, vibration, and production readings. Kafka can buffer these readings and distribute them to monitoring, predictive-maintenance, and long-term storage systems.

### Observability

Application logs, metrics, audit events, and traces are generated by many services. A streaming backbone separates event generation from indexing, alerting, and archival.

The business domain changes, but the architectural problem remains similar: many events, many consumers, different processing speeds, and a need to recover or replay.

---

## From Event Stream to Historical Intelligence

Kafka is excellent for moving and retaining event streams, but long-term analytics usually needs another storage layer.

Keeping years of raw events only in Kafka is not always the most economical or convenient option. Analytical tools also benefit from files organized by topic and time.

This is where Amazon S3 enters the architecture.

A consumer reads events from Kafka, groups them into batches, and writes those batches into S3. S3 provides durable and comparatively inexpensive object storage. The same events that powered immediate processing can now support historical analysis.

For example, objects can be organized as:

```text
streaming-events/
  topic=business-events/
  year=2026/month=06/day=12/hour=19/
  batch-<timestamp>-<id>.jsonl
```

AWS Glue can crawl this location, discover its structure, and register metadata in the Glue Data Catalog. Amazon Athena can then query the files with SQL without maintaining a separate database server.

This creates two useful views of the same event:

- Kafka supports immediate, ordered, replayable processing.
- S3 supports durable retention and historical analytics.

---

## The System I Built to Model This Flow

To make these ideas concrete, I built **MarketPulse**, an end-to-end pipeline that turns a static dataset into a stream of events and moves them through Kafka into AWS. Historical stock records provide an accessible sample dataset; the project is really a model of a general streaming architecture.

![MarketPulse architecture](https://raw.githubusercontent.com/kallagoutham/marketpulse/main/Architecture.jpg)

The flow is:

```text
CSV or Excel stock dataset
          |
          v
Django control interface and Python producer
          |
          v
Apache Kafka running on Amazon EC2
          |
          v
Python consumer using confluent-kafka
          |
          v
Amazon S3
          |
          v
AWS Glue Data Catalog
          |
          v
Amazon Athena
```

The project uses a historical stock-market file as its demonstration source. Each row becomes a JSON event, just as an order, payment, sensor reading, or application activity record could become an event in another implementation.

The input follows a defined template:

```text
Index, Date, Open, High, Low, Close, Adj Close, Volume, CloseUSD
```

The producer supports CSV and Excel files, validates the required columns, creates the Kafka topic when needed, and publishes each row as a structured event.

A Django application sits around the pipeline as a control interface. From the UI, a user can:

- choose a Kafka broker and topic;
- upload a CSV or Excel dataset;
- download the expected sample format;
- create a topic if it does not exist;
- control message delay and row limits;
- start a Kafka-to-S3 consumer;
- provide the S3 bucket, prefix, region, batch size, and consumer group;
- monitor producer and consumer runs.

This makes the project feel less like an isolated Python script and more like an operational data tool.

The source code and setup instructions are available at:

**[github.com/kallagoutham/marketpulse](https://github.com/kallagoutham/marketpulse)**

---

## What Happens Behind the Scenes

When a user uploads a dataset from the Producer page, MarketPulse validates the file and converts every row into a JSON event.

An example event looks like this:

```json
{
  "index": "HSI",
  "date": "1987-01-02",
  "open": 2540.100098,
  "high": 2540.100098,
  "low": 2540.100098,
  "close": 2540.100098,
  "adj_close": 2540.100098,
  "volume": 0.0,
  "close_usd": 330.21301274,
  "event_type": "stock_market_tick"
}
```

The producer sends the event to Kafka running on EC2. Kafka stores it in the selected topic and assigns it a partition offset.

The S3 consumer then reads events from Kafka. It does not upload one S3 object for every event because that would create a large number of tiny files. Instead, it accumulates records into newline-delimited JSON batches.

After an S3 batch upload succeeds, the consumer commits its Kafka offset.

The order is intentional:

```text
Read Kafka records
      -> upload batch to S3
      -> commit Kafka offset
```

If the offset were committed before the S3 upload, a failure could cause Kafka to consider the records complete even though they never reached durable storage.

This approach provides at-least-once delivery. A crash between the S3 write and offset commit can produce a duplicate batch, so a production version should use deterministic object identities or deduplication based on Kafka topic, partition, and offset.

---

## The Pain Points This Project Solves

### Static data is not an event stream

MarketPulse converts static historical data into events, allowing streaming behavior to be tested without depending on a live external source.

### Downstream services should not control ingestion

Kafka separates event production from S3 storage and analytics. The producer can continue even when downstream processing runs at a different speed.

### New consumers need historical context

Consumer groups and replay make it possible to process existing topic data from the beginning. This is useful for rebuilding data, testing new logic, or recovering from a downstream outage.

### Raw events need durable, queryable storage

S3 retains batches for long-term analysis. Glue and Athena provide a path from raw event files to cataloged SQL queries.

### Operational users should not edit source code

The Django interface exposes broker, topic, file, consumer group, and S3 settings as validated controls while secrets and environment defaults remain outside the repository.

### Success must mean more than “message received”

The consumer commits progress only after the S3 outcome succeeds. This connects Kafka's transport state to the actual storage objective.

---

## A Debugging Lesson from Building It

The most valuable part of the project was not the happy path. It was diagnosing a consumer that could see Kafka metadata but could not fetch records.

The topic reported:

```text
beginning offset: 0
end offset: 7233
```

Kafka's console consumer on EC2 successfully printed records. Network access and advertised listeners were correct. The original Python client could discover the partition and seek to offset zero, but it returned no record batches.

Switching the consuming path from `kafka-python` to `confluent-kafka`, backed by `librdkafka`, resolved the issue. A verification run consumed ten records and uploaded them to S3.

That debugging process reinforced an important principle: metadata connectivity is not the same as end-to-end data flow.

When debugging a streaming pipeline, verify each stage separately:

1. Can the client open the broker port?
2. Does Kafka advertise a reachable broker address?
3. Does the topic exist?
4. What are the beginning and end offsets?
5. Can a console consumer read actual records?
6. Can the application client fetch those records?
7. Can the consumer write to downstream storage?
8. Are offsets committed only after that write?

This layered approach is applicable far beyond Kafka.

---

## Where This Architecture Fits in the Real World

MarketPulse is a learning project, but its architecture resembles smaller versions of streaming systems used across many industries.

An online retailer could publish order events into Kafka. One consumer could reserve inventory, another could detect suspicious transactions, and another could write raw events into S3 for reporting. A logistics company could apply the same pattern to location updates, while a manufacturer could use it for machine telemetry and predictive maintenance. In each case, Athena could later investigate activity for a particular entity or time range.

The same retained stream could also support new applications that did not exist when the events were first produced. This is one of the strongest reasons to design around events: future consumers can reuse historical facts without changing the original source.

---

## What a Production Version Would Need

MarketPulse intentionally keeps the deployment approachable, so it should not be mistaken for a finished production platform.

The current Kafka deployment uses a single EC2-hosted broker. A production system would normally use multiple brokers, replication, monitoring, and private networking. Amazon MSK could also replace self-managed Kafka.

The Django application currently starts background work in local threads and keeps run status in memory. A production application should use a worker system such as Celery or a dedicated consumer service, with persistent run metadata in PostgreSQL or DynamoDB.

Other production improvements include:

- TLS and SASL authentication for Kafka;
- IAM roles instead of long-lived AWS credentials;
- schema management with Avro, Protobuf, or JSON Schema;
- dead-letter topics for invalid records;
- consumer-lag, throughput, and failure metrics;
- deterministic S3 writes or downstream deduplication;
- file compaction to avoid Athena's small-files problem;
- infrastructure as code for networking, IAM, Kafka, S3, and Glue;
- authentication and role-based permissions for Django actions.

These are not cosmetic enhancements. They address availability, security, observability, and correctness at scale.

---

## Final Thoughts

A real-time system is not defined only by how quickly it moves data. It is defined by what happens when part of the system slows down, fails, restarts, or needs to replay history.

Kafka provides a durable event boundary. S3 provides long-term storage. Glue and Athena provide discovery and analysis. Django provides an operational interface around the workflow.

MarketPulse brings those pieces together in a system that mimics a real-time streaming pipeline and exposes the engineering decisions behind it. Stock data is the example carried through the implementation, not a limitation of the design.

If you are learning Kafka or data engineering, I recommend building the full path rather than stopping after a producer prints “message sent.” Verify the consumer, inspect offsets, force a replay, simulate failure, confirm the downstream object, and understand when progress is committed.

That is where the architecture stops being a diagram and starts becoming a system.

Explore the project here:

**[MarketPulse: Real-Time Streaming Pipeline on GitHub](https://github.com/kallagoutham/marketpulse)**

---
