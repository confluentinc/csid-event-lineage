# OpenTelemetry Instrumentation Extensions for Kafka Clients and Kafka Streams

## Overview

This project provides **experimental OpenTelemetry instrumentation extensions** for Kafka Clients and Kafka Streams that enable the capture of **key/value payloads at the point of production and consumption**. These extensions are designed to enhance the **observability and traceability** of Kafka-based applications by linking spans with actual business payloads—enabling end-to-end lineage visualisation across microservices.

> Unlike the official OpenTelemetry instrumentation (which tracks messaging events), this extension adds **payload awareness**, making it easier to reason about data movement, transformations, and ownership across systems.

This work aligns with the goals of the [Event Lineage reference architecture](https://github.com/confluentinc/csid-event-lineage-demos), which focuses on giving developers and operators insight into **what data was processed, by which component, and why**.

Watch the talk presented at **Current 2022** for the full background:
▶️ [Implementing End-to-End Tracing for Kafka-Based Systems (Confluent Current)](https://www.confluent.io/events/current/2022/implementing-end-to-end-tracing/)

---

## ⚠️ Disclaimer

This repository is not actively maintained and is intended solely as an internal proof of concept. It is provided *as-is*, without guarantees of support, updates, or compatibility with future versions of OpenTelemetry or related tooling.

This is **not an official or supported Confluent integration**. Use of this code is entirely at your own risk, and it should not be considered production-ready without independent validation.

---

## What Problem Does This Solve?

Kafka applications often struggle with **blind spots in distributed tracing**. While standard OpenTelemetry instrumentation tracks the fact that a message was produced or consumed, it **does not track the data content**, nor does it establish strong links between **input and output across processing components**.

This extension addresses that gap by:
- Capturing **key/value payloads** in spans.
- Enabling trace inspection tools like Jaeger or Splunk to display **actual event contents**.
- Preserving **causal lineage** across multiple Kafka topics, producers, consumers, and stream processors.

This allows teams to:
- Debug complex streaming workflows
- Correlate transformations back to source events
- Visualise business-critical event journeys

---

## How It Works

These extensions hook into the Kafka client and Kafka Streams APIs via the OpenTelemetry Java Agent. They inject span attributes representing key/value payloads at the point of produce and consume.

> Note: Span lifecycle and propagation is still handled by the [official OpenTelemetry instrumentation for Kafka](https://github.com/open-telemetry/opentelemetry-java-instrumentation).

---

## Installation

To use the extensions, launch your Kafka-based application with both the **OpenTelemetry Java agent** and this **custom extension JAR**.

```bash
java \
-javaagent:/path/to/agent/opentelemetry-javaagent-all.jar \
-Dotel.instrumentation.kafka.experimental-span-attributes=true \
-Dotel.instrumentation.common.experimental.suppress-messaging-receive-spans=true \
-Dotel.javaagent.extensions=/path/to/extension/instrumentation-x.x.x-all.jar \
-jar your-application.jar
