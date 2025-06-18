# OpenTelemetry Instrumentation Extensions for Kafka Clients and Kafka Streams

⚠️ **Disclaimer**

This repository is not actively maintained and is intended solely as an internal proof of concept. It is provided *as-is*, without any guarantees of support, updates, or compatibility with future versions of OpenTelemetry or related tooling.

This is not an official project and does not represent a supported product or endorsed integration. Users are welcome to fork, adapt, and use this code at their own discretion, but should do so with caution and conduct their own validation before use in production environments. Use of this code is entirely at your own risk.

---

## Overview

Modern event-driven architectures rely heavily on Kafka to stream critical business events. However, traditional observability tools often fall short when it comes to tracing **data payload lineage** — particularly **key/value pairs** — across distributed Kafka producers and consumers.

This repository provides **OpenTelemetry Java agent extensions** to enrich existing Kafka instrumentation by **capturing and forwarding payload-level metadata** (i.e. message keys and values) for applications using:

* **Kafka Producer and Consumer clients**
* **Kafka Streams**

This extension is particularly useful in scenarios where you want to:

* Trace **end-to-end data flow across services**
* Understand **event transformations** or **drops** across Kafka topics
* Enable **compliance**, **debugging**, or **security auditing** by inspecting message content

> *Note: This extension does **not** create or manage spans. Span creation, propagation, and lifecycle handling is provided by the [official OpenTelemetry Kafka Instrumentation](https://github.com/open-telemetry/opentelemetry-java-instrumentation).*

---

## Problem It Solves

By default, OpenTelemetry’s Kafka instrumentation provides visibility into *message flow* (e.g. spans for `send` or `poll`), but not into *what* is being sent—**message payloads, keys, and relevant context** often remain opaque.

This project solves that gap by allowing users to:

* Intercept and record Kafka **key/value pairs** on both produce and consume events
* Tie payloads to existing OpenTelemetry spans for downstream ingestion and analysis
* Enable **payload-aware tracing** to enrich metrics, logs, and traces with meaningful business data

---

## Quick Start

To use this extension, you'll need:

1. The official **OpenTelemetry Java agent**
2. This repository's **custom instrumentation extension JAR**

### Sample Launch Command

```bash
java \
-javaagent:/path/to/agent/opentelemetry-javaagent-all.jar \
-Dotel.instrumentation.kafka.experimental-span-attributes=true \
-Dotel.instrumentation.common.experimental.suppress-messaging-receive-spans=true \
-Dotel.javaagent.extensions=/path/to/extension/instrumentation-x.x.x-all.jar \
-jar application.jar
```

### Required JVM Options

```bash
-Dotel.instrumentation.kafka.experimental-span-attributes=true
-Dotel.instrumentation.common.experimental.suppress-messaging-receive-spans=true
```

These ensure that context is **propagated end-to-end** across services without duplicating spans on each consume call.

---

## Compatibility

This extension was developed and tested against:

* **OpenTelemetry Java Instrumentation v1.13.0**

Ensure the extension version aligns with the OpenTelemetry agent version in your environment for reliable behaviour.

---

## Example: Credit Card Tracing Demo

This repo includes a tutorial project illustrating **event lineage tracing** with a simulated credit card transaction pipeline. It's intended to show how payload-level data can be captured and visualised in distributed traces.

Explore the full [event lineage demo](https://github.com/confluentinc/csid-event-lineage-demos) to see:

* Full trace propagation through Kafka Producer → Kafka Streams → Kafka Consumer
* Captured payloads included in trace metadata
* Practical use cases like fraud detection, data loss debugging, or SLA tracking

Presentation slides are available under [`presentations/`](./presentations).

> Looking for a version that includes **Kafka Connect**? Check the [`demo-with-connect`](https://github.com/confluentinc/csid-event-lineage-demos/tree/demo-with-connect) branch.

---

## Summary

| Feature             | Description                                                 |
| ------------------- | ----------------------------------------------------------- |
|  Payload Capture  | Intercepts key/value records in Kafka clients and streams   |
|  Span Extension   | Augments existing spans with business metadata              |
|  Trace Continuity | Preserves end-to-end trace IDs from producer to consumer    |
|  Demo Included    | Practical lineage use case included via credit card tracing |

