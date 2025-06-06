# OpenTelemetry Instrumentation Extensions for Kafka Clients and Kafka Streams

⚠️ **Disclaimer**  

This repository is not actively maintained and is intended solely as an internal proof of concept. It is provided *as-is*, without any guarantees of support, updates, or compatibility with future versions of OpenTelemetry or related tooling. 

This is not an official project and does not represent a supported product or endorsed integration. Users are welcome to fork, adapt, and use this code at their own discretion, but should do so with caution and conduct their own validation before use in production environments. Use of this code is entirely at your own risk.

## Introduction

This repository contains **OpenTelemetry Instrumentation extensions** for Kafka Clients and Kafka Streams. The purpose of these extensions is to **capture key/value payloads** as they are consumed and produced by applications.

> Note: Actual `Span` creation and propagation is handled by the official OpenTelemetry Kafka Instrumentation and Agent.

- 📚 [OpenTelemetry Documentation](https://opentelemetry.io/)
- 💻 [OpenTelemetry GitHub](https://github.com/open-telemetry)

## Installation

To enable the extension, the application should be run with both the **OpenTelemetry Java agent** and the **custom extension JAR**:

```
java \
-javaagent:/path/to/agent/opentelemetry-javaagent-all.jar \
-Dotel.instrumentation.kafka.experimental-span-attributes=true \
-Dotel.instrumentation.common.experimental.suppress-messaging-receive-spans=true \
-Dotel.javaagent.extensions=/path/to/extension/instrumentation-x.x.x-all.jar \
-jar application.jar
```

### Key VM Options

These options ensure **trace context is propagated** across produce and consume events:

```bash
-Dotel.instrumentation.kafka.experimental-span-attributes=true
-Dotel.instrumentation.common.experimental.suppress-messaging-receive-spans=true
```

This setup avoids starting a new trace on each consume and instead continues a single trace across the full lifecycle, maintaining linkage between producer and consumer spans.

## Compatible OpenTelemetry Versions

This extension is built and tested against:

* **OpenTelemetry Java Instrumentation v1.13.0**

Make sure both your extension and main OpenTelemetry agent match compatible versions for proper operation.

## Tutorial Repo: Credit Card Tracing Demo

This repository also includes a **simple credit card tracing example**, demonstrating a practical use case aligned with the proposed **Event Lineage reference architecture**.

To learn more, visit the full [Event Lineage demo repository](https://github.com/confluentinc/csid-event-lineage-demos).

> Presentation slides for this demo are available in the `presentations/` directory.

### Kafka Connect Note

This version of the demo **does not use Kafka Connect** in the data flow. For a version that **includes Kafka Connect**, please refer to the [`demo-with-connect`](https://github.com/confluentinc/csid-event-lineage-demos/tree/demo-with-connect) branch.

