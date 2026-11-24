# Changelog

## 2.0.0

### Compatibility
- Works with Kafka Connect 4.x (tested with 3.9.2 and 4.4.0). Requires Kafka Connect 2.4 or newer.
- New `jakarta.jms` variant (`kafka-connect-jms-jakarta`) for ActiveMQ 6, Artemis and other Jakarta Messaging
  providers, built from the same sources as the `javax.jms` one.

### Fixes
- **Kafka 4:** JMS messages were never acknowledged, because the single-argument `SourceTask.commitRecord` the
  connector relied on was removed in Kafka 4.0. Acknowledgement now happens in
  `commitRecord(SourceRecord, RecordMetadata)`.
- **Kafka 4:** the connectors and converter are registered for `ServiceLoader` discovery, which is the default
  `plugin.discovery` mode in Kafka 4.
- A message that was being polled could be acknowledged before it had been delivered to Kafka when another record was
  committed at the same time, which could lose it if the worker then failed.
- Two JMS messages that produced equal records were tracked as one, so the second could be acknowledged before it had
  been delivered.
- `stop()` no longer waits forever for acknowledgements that never arrive. New property `shutdown.timeout.ms`
  (default 10000) bounds the wait; unacknowledged messages are redelivered by the JMS server.

### Build
- Gradle 9, tested on Java 8, 17, 21 and 25, with a Kafka version / JDK test matrix (`./gradlew testMatrix`).
- Test dependencies updated to releases without known vulnerabilities; the connector itself still ships only the JMS
  API JAR.
- Apache Avro output format covered by tests.
- GitHub Actions replaces Travis; pushing a `v*` tag publishes a GitHub Release.
