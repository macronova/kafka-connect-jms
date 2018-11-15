# Kafka Connect JMS

[![Build Status](https://travis-ci.org/macronova/kafka-connect-jms.svg)](https://travis-ci.org/macronova/kafka-connect-jms) [![Code Coverage](https://codecov.io/gh/macronova/kafka-connect-jms/branch/master/graph/badge.svg)](https://codecov.io/gh/macronova/kafka-connect-jms) [![Maven Central](https://img.shields.io/maven-central/v/io.macronova.kafka/kafka-connect-jms/1.svg)](http://central.maven.org/maven2/io/macronova/kafka/kafka-conect-jms/1.0.0)

Apache Kafka JMS Connector provides sink and source capabilities to transfer messages between JMS server and Kafka brokers.

## Table of Contents

- [Installation](#installation)
- [JMS Interaction](#jms-interaction)
- [Configuration](#configuration)
  * [Connection via JNDI](#connection-via-jndi)
  * [Direct Connection](#direct-connection)
  * [Pushing and Polling](#pushing-and-polling)
  * [Sink Message Conversion](#sink-message-conversion)
  * [Source Message Conversion](#source-message-conversion)
  * [Retry on Error](#retry-on-error)
- [Extensions](#extensions)
  * [Custom JMS Converter](#custom-jms-converter)
  * [Custom JMS Dialect](#custom-jms-dialect)
- [Examples](#examples)

## Installation

1. Download latest release ZIP archive from GitHub and extract its content to temporary folder.
2. Copy _kafka-connect-jms-${version}.jar_ with all third-party dependencies to Connect `plugin.path` directory.
    1. Version 1.0.0 depends only on JMS 2.0 API JAR.
3. Copy JMS client (including dependencies) of given JMS server to Connect `plugin.path` directory.
3. Configure source and sink connectors according to below documentation.

## JMS Interaction

Kafka sink connectors are supposed to push batch of messages to the target system. To minimize number of duplicates in case of transient failures, JMS connector leverages transactional JMS session while publishing messages. On the other hand, to prevent message loss while receiving it from JMS server, connector takes advantage of client acknowledgement feature. JMS message is confirmed as processed only after successful write to Kafka log.

## Configuration

Hereby section describes how to configure sink and source JMS connectors. Key topics include connection setup (JNDI vs. direct) and message type conversion.

### Connection via JNDI

Below table describes configuration parameters responsible for JNDI connectivity.

| Property Name                    | Mandatory | Description                                            | Example                                                  |
|----------------------------------|-----------|--------------------------------------------------------|----------------------------------------------------------|
| java.naming.factory.initial      | Yes       | Initial context factory class name.                    | `org.apache.activemq.jndi.ActiveMQInitialContextFactory` |
| java.naming.provider.url         | Yes       | Provider URL.                                          | `tcp://localhost:61616`                                  |
| java.naming.security.principal   | No        | JNDI server username.                                  | `myuser`                                                 |
| java.naming.security.credentials | No        | JNDI server password.                                  | `mypassword`                                             |
| jndi.extra.params                | No        | Comma-separated list of additional JNDI properties.    | `MyKey1=Value1,MyKey2=Value2`                            |
| jndi.connection.factory          | Yes       | Connection factory name.                               | `ConnectionFactory`                                      |
| jms.username                     | No        | JMS server username.                                   | `myuser`                                                 |
| jms.password                     | No        | JMS server password.                                   | `mypassword`                                             |
| jms.client.id                    | No        | JMS client ID. Important for topic subscriptions.      | `myclinetid`                                             |

### Direct Connection

JMS specification does not unify construction of connection factory class. Hereby Connector assumes that JMS clients provide constructor accepting single text parameter representing broker URL, e.g. `#ConnectionFactory(String)`. Otherwise, feel free to implement [custom JMS dialect](#custom-jms-dialect) and link it via `jms.dialect.class` property. Users are always advised to access administrative objects of JMS server via JNDI.

| Property Name          | Mandatory | Description                                            | Example                                         |
|------------------------|-----------|--------------------------------------------------------|-------------------------------------------------|
| jms.connection.factory | Yes       | Connection factory class name.                         | `com.tibco.tibjms.TibjmsQueueConnectionFactory` |
| jms.url                | Yes       | Provider URL                                           | `tcp://localhost:7222`                          |
| jms.username           | No        | JMS server username.                                   | `myuser`                                        |
| jms.password           | No        | JMS server password.                                   | `mypassword`                                    |
| jms.client.id          | No        | JMS client ID. Important for topic subscriptions.      | `myclinetid`                                    |
| jms.dialect.class      | No        | Overrides default JMS dialect implementation.          |                                                 |

### Pushing and Polling

Target JMS destination should be specified using below configuration parameters.

| Property Name        | Description                                | Default |
|----------------------|--------------------------------------------|---------|
| jms.destination.name | JMS destination name.                      |         |
| jms.destination.type | JMS destination type (`queue` or `topic`). | `queue` |

Three parameters control JMS queue and topic polling behavior.

| Property Name    | Description                                                           | Default |
|------------------|-----------------------------------------------------------------------|---------|
| poll.timeout.ms  | Maximum poll timeout for incoming JMS message.                        | `1000`  |
| max.poll.records | Maximum number of JMS messages retrieved during every poll operation. | `100`   |
| jms.selector     | JMS message selector.                                                 |         |

While receiving messages from JMS topic, connector registers durable subscriber. Durable subscribers are identified by a combination of JMS client ID, subscription name and message selector. Therefore, the following properties should be always configured:
* `jms.client.id`
* `jms.topic.subscription.name` - Name of durable subscription.
* `jms.topic.subscription.shared` - Boolean flag specifying whether shared durable should be used (JMS 2.0 certified server required). Defaults to `true` and is not advised to change unless `tasks.max = 1` (generates duplicates otherwise).
* `jms.topic.subscription.durable` - Decides whether durable topic subscriber should be registered. Defaults to `true`. Altering the value implies message loss during outage of Connect engine.
* `jms.selector` - Optional. Set if required by business logic.

### Sink Message Conversion

While moving messages from Kafka to JMS, users may choose from five predefined converters. Outbound message type is controlled by `jms.message.converter.output.format` setting. Available options include: `text` (default), `json`, `avro`, `map` and `bytes`. Mentioned conversion affects only value of Connect record, key is always translated into `KafkaKey` JMS property.

![Sink Message Conversion](docs/sink-conversion.png)

Every message generated by sink connector contains the following JMS application headers.

| Property Name  | Type    | Description                                                                                                                      |
|----------------|---------|----------------------------------------------------------------------------------------------------------------------------------|
| KafkaKey       | *       | Connect record key. Only simple types are supported: `byte`, `short`, `integer`, `long`, `float`, `double`, `boolean`, `string`. |
| KafkaTopic     | String  | Kafka topic name.                                                                                                                |
| KafkaPartition | Integer | Kafka partition.                                                                                                                 |
| KafkaOffset    | Long    | Offset position within Kafka partition.                                                                                          |
| KafkaTimestamp | Long    | Kafka timestamp.                                                                                                                 |

Body of JMS message can be formed in five different ways depending on `jms.message.converter.output.format` setting:
* `text` - generates JMS text message. Conversion assumes that value of Connect record is of text type. Please configure `value.converter = org.apache.kafka.connect.storage.StringConverter`.
* `json` - generates JMS text message and converts any Connect value to JSON format (`org.apache.kafka.connect.json.JsonConverter` used internally). Setting `schemas.enable = false` disables schema output.
* `avro` - generates JMS bytes message and converts any Connect value to Apache Avro format (`io.confluent.connect.avro.AvroConverter` used internally). Requires deployment of [Confluent Schema Registry](https://www.confluent.io/confluent-schema-registry) and specification of `schema.registry.url` property.
* `map` - generates JMS map message. Atomic Connect value is stored under `payload` key. In case of structure, map key corresponds to field name.
* `bytes` - generates JMS bytes message. Conversion assumes that value of Connect record is byte array. Please configure `value.converter = ByteArrayConverter`.

### Source Message Conversion

Messages received from JMS server are translated into Kafka Connect schema as described below. Default converter does not currently support JMS object and stream messages.

#### JMS Message Schema

| Field Name    | Type                        | Optional | Description                                                  |
|---------------|-----------------------------|----------|--------------------------------------------------------------|
| type          | String                      | No       | `text`, `map` or `bytes` depending on received message type. |
| messageId     | String                      | No       | JMS message ID.                                              |
| correlationId | String                      | Yes      | JMS message correlation ID.                                  |
| destination   | JMS Destination             | No       | JMS source destination.                                      |
| replyTo       | JMS Destination             | Yes      | JMS destination where application should post the reply.     |
| priority      | Int32                       | No       | JMS message priority.                                        |
| expiration    | Int64                       | No       | JMS message expiration timestamp.                            |
| timestamp     | Int64                       | No       | JMS message submission timestamp.                            |
| redelivered   | Boolean                     | No       | Flag indicating whether message has been redelivered.        |
| properties    | Map<String, Property Value> | Yes      | JMS message application properties.                          |
| payloadText   | String                      | Yes      | Payload of text message, empty for other types.              |
| payloadMap    | Map<String, Property Value> | Yes      | Payload of map message, empty for other types.               |
| payloadBytes  | Bytes                       | Yes      | Payload of bytes message, empty for other types.             |

#### JMS Destination Schema

| Field Name | Type   | Optional | Description                                |
|------------|--------|----------|--------------------------------------------|
| type       | String | No       | JMS destination type (`queue` or `topic`). |
| name       | String | No       | JMS destination name.                      |

#### Property Value Schema

| Field Name | Type    | Optional | Description                                                                                             |
|------------|---------|----------|---------------------------------------------------------------------------------------------------------|
| type       | String  | No       | Type of property value (`boolean`, `byte`, `short`, `integer`, `long`, `float`, `double`, or `string`). |
| boolean    | Boolean | Yes      | Boolean value, empty otherwise.                                                                         |
| byte       | Byte    | Yes      | Byte value, empty otherwise.                                                                            |
| short      | Short   | Yes      | Short value, empty otherwise.                                                                           |
| integer    | Int32   | Yes      | Integer value, empty otherwise.                                                                         |
| long       | Int64   | Yes      | Long value, empty otherwise.                                                                            |
| float      | Float32 | Yes      | Float value, empty otherwise.                                                                           |
| double     | Float64 | Yes      | Double value, empty otherwise.                                                                          |
| string     | String  | Yes      | String value, empty otherwise.                                                                          |

### Retry on Error

JMS specification does not distinguish between retriable and fatal errors. Various JMS implementations may throw different exceptions in case of connectivity issues. Sink and source connectors provide the ability to retry on error. The main motivation to implement hereby feature was to support transient unavailability of JMS server and ease job of system operators with restarting failed Connect tasks.

| Property Name    | Description                                                                    | Default |
|------------------|--------------------------------------------------------------------------------|---------|
| max.retries      | Maximum number of retry attempts in case of error before failing the task.     | `10`    |
| retry.backoff.ms | The time in milliseconds to wait following an error before next retry attempt. | `5000`  |

Sink connector reestablishes connectivity with JMS server in case of issues with sending messages. Source connector tries to reconnect upon errors encountered while attempting to poll new records. Exceptions that require re-establishing server connectivity should be reported to `javax.jms.ExceptionListener` by JMS provider. To alter described behavior, implement [custom JMS dialect](#custom-jms-dialect).

Users may disable retry logic by setting `max.retries = 0`.

## Extensions

Connector provides two SPI interfaces that can be reimplemented by users to override default behavior and better support specific JMS server.

### Custom JMS Converter

If none of default converters described in sections [Sink Message Conversion](#sink-message-conversion) and [Source Message Conversion](#source-message-conversion) works for your use-case, developers may define custom `JmsConverter` and link it via `jms.message.converter` property. Implementations should provide default non-argument constructor. We encourage developers to submit pull-requests and bug reports so that default implementation (`StandardJmsConverter`) becomes even more generic and feature-rich.

```java
public interface JmsConverter {
    /**
     * Configure JMS message converter.
     *
     * @param properties Configuration properties.
     */
    void configure(Map<String, String> properties);

    /**
     * Convert sink record to JMS message.
     *
     * @param session Active JMS session.
     * @param record Connect sink record.
     * @return JMS message
     * @throws JMSException Report error.
     */
    Message recordToMessage(Session session, SinkRecord record) throws JMSException;

    /**
     * Convert JMS message to source record.
     *
     * @param message JMS message.
     * @param topic Target Kafka topic.
     * @param sourcePartition Target partition.
     * @param sourceOffset Target offset.
     * @return Connect source record.
     * @throws JMSException Report error.
     */
    SourceRecord messageToRecord(Message message, String topic,
        Map<String, ?> sourcePartition, Map<String, ?> sourceOffset) throws JMSException;
}
```

### Custom JMS Dialect

Various JMS servers, that do not provide JNDI service, may require instantiation of connection factory object in different ways. Two possible solutions:
* Leverage filesystem JNDI service. Typical workaround to access IBM MQ, see _examples/ibmmq_ folder.
* Implement custom JMS dialect and link it via `jms.dialect.class` property. JMS dialect enables users to create `javax.jms.ConnectionFactory` object themselves. Review _examples/rabbitmq_ folder.

```java
/**
 * Dialect controls behavior specific to given JMS server.
 */
public interface JmsDialect {
    /**
     * Instantiate JMS connection factory.
     *
     * @param config Connector configuration. Only connectivity-related parameters available. User may register
     * custom properties whit {@code jms.dialect.} prefix.
     * @return JMS connection factory.
     * @throws Exception Indicates error.
     */
    ConnectionFactory createConnectionFactory(Map<String, String> config) throws Exception;

    /**
     * @param e Encountered exception.
     * @return {@code true} if connector should re-establish connectivity upon given error,
     *         {@code false} otherwise.
     */
    boolean reconnectOnError(Exception e);
}
```

JMS dialect allows to trigger JMS reconnection procedure upon given exception encountered while sending or receiving messages.

## Examples

Please review _examples_ folder which contains Docker Compose manifests demonstrating integration with IBM MQ, Apache ActiveMQ, TIBCO EMS, Solace PubSub and RabbitMQ. Various message type conversions and topic subscriptions have been tested against TIBCO EMS, which is certified JMS 2.0 implementation. Connectivity with RabbitMQ has been accomplished by custom JMS dialect.

> **Note**: Due to license restrictions, project does not contain JMS client JARs. README documents present in _examples/${provider}/client_ folder indicate what binaries need to be copied.

> **Note**: Docker image of TIBCO EMS is not available in public Docker Hub.

### Running Docker Compose Samples

Steps to quickly run Docker Compose setup from _examples_ directory:
1. Build project from source code.
    ```bash
    $ gradle clean build zip
    ```
2. Create Docker image.
    ```bash
    $ make
    ```
3. Use Docker Compose to start all components. Replace _${provider}_ variable with chosen JMS server.
    ```bash
    $ docker-compose -f ./examples/${provider}/docker-compose.yml up
    ```
4. Execute setup script to create JMS destinations and configure sample sink and source tasks.
    ```bash
    $ ./examples/${provider}/setup.sh
    ```
5. When done, remove environment.
    ```bash
    $ docker-compose -f ./examples/${provider}/docker-compose.yml down --volumes
    ```

### Sink to JMS Queue (Text Message)

```json
{
    "name": "jms-sink",
    "config": {
        "connector.class": "io.macronova.kafka.connect.jms.JmsSinkConnector",
        "tasks.max": "2",
        "topics": "kafkaTopic1",
        "java.naming.factory.initial": "com.sun.jndi.fscontext.RefFSContextFactory",
        "java.naming.provider.url": "file:///tmp/config",
        "jndi.connection.factory": "ConnectionFactory",
        "jms.username": "app",
        "jms.password": "passw0rd",
        "jms.destination.name": "jmsQueue1",
        "jms.destination.type": "queue",
        "jms.message.converter": "io.macronova.kafka.connect.jms.common.StandardJmsConverter",
        "jms.message.converter.output.format": "text",
        "max.retries": 100,
        "retry.backoff.ms": 60000,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.storage.StringConverter"
    }
}
```

### Sink to JMS Topic (JSON Message)

```json
{
    "name": "jms-sink",
    "config": {
        "connector.class": "io.macronova.kafka.connect.jms.JmsSinkConnector",
        "tasks.max": "1",
        "topics": "kafkaTopic1",
        "jms.connection.factory": "com.tibco.tibjms.TibjmsQueueConnectionFactory",
        "jms.url": "tcp://localhost:7222,tcp://localhost:7222",
        "jms.username": "admin",
        "jms.password": "",
        "jms.destination.name": "jmsTopic1",
        "jms.destination.type": "topic",
        "jms.message.converter.output.format": "json",
        "schemas.enable": "false",
        "max.retries": 100,
        "retry.backoff.ms": 60000,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081"
    }
}
```

### Sink to JMS Queue (Apache Avro Message)

```json
{
    "name": "jms-sink",
    "config": {
        "connector.class": "io.macronova.kafka.connect.jms.JmsSinkConnector",
        "tasks.max": "1",
        "topics": "kafkaTopic1",
        "java.naming.factory.initial": "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
        "java.naming.provider.url": "tcp://localhost:61616",
        "java.naming.security.principal": "myuser",
        "java.naming.security.credentials": "mypassword",
        "jndi.connection.factory": "ConnectionFactory",
        "jms.username": "myuser",
        "jms.password": "mypassword",
        "jms.destination.name": "jmsQueue1",
        "jms.message.converter.output.format": "avro",
        "schema.registry.url": "http://schema-registry:8081",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081"
    }
}
```

### Source from JMS Queue

```json
{
    "name": "jms-source",
    "config": {
        "connector.class": "io.macronova.kafka.connect.jms.JmsSourceConnector",
        "tasks.max": "2",
        "topics": "kafkaTopic1",
        "java.naming.factory.initial": "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
        "java.naming.provider.url": "tcp://localhost:61616",
        "java.naming.security.principal": "myuser",
        "java.naming.security.credentials": "mypassword",
        "jndi.connection.factory": "ConnectionFactory",
        "jms.username": "myuser",
        "jms.password": "mypassword",
        "jms.destination.name": "jmsQueue1",
        "jms.destination.type": "queue",
        "jms.message.converter": "io.macronova.kafka.connect.jms.common.StandardJmsConverter",
        "max.poll.records": 100,
        "poll.timeout.ms": 1000,
        "max.retries": 100,
        "retry.backoff.ms": 60000,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}
```

### Source from JMS Topic (Shared Durable)

Requires JMS 2.0 compliant server.

```json
{
    "name": "jms-source",
    "config": {
        "connector.class": "io.macronova.kafka.connect.jms.JmsSourceConnector",
        "tasks.max": "2",
        "topics": "kafkaTopic1",
        "jms.connection.factory": "com.tibco.tibjms.TibjmsQueueConnectionFactory",
        "jms.url": "tcp://localhost:7222,tcp://localhost:7222",
        "jms.username": "admin",
        "jms.password": "",
        "jms.destination.name": "jmsTopic1",
        "jms.destination.type": "topic",
        "jms.topic.subscription.name": "my-shared-subscription",
        "jms.topic.subscription.durable": "true",
        "jms.topic.subscription.shared": "true",
        "jms.message.converter": "io.macronova.kafka.connect.jms.common.StandardJmsConverter",
        "max.poll.records": 100,
        "poll.timeout.ms": 1000,
        "max.retries": 100,
        "retry.backoff.ms": 60000,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}
```

### Source from JMS Topic (Durable)

Compatible with JMS 1.x specification.

```json
{
    "name": "jms-source",
    "config": {
        "connector.class": "io.macronova.kafka.connect.jms.JmsSourceConnector",
        "tasks.max": "1",
        "topics": "kafkaTopic1",
        "jms.connection.factory": "com.tibco.tibjms.TibjmsQueueConnectionFactory",
        "jms.url": "tcp://localhost:7222,tcp://localhost:7222",
        "jms.username": "admin",
        "jms.password": "",
        "jms.destination.name": "jmsTopic1",
        "jms.destination.type": "topic",
        "jms.topic.subscription.name": "my-shared-subscription",
        "jms.topic.subscription.durable": "true",
        "jms.topic.subscription.shared": "false",
        "max.poll.records": 100,
        "poll.timeout.ms": 1000,
        "max.retries": 100,
        "retry.backoff.ms": 60000,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}
```
