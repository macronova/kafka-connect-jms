#!/bin/bash

# Useful commands:
# docker-compose exec broker kafka-console-consumer --bootstrap-server localhost:29092 --topic from-jms-text-queue --from-beginning --max-messages 1
# docker-compose exec broker /bin/bash -c "echo 'Hello, World' | kafka-console-producer --broker-list localhost:29092 --topic to-jms-text-queue"
# docker-compose exec schema-registry /bin/bash -c "echo '{\"f1\": \"value1\", \"f2\": 13}' | kafka-avro-console-producer --broker-list broker:9092 --topic to-jms-map-queue --property value.schema='{\"type\":\"record\",\"name\":\"myRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"},{\"name\":\"f2\",\"type\":\"int\"}]}'"
# For TIBCO EMS management, use GEMS.

# Create JMS destinations on TIBCO EMS.
docker-compose exec ems bin/bash -c "echo -e '
create queue queue.from.kafka.text
create topic topic.from.kafka.text

create queue queue.from.kafka.map
create queue queue.from.kafka.bytes
create queue queue.from.kafka.json
create queue queue.from.kafka.avro

create queue queue.to.kafka.text
create topic topic.to.kafka.text

create queue queue.to.kafka.map
create queue queue.to.kafka.bytes

commit' > /opt/tibco/ems-setup"
docker-compose exec ems /opt/tibco/ems/8.4/bin/tibemsadmin64 -server tcp://localhost:7222 -user admin -password '' -script /opt/tibco/ems-setup
docker-compose exec ems rm -rf /opt/tibco/ems-setup

# Create Apache Kafka topics.
docker-compose exec broker kafka-topics --create --topic to-jms-text-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic from-jms-text-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic to-jms-map-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic to-jms-bytes-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic to-jms-json-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic to-jms-avro-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic from-jms-map-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic from-jms-bytes-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic to-jms-text-topic --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic from-jms-text-topic --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181

# Setup sink connectors.
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-sink-text-queue
curl -X POST -H "Content-Type: application/json" -d @jms-sink-text-queue.json http://localhost:8083/connectors
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-sink-map-queue
curl -X POST -H "Content-Type: application/json" -d @jms-sink-map-queue.json http://localhost:8083/connectors
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-sink-bytes-queue
curl -X POST -H "Content-Type: application/json" -d @jms-sink-bytes-queue.json http://localhost:8083/connectors
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-sink-json-queue
curl -X POST -H "Content-Type: application/json" -d @jms-sink-json-queue.json http://localhost:8083/connectors
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-sink-avro-queue
curl -X POST -H "Content-Type: application/json" -d @jms-sink-avro-queue.json http://localhost:8083/connectors
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-sink-text-topic
curl -X POST -H "Content-Type: application/json" -d @jms-sink-text-topic.json http://localhost:8083/connectors

# Setup source connectors.
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-source-text-queue
curl -X POST -H "Content-Type: application/json" -d @jms-source-text-queue.json http://localhost:8083/connectors
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-source-map-queue
curl -X POST -H "Content-Type: application/json" -d @jms-source-map-queue.json http://localhost:8083/connectors
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-source-bytes-queue
curl -X POST -H "Content-Type: application/json" -d @jms-source-bytes-queue.json http://localhost:8083/connectors
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-source-text-topic
curl -X POST -H "Content-Type: application/json" -d @jms-source-text-topic.json http://localhost:8083/connectors