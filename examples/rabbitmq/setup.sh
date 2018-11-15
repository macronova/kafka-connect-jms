#!/bin/bash

# Create Apache Kafka topics.
docker-compose exec broker kafka-topics --create --topic to-jms-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic from-jms-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181

# Setup sink connectors.
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-sink-queue
curl -X POST -H "Content-Type: application/json" -d @jms-sink-queue.json http://localhost:8083/connectors

# Setup source connectors.
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-source-queue
curl -X POST -H "Content-Type: application/json" -d @jms-source-queue.json http://localhost:8083/connectors