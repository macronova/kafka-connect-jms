#!/bin/bash

# Create JMS destinations on Solace broker.
docker-compose exec solace /bin/bash -c "echo -e '
home
enable
config

message-spool message-vpn default
	! pragma:interpreter:ignore-already-exists
	create queue Q/queueFromKafka
		access-type non-exclusive
		owner default
		no shutdown
		exit
	! pragma:interpreter:ignore-already-exists
	create queue Q/queueToKafka
		access-type non-exclusive
		owner default
		no shutdown
		exit
	exit

jndi message-vpn default
	! pragma:interpreter:ignore-already-exists
	create queue /JNDI/Q/queueFromKafka
		property physical-name Q/queueFromKafka
		exit
	! pragma:interpreter:ignore-already-exists
	create queue /JNDI/Q/queueToKafka
		property physical-name Q/queueToKafka
		exit
	exit

exit
exit
exit
' > /usr/sw/jail/cliscripts/solace-setup"
echo
echo -e "\033[0;31mType and execute: source script solace-setup\033[0m"
echo
docker-compose exec solace /usr/sw/loads/currentload/bin/cli -A
docker-compose exec solace rm -rf /usr/sw/jail/cliscripts/solace-setup

# Create Apache Kafka topics.
docker-compose exec broker kafka-topics --create --topic to-jms-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec broker kafka-topics --create --topic from-jms-queue --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181

# Setup sink connectors.
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-sink-queue
curl -X POST -H "Content-Type: application/json" -d @jms-sink-queue.json http://localhost:8083/connectors

# Setup source connectors.
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/jms-source-queue
curl -X POST -H "Content-Type: application/json" -d @jms-source-queue.json http://localhost:8083/connectors