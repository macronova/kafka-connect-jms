/*
 * Copyright 2018 Macronova.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.macronova.kafka.connect.jms.source;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import io.macronova.kafka.connect.jms.util.StringUtils;
import org.apache.activemq.ActiveMQConnectionFactory;
import io.macronova.kafka.connect.jms.BaseFunctionalTest;
import io.macronova.kafka.connect.jms.JmsSourceConnector;
import org.apache.kafka.connect.source.SourceRecord;

public abstract class BaseSourceTest extends BaseFunctionalTest {
	/**
	 * Simulates execution of Kafka Connect source connector.
	 *
	 * @param configuration Connector configuration.
	 * @param taskCount Number of tasks to trigger.
	 * @return Generated source records.
	 * @throws Exception Indicates failure.
	 */
	protected List<SourceRecord> runSource(Map<String, String> configuration, int taskCount) throws Exception {
		final JmsSourceConnector connector = new JmsSourceConnector();
		connector.start( configuration );
		final List<Map<String, String>> taskConfigs = connector.taskConfigs( taskCount );
		final List<SourceRecord> result = new LinkedList<>();
		for ( Map<String, String> config : taskConfigs ) {
			final JmsSourceTask task = (JmsSourceTask) connector.taskClass().getConstructor().newInstance();
			task.start( config );
			List<SourceRecord> data = null;
			// Try to poll data until there is nothing more.
			while ( (data = task.poll()) != null && ! data.isEmpty() ) {
				result.addAll( data );
				Thread.sleep( 100 );
			}
			// Commit received records.
			for ( SourceRecord record : result ) {
				task.commitRecord( record );
			}
			task.stop();
		}
		connector.stop();
		return result;
	}

	/**
	 * While testing source connector to receive messages from JMS topic, we do the following:
	 * <ul>
	 *    <li>Create durable subscriber on particular JMS topic.</li>
	 *    <li>Send test messages.</li>
	 *    <li>Run Source tasks. Thanks to durable subscriber registered before, messages do not vanish.</li>
	 *
	 * @param selector JMS message selector.
	 * @throws Exception Indicates failure.
	 */
	protected void registerDurableSubscription(String selector) throws Exception {
		final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory( providerUrl() );
		final Connection connection = connectionFactory.createConnection();
		connection.setClientID( clientId() );
		connection.start();
		final Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
		final Topic destination = session.createTopic( jmsTopic() );
		if ( StringUtils.isEmpty( selector ) ) {
			session.createDurableSubscriber( destination, subscriptionName() );
		}
		else {
			session.createDurableSubscriber( destination, subscriptionName(), selector, false );
		}
		session.close();
		connection.close();
	}

	/**
	 * Send JMS text messages.
	 *
	 * @param msgs Payload of messages.
	 * @param includeSequenceProperty If {@code true}, JMS property named {@code MySequence} is
	 *                                added, which corresponds to message sequence number.
	 * @throws Exception Indicates failure.
	 */
	protected void sendMessages(String[] msgs, boolean includeSequenceProperty) throws Exception {
		final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory( providerUrl() );
		final Connection connection = connectionFactory.createConnection();
		connection.start();
		final Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
		Destination destination = null;
		if ( isQueueTest() ) {
			destination = session.createQueue( jmsQueue() );
		}
		else {
			destination = session.createTopic( jmsTopic() );
		}
		final MessageProducer producer = session.createProducer( destination );

		int count = 0;
		for ( String msg : msgs ) {
			final Message message = session.createTextMessage( msg );
			if ( includeSequenceProperty ) {
				message.setIntProperty( "MySequence", count++ );
			}
			producer.send( message );
		}

		producer.close();
		session.close();
		connection.close();
	}

	protected Map<String, String> configurationJndi() {
		final Map<String, String> properties = super.configurationJndi();
		properties.put( "max.retries", "0" );
		properties.put( "max.poll.records", "2" );
		properties.put( "poll.timeout.ms", "10" );
		if ( ! isQueueTest() ) {
			properties.put( "jms.client.id", clientId() );
			properties.put( "jms.topic.subscription.name", subscriptionName() );
			// TODO: ActiveMQ does not support JMS 2.0, so we are unable to unit-test.
			properties.put( "jms.topic.subscription.shared", "false" );
		}
		return properties;
	}

	protected Map<String, String> configurationDirect() {
		final Map<String, String> properties = super.configurationDirect();
		properties.put( "max.retries", "0" );
		properties.put( "max.poll.records", "2" );
		properties.put( "poll.timeout.ms", "10" );
		if ( ! isQueueTest() ) {
			properties.put( "jms.client.id", clientId() );
			properties.put( "jms.topic.subscription.name", subscriptionName() );
			properties.put( "jms.topic.subscription.shared", "false" );
		}
		return properties;
	}

	protected String clientId() {
		return "my-client";
	}

	protected String subscriptionName() {
		return "my-subscription";
	}
}
