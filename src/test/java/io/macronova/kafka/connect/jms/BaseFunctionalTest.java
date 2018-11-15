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
package io.macronova.kafka.connect.jms;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;

import io.macronova.kafka.connect.jms.common.StandardJmsConverter;
import org.apache.activemq.broker.BrokerService;

/**
 * Base functional test class allowing to interact with embedded Apache ActiveMQ server.
 */
public abstract class BaseFunctionalTest {
	protected BrokerService broker = null;

	@Before
	public void setUp() throws Exception {
		startBroker();
	}

	@After
	public void tearDown() {
		stopBroker();
	}

	protected void startBroker() throws Exception {
		System.setProperty( "org.apache.activemq.SERIALIZABLE_PACKAGES", "*" );
		broker = new BrokerService();
		broker.addConnector( providerUrl() );
		broker.setPersistent( false );
		broker.start();
		TestUtils.waitForCondition( new TestCondition() {
			@Override
			public boolean conditionMet() {
				return broker.isStarted();
			}
		}, 5000, "Broker start timeout." );
	}

	protected void stopBroker() {
		try {
			if ( broker != null ) {
				broker.stop();
				TestUtils.waitForCondition( new TestCondition() {
					@Override
					public boolean conditionMet() {
						return broker.isStopped();
					}
				}, 5000, "Broker stop timeout." );
			}
		}
		catch (Exception e) {
			// Ignore.
		}
		broker = null;
	}

	/**
	 * @return Connector configuration to access JMS server via JNDI.
	 */
	protected Map<String, String> configurationJndi() {
		final Map<String, String> properties = new HashMap<>();
		properties.put( "topics", kafkaTopic() );
		properties.put( "java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory" );
		properties.put( "java.naming.provider.url", providerUrl() );
		properties.put( "java.naming.security.principal", "" );
		properties.put( "java.naming.security.credentials", "" );
		properties.put( "jndi.connection.factory", "ConnectionFactory" );
		properties.put( "jms.username", "" );
		properties.put( "jms.password", "" );
		if ( isQueueTest() ) {
			properties.put( "jms.destination.name", "dynamicQueues/" + jmsQueue() );
			properties.put( "jms.destination.type", "queue" );
		}
		else {
			properties.put( "jms.destination.name", "dynamicTopics/" + jmsTopic() );
			properties.put( "jms.destination.type", "topic" );
		}
		// Default should be taken.
		// properties.put( "jms.message.converter", StandardJmsConverter.class.getName() );
		// properties.put( "jms.message.converter.output.format", "text" );
		return properties;
	}

	/**
	 * @return Connector configuration to access JMS server by instantiating connection factory class.
	 */
	protected Map<String, String> configurationDirect() {
		final Map<String, String> properties = new HashMap<>();
		properties.put( "topics", kafkaTopic() );
		properties.put( "jms.connection.factory", "org.apache.activemq.ActiveMQConnectionFactory" );
		properties.put( "jms.url", providerUrl() );
		properties.put( "jms.username", "" );
		properties.put( "jms.password", "" );
		if ( isQueueTest() ) {
			properties.put( "jms.destination.name", jmsQueue() );
			// Default should be taken.
			// properties.put( "jms.destination.type", "queue" );
		}
		else {
			properties.put( "jms.destination.name", jmsTopic() );
			properties.put( "jms.destination.type", "topic" );
		}
		properties.put( "jms.message.converter", StandardJmsConverter.class.getName() );
		properties.put( "jms.message.converter.output.format", "text" );
		return properties;
	}

	protected String kafkaTopic() {
		return "topic1";
	}

	protected String jmsQueue() {
		return "queue1";
	}

	protected String jmsTopic() {
		return "topic1";
	}

	protected String providerUrl() {
		return "tcp://localhost:61616";
	}

	/**
	 * @return {@code true} if purpose of the test is to send and receive messages from JMS queue.
	 *         {@code false} in case of topics.
	 */
	protected abstract boolean isQueueTest();
}
