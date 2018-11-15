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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.macronova.kafka.connect.jms.common.BaseConnectorConfig;
import io.macronova.kafka.connect.jms.common.JmsConnectionPool;
import io.macronova.kafka.connect.jms.common.JmsConverter;
import io.macronova.kafka.connect.jms.common.JmsSessionProvider;
import io.macronova.kafka.connect.jms.util.JmsUtils;
import io.macronova.kafka.connect.jms.util.StringUtils;
import io.macronova.kafka.connect.jms.util.Version;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/**
 * JMS source task.
 */
public class JmsSourceTask extends SourceTask {
	private static final Logger log = LoggerFactory.getLogger( JmsSourceTask.class );

	private static final Map<String, ?> sourcePartition = new HashMap<>();
	private static final Map<String, ?> sourceOffset = new HashMap<>();

	private Map<String, String> configProperties = null;
	private JmsSourceConnectorConfig config = null;
	private int maxPollRecords = -1;
	private long pollingTimeout = -1;
	private String topic = null;
	private JmsConverter converter = null;

	private JmsSessionProvider provider = null;
	private Session session = null;
	private Destination destination = null;
	private MessageConsumer consumer = null;

	private Map<SourceRecord, Message> messagesToAcknowledge = new ConcurrentHashMap<>();
	private Message lastMessage = null;
	private volatile boolean shuttingDown = false;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		configProperties = properties;
		config = new JmsSourceConnectorConfig( properties );
		topic = config.getString( JmsSourceConnectorConfig.KAFKA_TOPIC_CONFIG );
		maxPollRecords = config.getInt( JmsSourceConnectorConfig.MAX_POLL_CONFIG );
		pollingTimeout = config.getLong( JmsSourceConnectorConfig.POLL_TIMEOUT_CONFIG );
		try {
			converter = (JmsConverter) Class.forName( config.getString( JmsSourceConnectorConfig.CONVERTER_CONFIG ) ).newInstance();
			converter.configure( configProperties );
			provider = JmsConnectionPool.getOrCreateConnection( configProperties );
			createConsumer();
			shuttingDown = false;
		}
		catch (Exception e) {
			terminate();
			throw new ConnectException(
					"Failed to start JMS source task: " + e.getMessage() + ".", e
			);
		}
	}

	private void createConsumer() throws Exception {
		session = provider.createSession( false );
		destination = provider.resolveDestination(
				session, config.getString( JmsSourceConnectorConfig.JMS_DESTINATION_NAME_CONFIG ),
				config.getString( JmsSourceConnectorConfig.JMS_DESTINATION_TYPE_CONFIG )
		);
		String selector = config.getString( JmsSourceConnectorConfig.JMS_SELECTOR_CONFIG );
		selector = StringUtils.isEmpty( selector ) ? null : selector;
		if ( destination instanceof Queue ) {
			consumer = session.createConsumer( destination, selector );
		}
		else {
			final String subscription = config.getString( JmsSourceConnectorConfig.JMS_SUBSCRIPTION_CONFIG );
			final Boolean durableSubscription = config.getBoolean( JmsSourceConnectorConfig.JMS_SUBSCRIPTION_DURABLE_CONFIG );
			final Boolean sharedSubscription = config.getBoolean( JmsSourceConnectorConfig.JMS_SUBSCRIPTION_SHARED_CONFIG );
			if ( ! durableSubscription ) {
				consumer = session.createConsumer( destination, selector );
			}
			else if ( StringUtils.isEmpty( subscription ) ) {
				throw new ConnectException(
						"When polling messages from JMS topic, please specify '"
								+ JmsSourceConnectorConfig.JMS_SUBSCRIPTION_CONFIG + "' property."
				);
			}
			else if ( ! sharedSubscription ) {
				consumer = session.createDurableSubscriber( (Topic) destination, subscription, selector, false );
			}
			else {
				consumer = session.createSharedDurableConsumer( (Topic) destination, subscription, selector );
			}
		}
	}

	private void terminateConsumer() {
		if ( consumer != null ) {
			try {
				consumer.close();
			}
			catch (Exception e) {
				// Ignore.
			}
			consumer = null;
		}
		if ( session != null ) {
			try {
				session.close();
			}
			catch (Exception e) {
				// Ignore.
			}
			session = null;
		}
	}

	private void terminate() {
		terminateConsumer();
		if ( provider != null ) {
			JmsConnectionPool.destroyConnection( configProperties );
			provider = null;
		}
	}

	@Override
	public synchronized List<SourceRecord> poll() {
		if ( shuttingDown ) {
			// Different thread requested connector to shut down. Do not try to poll messages any more.
			return null;
		}
		final List<SourceRecord> result = new ArrayList<>( maxPollRecords );
		final Map<SourceRecord, Message> receivedMessages = new HashMap<>();
		int count = 0;
		int remainingRetries = config.getInt( JmsSourceConnectorConfig.MAX_RETRIES_CONFIG );
		Exception lastException = null;
		Message latestMessage = null;
		while ( count < maxPollRecords && remainingRetries >= 0 ) {
			try {
				if ( provider.isClosed() || session == null || consumer == null ) {
					tryToReconnect();
				}
				while ( count < maxPollRecords ) {
					final Message message = consumer.receive( pollingTimeout / maxPollRecords );
					if ( message != null ) {
						final SourceRecord record = converter.messageToRecord(
								message, topic, sourcePartition, sourceOffset
						);
						result.add( record );
						receivedMessages.put( record, message );
						latestMessage = message;
					}
					++count;
					remainingRetries = config.getInt( JmsSourceConnectorConfig.MAX_RETRIES_CONFIG );
					lastException = null;
				}
			}
			catch (Exception e) {
				log.error(
						"Failed to poll messages from destination '" + JmsUtils.destinationNameForLog( destination )
						+ "': " + e.getMessage() + ".", e
				);

				// Recover JMS session to mark all previous received messages for redelivery.
				recover();

				// Clear state. All messages we picked up from JMS provider will be redelivered, because they
				// have not been acknowledged.
				lastException = e;
				latestMessage = null;
				count = 0;
				receivedMessages.clear();
				result.clear();

				--remainingRetries;

				if ( remainingRetries >= 0 ) {
					if ( provider.getDialect().reconnectOnError( e ) ) {
						provider.closeQuietly();
					}

					// Only sleep if next retry attempt will take place.
					sleep();
				}
			}
		}
		if ( remainingRetries < 0 && lastException != null ) {
			throw new ConnectException(
					"Failed to poll records from JMS destination '" + JmsUtils.destinationNameForLog( destination ) +
					"': " + lastException.getMessage() + ".", lastException
			);
		}
		messagesToAcknowledge.putAll( receivedMessages );
		if ( ! receivedMessages.isEmpty() ) {
			lastMessage = latestMessage;
		}
		return result;
	}

	private void recover() {
		try {
			if ( session != null ) {
				session.recover();
			}
		}
		catch (JMSException e) {
			log.error( "Failed to recover JMS session: " + e.getMessage() + ".", e );
			provider.closeQuietly(); // Trigger reconnection.
		}
	}

	private void tryToReconnect() throws Exception {
		log.info( "Reconnecting to JMS server: " + BaseConnectorConfig.getBrokerUrl( configProperties ) + "." );
		try {
			terminateConsumer();
			if ( provider.reconnect() ) {
				createConsumer();
			}
			else {
				throw new IllegalStateException( "JMS reconnection in progress..." );
			}
		}
		catch ( Exception e ) {
			log.error( "Failed to re-establish connectivity with JMS server: " + e.getMessage() + ".", e );
			throw e;
		}
	}

	private void sleep() {
		try {
			// TODO: Unfortunately, there is no backoff functionality offered by source context.
			Thread.sleep( config.getLong( JmsSourceConnectorConfig.RETRY_BACKOFF_CONFIG ) );
		}
		catch ( InterruptedException ex ) {
			// Ignore.
		}
	}

	@Override
	public void commitRecord(SourceRecord record) throws InterruptedException {
		super.commitRecord( record );

		messagesToAcknowledge.remove( record );

		if ( messagesToAcknowledge.isEmpty() ) {
			try {
				lastMessage.acknowledge();
			}
			catch ( Exception e ) {
				// Ignore. Cannot do anything about it, message will be eventually redelivered. Hopefully in this
				// case Kafka Connect has configured idempotent producer and record key has been filled in.
				log.error(
						"Failed to acknowledge JMS message with ID '" + JmsUtils.messageIdForLog( lastMessage ) +
								"' received from destination '" + JmsUtils.destinationNameForLog( destination ) +
								"'. Redelivery will take place.", e
				);
				recover();
			}
			lastMessage = null;
			synchronized ( this ) {
				notifyAll();
			}
		}
	}

	@Override
	public synchronized void stop() {
		shuttingDown = true;
		try {
			// Waiting for all received JMS message to be acknowledged.
			while ( ! messagesToAcknowledge.isEmpty() ) {
				wait();
			}
		}
		catch ( InterruptedException e ) {
			log.warn( "Interrupted process waiting for all in-flight JMS messages to be acknowledged.", e );
		}
		terminate();
	}

	/**
	 * Visible for testing only.
	 *
	 * @param converter Test converter instance.
	 */
	void setConverter(JmsConverter converter) {
		this.converter = converter;
	}
}
