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
package io.macronova.kafka.connect.jms.sink;

import java.util.Collection;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.macronova.kafka.connect.jms.common.BaseConnectorConfig;
import io.macronova.kafka.connect.jms.util.JmsUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import io.macronova.kafka.connect.jms.common.JmsConverter;
import io.macronova.kafka.connect.jms.common.JmsConnectionPool;
import io.macronova.kafka.connect.jms.common.JmsSessionProvider;
import io.macronova.kafka.connect.jms.util.Version;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 * JMS sink task.
 */
public class JmsSinkTask extends SinkTask {
	private static final Logger log = LoggerFactory.getLogger( JmsSinkTask.class );

	private JmsSinkConnectorConfig config = null;
	private Map<String, String> configProperties = null;
	private int remainingRetries = -1;
	private JmsSessionProvider provider = null;
	private Session session = null;
	private Destination destination = null;
	private MessageProducer producer = null;
	private JmsConverter converter = null;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		configProperties = properties;
		config = new JmsSinkConnectorConfig( properties );
		remainingRetries = config.getInt( JmsSinkConnectorConfig.MAX_RETRIES_CONFIG );
		try {
			provider = JmsConnectionPool.getOrCreateConnection( configProperties );
			converter = (JmsConverter) Class.forName( config.getString( JmsSinkConnectorConfig.CONVERTER_CONFIG ) ).newInstance();
			converter.configure( configProperties );
			createProducer();
		}
		catch (Exception e) {
			terminate();
			throw new ConnectException(
					"Failed to start JMS sink task: " + e.getMessage() + ".", e
			);
		}
	}

	private void createProducer() throws Exception {
		session = provider.createSession( true );
		destination = provider.resolveDestination(
				session, config.getString( JmsSinkConnectorConfig.JMS_DESTINATION_NAME_CONFIG ),
				config.getString( JmsSinkConnectorConfig.JMS_DESTINATION_TYPE_CONFIG )
		);
		producer = session.createProducer( destination );
	}

	private void terminateProducer() {
		if ( producer != null ) {
			try {
				producer.close();
			}
			catch (Exception e) {
				// Ignore.
			}
			producer = null;
		}
		if ( session != null ) {
			tryRollback();
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
		terminateProducer();
		if ( provider != null ) {
			JmsConnectionPool.destroyConnection( configProperties );
			provider = null;
		}
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		if ( records.isEmpty() ) {
			return;
		}
		try {
			if ( provider.isClosed() || session == null || producer == null ) {
				tryToReconnect();
			}
			for ( SinkRecord record : records ) {
				producer.send( converter.recordToMessage( session, record ) );
			}
			session.commit();
			// TODO: Does it make sense to invoke context.requestCommit()?
		}
		catch ( Exception e ) {
			log.error(
					"Failed to publish messages to JMS destination '" + JmsUtils.destinationNameForLog( destination ) +
					"': " + e.getMessage() + ".", e
			);
			tryRollback();
			if ( remainingRetries == 0 ) {
				throw new ConnectException( e );
			}
			else {
				if ( provider.getDialect().reconnectOnError( e ) ) {
					provider.closeQuietly();
				}
				--remainingRetries;
				context.timeout( config.getLong( JmsSinkConnectorConfig.RETRY_BACKOFF_CONFIG ) );
				throw new RetriableException( e );
			}
		}
		remainingRetries = config.getInt( JmsSinkConnectorConfig.MAX_RETRIES_CONFIG );
	}

	private void tryRollback() {
		try {
			if ( session != null ) {
				session.rollback();
			}
		}
		catch (Exception e) {
			// Ignore.
		}
	}

	private void tryToReconnect() throws Exception {
		log.info( "Reconnecting to JMS server: " + BaseConnectorConfig.getBrokerUrl( configProperties ) + "." );
		try {
			terminateProducer();
			if ( provider.reconnect() ) {
				createProducer();
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

	@Override
	public void stop() {
		terminate();
	}

	/**
	 * Visible for testing only.
	 *
	 * @return Number of remaining retries to publish JMS messages.
	 */
	int getRemainingRetries() {
		return remainingRetries;
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