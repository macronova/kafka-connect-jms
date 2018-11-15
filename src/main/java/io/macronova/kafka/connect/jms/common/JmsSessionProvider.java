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
package io.macronova.kafka.connect.jms.common;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.InitialContext;

import io.macronova.kafka.connect.jms.util.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Factory for JMS sessions. Manages single, active JMS connection.
 */
public class JmsSessionProvider {
	private final Map<String, String> config;
	private final JmsDialect dialect;
	private InitialContext context;
	private Connection connection;
	private final AtomicBoolean reconnectionGuard = new AtomicBoolean( false );
	private final AtomicBoolean closed = new AtomicBoolean( false );

	/**
	 * Establishes new JMS connection.
	 *
	 * @param properties Connector configuration.
	 */
	public JmsSessionProvider(Map<String, String> properties) throws ConnectException {
		try {
			config = properties;
			final String dialectClass = properties.get( BaseConnectorConfig.JMS_DIALECT_CONFIG );
			dialect = (JmsDialect) Class.forName( dialectClass ).newInstance();
			connect( properties );
		}
		catch ( Exception e ) {
			throw new ConnectException(
					"Failed to establish JMS connectivity: " + e.getMessage() + ".", e
			);
		}
	}

	private void connect(Map<String, String> properties) throws ConnectException {
		try {
			ConnectionFactory factory = null;
			if ( !StringUtils.isEmpty( properties.get( InitialContext.INITIAL_CONTEXT_FACTORY ) ) ) {
				// JNDI based access.
				final Properties contextConfig = new Properties();
				contextConfig.put( InitialContext.INITIAL_CONTEXT_FACTORY, properties.get( InitialContext.INITIAL_CONTEXT_FACTORY ) );
				contextConfig.put( InitialContext.PROVIDER_URL, properties.get( InitialContext.PROVIDER_URL ) );
				final String user = properties.get( InitialContext.SECURITY_PRINCIPAL );
				if ( ! StringUtils.isEmpty( user ) ) {
					contextConfig.put( InitialContext.SECURITY_PRINCIPAL, user );
				}
				final String password = properties.get( InitialContext.SECURITY_CREDENTIALS );
				if ( ! StringUtils.isEmpty( password ) ) {
					contextConfig.put( InitialContext.SECURITY_CREDENTIALS, password );
				}
				final String extraParams = properties.get( BaseConnectorConfig.JNDI_PARAMS_CONFIG );
				if ( extraParams != null ) {
					for ( String param : extraParams.split( "," ) ) {
						final String[] kv = param.split( "=" );
						contextConfig.put( kv[0].trim(), kv[1].trim() );
					}
				}
				context = new InitialContext( contextConfig );
				factory = (ConnectionFactory) context.lookup( properties.get( BaseConnectorConfig.CONNECTION_FACTORY_NAME_CONFIG ) );
			}
			else {
				// Direct server access.
				context = null;
				factory = dialect.createConnectionFactory( properties );
			}
			final String user = properties.get( BaseConnectorConfig.JMS_USERNAME_CONFIG );
			if ( ! StringUtils.isEmpty( user ) ) {
				connection = factory.createConnection( user, properties.get( BaseConnectorConfig.JMS_PASSWORD_CONFIG ) );
			}
			else {
				connection = factory.createConnection();
			}
			final String clientId = properties.get( BaseConnectorConfig.JMS_CLIENT_ID_CONFIG );
			if ( ! StringUtils.isEmpty( clientId ) ) {
				connection.setClientID( clientId );
			}
			connection.start();
			connection.setExceptionListener( new ExceptionListener() {
				@Override
				public void onException(JMSException exception) {
					closed.set( true );
				}
			} );
			closed.set( false );
		}
		catch ( Exception e ) {
			throw new ConnectException(
					"Failed to establish JMS connectivity: " + e.getMessage() + ".", e
			);
		}
	}

	/**
	 * Tries to re-establish connectivity with JMS server. Only one thread will try to reconnect at a time.
	 *
	 * @return {@code true} if successfully reconnected, {@code false} otherwise.
	 */
	public boolean reconnect() {
		// Applied pattern: http://concurrencyfreaks.blogspot.com/2014/06/elected-pattern.html.
		if ( closed.get() && reconnectionGuard.compareAndSet( false, true ) ) {
			try {
				closeQuietly();
				connect( config );
				return true;
			}
			finally {
				reconnectionGuard.set( false );
			}
		}
		return ! closed.get();
	}

	/**
	 * @return {@code true} if JMS connection seems to be broken or closed.
	 */
	public boolean isClosed() {
		return closed.get();
	}

	/**
	 * @return JMS dialect.
	 */
	public JmsDialect getDialect() {
		return dialect;
	}

	/**
	 * @param transacted Flag specifying whether session needs to be transacted.
	 * @return JMS session with client acknowledgement mode.
	 * @throws JMSException Indicates runtime error.
	 */
	public Session createSession(boolean transacted) throws JMSException {
		return connection.createSession( transacted, Session.CLIENT_ACKNOWLEDGE );
	}

	/**
	 * Resolve JMS destination either by JNDI lookup or call to JMS API.
	 *
	 * @param session JMS session.
	 * @param destination Destination name.
	 * @param type Destination type: {@code queue} or {@code topic}.
	 * @return JMS destination.
	 * @throws Exception Indicates failure.
	 */
	public Destination resolveDestination(Session session, String destination, String type) throws Exception {
		if ( context != null ) {
			return (Destination) context.lookup( destination );
		}
		else if ( "topic".equals( type ) ) {
			return session.createTopic( destination );
		}
		else {
			return session.createQueue( destination );
		}
	}

	/**
	 * Quietly close JMS connection.
	 */
	public void closeQuietly() {
		if ( connection != null ) {
			try {
				connection.close();
			}
			catch (Exception e) {
				// Ignore.
			}
		}
		if ( context != null ) {
			try {
				context.close();
			}
			catch (Exception e) {
				// Ignore.
			}
		}
		closed.set( true );
	}
}
