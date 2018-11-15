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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic JMS connection pool implementation. Every connector instance (sink and source) tries to establishes JMS
 * connection, but reuses existing one if all connectivity-related parameters match. Each task works in
 * dedicated JMS sessions.
 */
public abstract class JmsConnectionPool {
	private static final Logger log = LoggerFactory.getLogger( JmsConnectionPool.class );
	private static final Map<Map<String, String>, AtomicInteger> usageCount = new HashMap<>();
	private static final Map<Map<String, String>, JmsSessionProvider> pool = new HashMap<>();

	/**
	 * Open new JMS connection or reuse existing one.
	 *
	 * @param key Connector configuration.
	 * @return JMS session provider.
	 * @throws Exception Failed to establish JMS connectivity.
	 */
	public static synchronized JmsSessionProvider getOrCreateConnection(Map<String, String> key) throws Exception {
		final Map<String, String> connectionKey = toConnectionKey( key );
		JmsSessionProvider provider = pool.get( connectionKey );
		if ( provider == null ) {
			log.info( "Opening new JMS connection to server: " + BaseConnectorConfig.getBrokerUrl( connectionKey ) + "." );
			// First try to establish JMS connectivity. If failed, we do not need to clean-up usage counter.
			provider = new JmsSessionProvider( connectionKey );
			usageCount.put( connectionKey, new AtomicInteger( 1 ) );
			pool.put( connectionKey, provider );
		}
		else {
			// Connection already established, just increase usage count.
			usageCount.get( connectionKey ).incrementAndGet();
		}
		return provider;
	}

	/**
	 * Close JMS connection.
	 *
	 * @param key Connector configuration.
	 */
	public static synchronized void destroyConnection(Map<String, String> key) {
		final Map<String, String> connectionKey = toConnectionKey( key );
		final AtomicInteger count = usageCount.get( connectionKey );
		if ( count != null ) {
			final int usage = count.decrementAndGet();
			if ( usage == 0 ) {
				log.info( "Closing JMS connection to server: " + BaseConnectorConfig.getBrokerUrl( connectionKey ) + "." );
				usageCount.remove( connectionKey );
				pool.remove( connectionKey ).closeQuietly();
			}
			else {
				// We are not the last user of this connection.
				usageCount.get( connectionKey ).set( usage );
			}
		}
	}

	/**
	 * To minimize number of required JMS connections, create connection identifier by extracting only
	 * connectivity-related configuration parameters.
	 *
	 * @param config Connector configuration.
	 * @return JMS connection key.
	 */
	private static Map<String, String> toConnectionKey(Map<String, String> config) {
		final Map<String, String> key = new HashMap<>();
		key.put( BaseConnectorConfig.CONTEXT_FACTORY_CONFIG, config.get( BaseConnectorConfig.CONTEXT_FACTORY_CONFIG ) );
		key.put( BaseConnectorConfig.PROVIDER_URL_CONFIG, config.get( BaseConnectorConfig.PROVIDER_URL_CONFIG ) );
		key.put( BaseConnectorConfig.SECURITY_PRINCIPAL_CONFIG, config.get( BaseConnectorConfig.SECURITY_PRINCIPAL_CONFIG ) );
		key.put( BaseConnectorConfig.SECURITY_CREDENTIALS_CONFIG, config.get( BaseConnectorConfig.SECURITY_CREDENTIALS_CONFIG ) );
		key.put( BaseConnectorConfig.JNDI_PARAMS_CONFIG, config.get( BaseConnectorConfig.JNDI_PARAMS_CONFIG ) );
		key.put( BaseConnectorConfig.CONNECTION_FACTORY_NAME_CONFIG, config.get( BaseConnectorConfig.CONNECTION_FACTORY_NAME_CONFIG ) );
		key.put( BaseConnectorConfig.CONNECTION_FACTORY_CLASS_CONFIG, config.get( BaseConnectorConfig.CONNECTION_FACTORY_CLASS_CONFIG ) );
		key.put( BaseConnectorConfig.JMS_URL_CONFIG, config.get( BaseConnectorConfig.JMS_URL_CONFIG ) );
		key.put( BaseConnectorConfig.JMS_USERNAME_CONFIG, config.get( BaseConnectorConfig.JMS_USERNAME_CONFIG ) );
		key.put( BaseConnectorConfig.JMS_PASSWORD_CONFIG, config.get( BaseConnectorConfig.JMS_PASSWORD_CONFIG ) );
		key.put( BaseConnectorConfig.JMS_CLIENT_ID_CONFIG, config.get( BaseConnectorConfig.JMS_CLIENT_ID_CONFIG ) );
		key.put( BaseConnectorConfig.JMS_DIALECT_CONFIG, config.getOrDefault( BaseConnectorConfig.JMS_DIALECT_CONFIG, BaseConnectorConfig.JMS_DIALECT_DEFAULT ) );
		for ( Map.Entry<String, String> param : config.entrySet() ) {
			// Copy all custom dialect properties.
			if ( param.getKey().startsWith( "jms.dialect." ) ) {
				key.put( param.getKey(), param.getValue() );
			}
		}
		return key;
	}
}
