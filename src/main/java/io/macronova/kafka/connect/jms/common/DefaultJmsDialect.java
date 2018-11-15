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
import javax.jms.ConnectionFactory;

/**
 * Default JMS dialect.
 */
public class DefaultJmsDialect implements JmsDialect {
	@Override
	public ConnectionFactory createConnectionFactory(Map<String, String> config) throws Exception {
		final String connectionFactoryClass = config.get( BaseConnectorConfig.CONNECTION_FACTORY_CLASS_CONFIG );
		final String brokerUrl = config.get( BaseConnectorConfig.JMS_URL_CONFIG );
		// By default assume that given connection factory class will provide constructor
		// with one argument representing broker URL.
		return (ConnectionFactory) Class.forName( connectionFactoryClass ).getConstructor( String.class ).newInstance( brokerUrl );
	}

	@Override
	public boolean reconnectOnError(Exception e) {
		// By default we reconnect only on exceptions notified to ExceptionListener.
		return false;
	}
}
