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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.macronova.kafka.connect.jms.sink.JmsSinkConnectorConfig;
import io.macronova.kafka.connect.jms.sink.JmsSinkTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import io.macronova.kafka.connect.jms.util.Version;

/**
 * JMS sink connector.
 */
public class JmsSinkConnector extends SinkConnector {
	private static final Logger log = LoggerFactory.getLogger( JmsSinkConnector.class );

	private Map<String, String> configProperties = null;
	private JmsSinkConnectorConfig config = null;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		log.info( "Starting JMS sink connector version " + version() + "." );
		try {
			configProperties = properties;
			config = new JmsSinkConnectorConfig( configProperties );
		}
		catch (ConfigException e) {
			throw new ConnectException(
					"Failed to start JMS sink connector due to configuration error: " + e.getMessage() + ".", e
			);
		}
	}

	@Override
	public Class<? extends Task> taskClass() {
		return JmsSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		final List<Map<String, String>> configs = new ArrayList<>( maxTasks );
		for ( int i = 0; i < maxTasks; ++i ) {
			configs.add( configProperties );
		}
		return configs;
	}

	@Override
	public void stop() {
	}

	@Override
	public ConfigDef config() {
		return JmsSinkConnectorConfig.CONFIG_DEF;
	}
}
