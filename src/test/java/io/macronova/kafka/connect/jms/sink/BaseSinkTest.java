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
import java.util.List;
import java.util.Map;

import io.macronova.kafka.connect.jms.BaseFunctionalTest;
import io.macronova.kafka.connect.jms.JmsSinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public abstract class BaseSinkTest extends BaseFunctionalTest {
	/**
	 * Simulates execution of Kafka Connect sink connector.
	 *
	 * @param configuration Connector configuration.
	 * @param records Collection of sink records.
	 * @param taskCount Number of tasks to trigger.
	 * @throws Exception Indicates failure.
	 */
	protected void runSink(Map<String, String> configuration, Collection<SinkRecord> records, int taskCount) throws Exception {
		final JmsSinkConnector connector = new JmsSinkConnector();
		connector.start( configuration );
		final List<Map<String, String>> taskConfigs = connector.taskConfigs( taskCount );
		for ( Map<String, String> config : taskConfigs ) {
			final SinkTask task = (SinkTask) connector.taskClass().getConstructor().newInstance();
			task.start( config );
			task.put( records );
			task.stop();
		}
		connector.stop();
	}

	protected Map<String, String> configurationJndi() {
		final Map<String, String> properties = super.configurationJndi();
		properties.put( "max.retries", "0" ); // By default, all tests should pass without necessity to retry.
		return properties;
	}

	protected Map<String, String> configurationDirect() {
		final Map<String, String> properties = super.configurationDirect();
		properties.put( "max.retries", "0" ); // By default, all tests should pass without necessity to retry.
		return properties;
	}
}
