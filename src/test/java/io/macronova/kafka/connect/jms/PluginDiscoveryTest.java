/*
 * Copyright 2018 Macronova.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.macronova.kafka.connect.jms;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import io.macronova.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;

/**
 * Kafka Connect 4.0 discovers plugins with {@link ServiceLoader} by default (plugin.discovery=service_load), so
 * every connector and converter has to be listed in META-INF/services.
 */
public class PluginDiscoveryTest {
	@Test
	public void testSourceConnectorDiscoverable() {
		Assert.assertTrue( discover( SourceConnector.class ).contains( JmsSourceConnector.class.getName() ) );
	}

	@Test
	public void testSinkConnectorDiscoverable() {
		Assert.assertTrue( discover( SinkConnector.class ).contains( JmsSinkConnector.class.getName() ) );
	}

	@Test
	public void testConverterDiscoverable() {
		Assert.assertTrue( discover( Converter.class ).contains( ByteArrayConverter.class.getName() ) );
	}

	private static <T> Set<String> discover(Class<T> type) {
		final Set<String> found = new HashSet<>();
		for ( T plugin : ServiceLoader.load( type, PluginDiscoveryTest.class.getClassLoader() ) ) {
			found.add( plugin.getClass().getName() );
		}
		return found;
	}
}
