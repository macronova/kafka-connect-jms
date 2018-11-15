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

import java.util.Map;

import io.macronova.kafka.connect.jms.common.BaseConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Sink connector configuration.
 */
public class JmsSinkConnectorConfig extends BaseConnectorConfig {
	public static final ConfigDef CONFIG_DEF = baseConfigDef();

	public static final String OUTPUT_FORMAT_CONFIG = "jms.message.converter.output.format";
	public static final String OUTPUT_FORMAT_DEFAULT = "text";
	private static final String OUTPUT_FORMAT_DOC = "Output JMS message format. Options: text | map | object | bytes. Default: text.";
	private static final String OUTPUT_FORMAT_DISPLAY = "Output format";

	public JmsSinkConnectorConfig(Map<?, ?> originals) {
		super( CONFIG_DEF, originals );
	}

	private static ConfigDef baseConfigDef() {
		final ConfigDef config = new ConfigDef();
		addKafkaGroup( config );
		addJndiGroup( config );
		addJmsGroup( config );
		addCustomFormatGroup( config );
		addRetryGroup( config );
		return config;
	}

	private static void addCustomFormatGroup(ConfigDef config) {
		int orderInGroup = BaseConnectorConfig.addFormatGroup( config );
		config.define(
				OUTPUT_FORMAT_CONFIG, ConfigDef.Type.STRING, OUTPUT_FORMAT_DEFAULT, ConfigDef.Importance.MEDIUM,
				OUTPUT_FORMAT_DOC, FORMAT_GROUP, ++orderInGroup, ConfigDef.Width.SHORT, OUTPUT_FORMAT_DISPLAY
		);
	}
}
