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

import java.util.Map;

import io.macronova.kafka.connect.jms.common.BaseConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Source connector configuration.
 */
public class JmsSourceConnectorConfig extends BaseConnectorConfig {
	public static final ConfigDef CONFIG_DEF = baseConfigDef();

	public static final String MAX_POLL_CONFIG = "max.poll.records";
	private static final int MAX_POLL_DEFAULT = 100;
	private static final String MAX_POLL_DOC = "The maximum number of JMS messages retrieved during every poll operation.";
	private static final String MAX_POLL_DISPLAY = "Maximum poll records";

	public static final String POLL_TIMEOUT_CONFIG = "poll.timeout.ms";
	private static final long POLL_TIMEOUT_DEFAULT = 1000L;
	private static final String POLL_TIMEOUT_DOC = "Maximum poll timeout for incoming message [ms.].";
	private static final String POLL_TIMEOUT_DISPLAY = "Poll timeout";

	public static final String JMS_SELECTOR_CONFIG = "jms.selector";
	private static final String JMS_SELECTOR_DOC = "JMS message selector.";
	private static final String JMS_SELECTOR_DISPLAY = "Selector";

	public static final String JMS_SUBSCRIPTION_CONFIG = "jms.topic.subscription.name";
	private static final String JMS_SUBSCRIPTION_DOC = "If source connector should poll messages from JSM topic, specify name of subscription.";
	private static final String JMS_SUBSCRIPTION_DISPLAY = "Subscription name";

	public static final String JMS_SUBSCRIPTION_DURABLE_CONFIG = "jms.topic.subscription.durable";
	private static final String JMS_SUBSCRIPTION_DURABLE_DOC = "Positive if user wishes to register durable topic subscription. Default: true.";
	private static final String JMS_SUBSCRIPTION_DURABLE_DISPLAY = "Register durable subscription";

	public static final String JMS_SUBSCRIPTION_SHARED_CONFIG = "jms.topic.subscription.shared";
	private static final String JMS_SUBSCRIPTION_SHARED_DOC = "Positive if user wishes to register shared durable topic subscription " +
			"(requires JMS 2.0 compliant server). Default: true.";
	private static final String JMS_SUBSCRIPTION_SHARED_DISPLAY = "Register shared durable subscription";

	public JmsSourceConnectorConfig(Map<?, ?> originals) {
		super( CONFIG_DEF, originals );
	}

	private static ConfigDef baseConfigDef() {
		final ConfigDef config = new ConfigDef();
		addKafkaGroup( config );
		addJndiGroup( config );
		addCustomJmsGroup( config );
		addFormatGroup( config );
		addRetryGroup( config );
		return config;
	}

	private static void addCustomJmsGroup(ConfigDef config) {
		int orderInGroup = BaseConnectorConfig.addJmsGroup( config );
		config.define(
				JMS_SELECTOR_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
				JMS_SELECTOR_DOC, JMS_GROUP, ++orderInGroup, ConfigDef.Width.LONG, JMS_SELECTOR_DISPLAY
		);
		config.define(
				MAX_POLL_CONFIG, ConfigDef.Type.INT, MAX_POLL_DEFAULT, ConfigDef.Importance.LOW,
				MAX_POLL_DOC, JMS_GROUP, ++orderInGroup, ConfigDef.Width.SHORT, MAX_POLL_DISPLAY
		);
		config.define(
				POLL_TIMEOUT_CONFIG, ConfigDef.Type.LONG, POLL_TIMEOUT_DEFAULT, ConfigDef.Importance.LOW,
				POLL_TIMEOUT_DOC, JMS_GROUP, ++orderInGroup, ConfigDef.Width.SHORT, POLL_TIMEOUT_DISPLAY
		);
		config.define(
				JMS_SUBSCRIPTION_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
				JMS_SUBSCRIPTION_DOC, JMS_GROUP, ++orderInGroup, ConfigDef.Width.LONG, JMS_SUBSCRIPTION_DISPLAY
		);
		config.define(
				JMS_SUBSCRIPTION_DURABLE_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
				JMS_SUBSCRIPTION_DURABLE_DOC, JMS_GROUP, ++orderInGroup, ConfigDef.Width.SHORT, JMS_SUBSCRIPTION_DURABLE_DISPLAY
		);
		config.define(
				JMS_SUBSCRIPTION_SHARED_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
				JMS_SUBSCRIPTION_SHARED_DOC, JMS_GROUP, ++orderInGroup, ConfigDef.Width.SHORT, JMS_SUBSCRIPTION_SHARED_DISPLAY
		);
	}
}
