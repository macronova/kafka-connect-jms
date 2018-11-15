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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Common JMS connector configuration properties.
 */
public abstract class BaseConnectorConfig extends AbstractConfig {
	private static final String KAFKA_GROUP = "Kafka";

	public static final String KAFKA_TOPIC_CONFIG = "topics";
	private static final String KAFKA_TOPIC_DOC = "Target Kafka topic name.";
	private static final String KAFKA_TOPIC_DISPLAY = "Topic name";

	private static final String JNDI_GROUP = "JNDI";

	public static final String CONTEXT_FACTORY_CONFIG = "java.naming.factory.initial";
	private static final String CONTEXT_FACTORY_DOC =
			"Fully qualified name of JNDI context factory implementation. Specify only when creation of administrative objects" +
					"should be done via JNDI look-up. Example: org.apache.activemq.jndi.ActiveMQInitialContextFactory.";
	private static final String CONTEXT_FACTORY_DISPLAY = "Context factory implementation class";

	public static final String PROVIDER_URL_CONFIG = "java.naming.provider.url";
	private static final String PROVIDER_URL_DOC = "JNDI provider URL. Example: tcp://localhost:61616.";
	private static final String PROVIDER_URL_DISPLAY = "Provider URL";

	public static final String SECURITY_PRINCIPAL_CONFIG = "java.naming.security.principal";
	private static final String SECURITY_PRINCIPAL_DOC = "Security principal.";
	private static final String SECURITY_PRINCIPAL_DISPLAY = "Security principal";

	public static final String SECURITY_CREDENTIALS_CONFIG = "java.naming.security.credentials";
	private static final String SECURITY_CREDENTIALS_DOC = "Security credentials.";
	private static final String SECURITY_CREDENTIALS_DISPLAY = "Security credentials";

	public static final String JNDI_PARAMS_CONFIG = "jndi.extra.params";
	private static final String JNDI_PARAMS_DOC = "Comma-separated list of extra JNDI parameters. Example: MyKey1=Value1,MyKey2=Value2.";
	private static final String JNDI_PARAMS_DISPLAY = "Extra parameters";

	public static final String CONNECTION_FACTORY_NAME_CONFIG = "jndi.connection.factory";
	private static final String CONNECTION_FACTORY_NAME_DOC = "Name of connection factory to look-up.";
	private static final String CONNECTION_FACTORY_NAME_DISPLAY = "Connection factory";

	protected static final String JMS_GROUP = "JMS";

	public static final String CONNECTION_FACTORY_CLASS_CONFIG = "jms.connection.factory";
	private static final String CONNECTION_FACTORY_CLASS_DOC = "Fully qualified name of JMS connection factory implementation. " +
			"Specify only in case of direct broker access (without JNDI lookup). Example: org.apache.activemq.ActiveMQConnectionFactory.";
	private static final String CONNECTION_FACTORY_CLASS_DISPLAY = "Connection factory implementation class";

	public static final String JMS_URL_CONFIG = "jms.url";
	private static final String JMS_URL_DOC = "JMS broker URL. Required only in case of direct broker access (without JNDI involved).";
	private static final String JMS_URL_DISPLAY = "Broker URL";

	public static final String JMS_USERNAME_CONFIG = "jms.username";
	private static final String JMS_USERNAME_DOC = "JMS username.";
	private static final String JMS_USERNAME_DISPLAY = "Username";

	public static final String JMS_PASSWORD_CONFIG = "jms.password";
	private static final String JMS_PASSWORD_DOC = "JMS password.";
	private static final String JMS_PASSWORD_DISPLAY = "Password";

	public static final String JMS_DESTINATION_NAME_CONFIG = "jms.destination.name";
	private static final String JMS_DESTINATION_NAME_DOC = "JMS destination name.";
	private static final String JMS_DESTINATION_NAME_DISPLAY = "Destination name";

	public static final String JMS_DESTINATION_TYPE_CONFIG = "jms.destination.type";
	private static final String JMS_DESTINATION_TYPE_DOC = "JMS destination type. Options: queue | topic. Default: queue.";
	private static final String JMS_DESTINATION_TYPE_DISPLAY = "Destination type";

	public static final String JMS_CLIENT_ID_CONFIG = "jms.client.id";
	private static final String JMS_CLIENT_ID_DOC = "JMS client ID.";
	private static final String JMS_CLIENT_ID_DISPLAY = "JMS client ID";

	public static final String JMS_DIALECT_CONFIG = "jms.dialect.class";
	public static final String JMS_DIALECT_DEFAULT = DefaultJmsDialect.class.getName();
	private static final String JMS_DIALECT_DOC = "Implementation of " + JmsDialect.class.getName() + " interface, " +
			"which allows to customize JMS provider specific behavior. Default: " + JMS_DIALECT_DEFAULT + ".";
	private static final String JMS_DIALECT_DISPLAY = "JMS dialect class";

	protected static final String FORMAT_GROUP = "Format";

	public static final String CONVERTER_CONFIG = "jms.message.converter";
	private static final String CONVERTER_DEFAULT = StandardJmsConverter.class.getName();
	private static final String CONVERTER_DOC = "Implementation of " + JmsConverter.class.getName() + " interface, " +
			"which allows to customize conversion between Kafka Connect records and JMS messages. Default: " +
			CONVERTER_DEFAULT + ".";
	private static final String CONVERTER_DISPLAY = "Converter class";

	private static final String RETRY_GROUP = "Retry";

	public static final String MAX_RETRIES_CONFIG = "max.retries";
	private static final int MAX_RETRIES_DEFAULT = 10;
	private static final String MAX_RETRIES_DOC = "The maximum number of retry attempts in case of error before failing the task.";
	private static final String MAX_RETRIES_DISPLAY = "Maximum retries";

	public static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
	private static final int RETRY_BACKOFF_DEFAULT = 5000;
	private static final String RETRY_BACKOFF_DOC =
			"The time in milliseconds to wait following an error before next retry attempt.";
	private static final String RETRY_BACKOFF_DISPLAY = "Retry backoff [ms.]";

	protected BaseConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
		super( definition, originals );
	}

	protected static void addKafkaGroup(ConfigDef config) {
		final String group = KAFKA_GROUP;
		int orderInGroup = 0;
		config.define(
				KAFKA_TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
				KAFKA_TOPIC_DOC, group, ++orderInGroup, ConfigDef.Width.LONG, KAFKA_TOPIC_DISPLAY
		);
	}

	protected static int addJndiGroup(ConfigDef config) {
		final String group = JNDI_GROUP;
		int orderInGroup = 0;
		config.define(
				CONTEXT_FACTORY_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, CONTEXT_FACTORY_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, CONTEXT_FACTORY_DISPLAY
		);
		config.define(
				PROVIDER_URL_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, PROVIDER_URL_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, PROVIDER_URL_DISPLAY
		);
		config.define(
				SECURITY_PRINCIPAL_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, SECURITY_PRINCIPAL_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, SECURITY_PRINCIPAL_DISPLAY
		);
		config.define(
				SECURITY_CREDENTIALS_CONFIG, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.HIGH, SECURITY_CREDENTIALS_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, SECURITY_CREDENTIALS_DISPLAY
		);
		config.define(
				JNDI_PARAMS_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, JNDI_PARAMS_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, JNDI_PARAMS_DISPLAY
		);
		config.define(
				CONNECTION_FACTORY_NAME_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, CONNECTION_FACTORY_NAME_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, CONNECTION_FACTORY_NAME_DISPLAY
		);
		return orderInGroup;
	}

	protected static int addJmsGroup(ConfigDef config) {
		final String group = JMS_GROUP;
		int orderInGroup = 0;
		config.define(
				CONNECTION_FACTORY_CLASS_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, CONNECTION_FACTORY_CLASS_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, CONNECTION_FACTORY_CLASS_DISPLAY
		);
		config.define(
				JMS_URL_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, JMS_URL_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, JMS_URL_DISPLAY
		);
		config.define(
				JMS_USERNAME_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, JMS_USERNAME_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, JMS_USERNAME_DISPLAY
		);
		config.define(
				JMS_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.HIGH, JMS_PASSWORD_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, JMS_PASSWORD_DISPLAY
		);
		config.define(
				JMS_CLIENT_ID_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, JMS_CLIENT_ID_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, JMS_CLIENT_ID_DISPLAY
		);
		config.define(
				JMS_DESTINATION_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JMS_DESTINATION_NAME_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, JMS_DESTINATION_NAME_DISPLAY, Collections.singletonList( JMS_DESTINATION_TYPE_CONFIG )
		);
		config.define(
				JMS_DESTINATION_TYPE_CONFIG, ConfigDef.Type.STRING, "queue", ConfigDef.Importance.MEDIUM, JMS_DESTINATION_TYPE_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, JMS_DESTINATION_TYPE_DISPLAY, Collections.singletonList( JMS_DESTINATION_NAME_CONFIG ),
				new ConfigDef.Recommender() {
					@Override
					public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
						return Arrays.asList( "queue", "topic" );
					}

					@Override
					public boolean visible(String name, Map<String, Object> parsedConfig) {
						return true;
					}
				}
		);
		config.define(
				JMS_DIALECT_CONFIG, ConfigDef.Type.STRING, JMS_DIALECT_DEFAULT, ConfigDef.Importance.MEDIUM, JMS_DIALECT_DOC,
				group, ++orderInGroup, ConfigDef.Width.LONG, JMS_DIALECT_DISPLAY
		);
		return orderInGroup;
	}

	protected static int addFormatGroup(ConfigDef config) {
		final String group = FORMAT_GROUP;
		int orderInGroup = 0;
		config.define(
				CONVERTER_CONFIG, ConfigDef.Type.STRING, CONVERTER_DEFAULT, ConfigDef.Importance.HIGH,
				CONVERTER_DOC, group, ++orderInGroup, ConfigDef.Width.LONG, CONVERTER_DISPLAY
		);
		return orderInGroup;
	}

	protected static void addRetryGroup(ConfigDef config) {
		final String group = RETRY_GROUP;
		int orderInGroup = 0;
		config.define(
				MAX_RETRIES_CONFIG, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, ConfigDef.Importance.MEDIUM,
				MAX_RETRIES_DOC, group, ++orderInGroup, ConfigDef.Width.SHORT, MAX_RETRIES_DISPLAY
		);
		config.define(
				RETRY_BACKOFF_CONFIG, ConfigDef.Type.LONG, RETRY_BACKOFF_DEFAULT, ConfigDef.Importance.MEDIUM,
				RETRY_BACKOFF_DOC, group, ++orderInGroup, ConfigDef.Width.SHORT, RETRY_BACKOFF_DISPLAY
		);
	}

	public static String getBrokerUrl(Map<String, String> config) {
		return config.get( PROVIDER_URL_CONFIG ) == null ? config.get( JMS_URL_CONFIG ) : config.get( PROVIDER_URL_CONFIG );
	}
}
