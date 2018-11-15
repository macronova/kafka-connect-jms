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
package io.macronova.kafka.connect.jms.domain;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Domain object representing JMS message.
 */
public class JmsMessage {
	public static final Schema SCHEMA_V1 = SchemaBuilder.struct().name( "JMSMessage" ).version( 1 )
			.field( "type", Schema.STRING_SCHEMA )
			.field( "messageId", Schema.STRING_SCHEMA )
			.field( "correlationId", Schema.OPTIONAL_STRING_SCHEMA )
			.field( "destination", JmsDestination.SCHEMA_V1 )
			.field( "replyTo", JmsDestination.SCHEMA_V1 )
			.field( "priority", Schema.INT32_SCHEMA )
			.field( "expiration", Schema.INT64_SCHEMA )
			.field( "timestamp", Schema.INT64_SCHEMA )
			.field( "redelivered", Schema.BOOLEAN_SCHEMA )
			.field( "properties", SchemaBuilder.map( Schema.STRING_SCHEMA, PropertyValue.SCHEMA_V1 ).optional() )
			.field( "payloadText", Schema.OPTIONAL_STRING_SCHEMA ) // Populated only for JMS text message.
			.field( "payloadMap", SchemaBuilder.map( Schema.STRING_SCHEMA, PropertyValue.SCHEMA_V1 ).optional() ) // Populated only for JMS map message.
			.field( "payloadBytes", Schema.OPTIONAL_BYTES_SCHEMA) // Populated only for JMS bytes message.
			.build();

	private final String type;
	private final String messageId;
	private final String correlationId;
	private final JmsDestination destination;
	private final JmsDestination replyTo;
	private final int priority;
	private final long expiration;
	private final long timestamp;
	private final boolean redelivered;
	private final Map<String, Struct> properties;
	private final String payloadText;
	private final Map<String, Struct> payloadMap;
	private final byte[] payloadBytes;

	public JmsMessage(Message jms) throws JMSException {
		this.messageId = jms.getJMSMessageID();
		this.correlationId = jms.getJMSCorrelationID();
		this.destination = jms.getJMSDestination() != null ? new JmsDestination( jms.getJMSDestination() ) : null;
		this.replyTo = jms.getJMSReplyTo() != null ? new JmsDestination( jms.getJMSReplyTo() ) : null;
		this.priority = jms.getJMSPriority();
		this.expiration = jms.getJMSExpiration();
		this.timestamp = jms.getJMSTimestamp();
		this.redelivered = jms.getJMSRedelivered();
		this.properties = propertiesMap( jms );
		if ( jms instanceof TextMessage ) {
			this.type = "text";
			this.payloadText = ( (TextMessage) jms ).getText();
			this.payloadMap = null;
			this.payloadBytes = null;
		}
		else if ( jms instanceof MapMessage ) {
			this.type = "map";
			final MapMessage mapMessage = (MapMessage) jms;
			final Map<String, Struct> map = new HashMap<>();
			final Enumeration names = mapMessage.getMapNames();
			while ( names.hasMoreElements() ) {
				final String name = names.nextElement().toString();
				map.put( name, new PropertyValue( mapMessage.getObject( name ) ).toStructV1() );
			}
			this.payloadText = null;
			this.payloadMap = map;
			this.payloadBytes = null;
		}
		else if ( jms instanceof BytesMessage ) {
			this.type = "bytes";
			final BytesMessage bytesMessage = (BytesMessage) jms;
			final byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
			bytesMessage.reset();
			bytesMessage.readBytes( bytes );
			this.payloadText = null;
			this.payloadMap = null;
			this.payloadBytes = bytes;
		}
		else {
			throw new UnsupportedOperationException( "JMS message type '" + jms.getClass() + "' is not supported." );
		}
	}

	private static Map<String, Struct> propertiesMap(Message jms) throws JMSException {
		final Map<String, Struct> result = new HashMap<>();
		final Enumeration names = jms.getPropertyNames();
		while ( names.hasMoreElements() ) {
			final String name = names.nextElement().toString();
			result.put( name, new PropertyValue( jms.getObjectProperty( name ) ).toStructV1() );
		}
		return result;
	}

	public Struct toStructV1() {
		final Struct result = new Struct( SCHEMA_V1 )
				.put( "type", type )
				.put( "messageId", messageId )
				.put( "correlationId", correlationId )
				.put( "priority", priority )
				.put( "expiration", expiration )
				.put( "timestamp", timestamp )
				.put( "redelivered", redelivered )
				.put( "properties", properties );
		if ( payloadText != null ) {
			result.put( "payloadText", payloadText );
		}
		if ( payloadMap != null ) {
			result.put( "payloadMap", payloadMap );
		}
		if ( payloadBytes != null ) {
			result.put( "payloadBytes", payloadBytes );
		}
		if ( destination != null ) {
			result.put( "destination", destination.toStructV1() );
		}
		if ( replyTo != null ) {
			result.put( "replyTo", replyTo.toStructV1() );
		}
		return result;
	}
}
