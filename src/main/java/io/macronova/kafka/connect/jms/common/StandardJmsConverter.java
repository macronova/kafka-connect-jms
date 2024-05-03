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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import io.confluent.connect.avro.AvroConverter;
import io.macronova.kafka.connect.jms.sink.JmsSinkConnectorConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import io.macronova.kafka.connect.jms.domain.JmsMessage;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Default implementation of {@link JmsConverter} interface.
 */
public class StandardJmsConverter implements JmsConverter {
	/**
	 * Output message types produced by sink connector.
	 */
	public enum OutputFormat {
		TEXT, MAP, OBJECT, BYTES, JSON, AVRO
	}

	private Map<String, String> configuration = null;
	private OutputFormat outputFormat = null;
	private Converter jsonConverter = null;
	private Converter avroConverter = null;

	@Override
	public void configure(Map<String, String> properties) {
		configuration = new HashMap<>( properties );
		outputFormat = OutputFormat.valueOf(
				properties.getOrDefault(
						JmsSinkConnectorConfig.OUTPUT_FORMAT_CONFIG, JmsSinkConnectorConfig.OUTPUT_FORMAT_DEFAULT
				).toUpperCase()
		);
	}

	@Override
	public Message recordToMessage(Session session, SinkRecord record) throws JMSException {
		Message message = null;
		switch ( outputFormat ) {
			case TEXT: message = createTextMessage( session, record ); break;
			case JSON: message = createJsonMessage( session, record ); break;
			case MAP: message =  createMapMessage( session, record ); break;
			case OBJECT: message = createObjectMessage( session, record ); break;
			case BYTES: message = createBytesMessage( session, record ); break;
			case AVRO: message = createAvroMessage( session, record ); break;
		}
		if ( message == null ) {
			throw new UnsupportedOperationException( "Unsupported output format: " + outputFormat + "." );
		}
		addCommonProperties( message, record );
		addCustomPropertiesFromRecordHeaders( message, record );

		return message;
	}

	private Message createTextMessage(Session session, SinkRecord record) throws JMSException {
		final String value = record.value() != null ? record.value().toString() : "";
		return session.createTextMessage( value );
	}

	private Message createJsonMessage(Session session, SinkRecord record) throws JMSException {
		final byte[] serialized = lazyJsonConverter().fromConnectData( record.topic(), record.valueSchema(), record.value() );
		return session.createTextMessage( new String( serialized ) );
	}

	private Message createMapMessage(Session session, SinkRecord record) throws JMSException {
		final MapMessage message = session.createMapMessage();
		if ( record.valueSchema() != null && Schema.Type.STRUCT.equals( record.valueSchema().type() ) ) {
			// Structure message.
			Struct value = (Struct) record.value();
			for ( Field field : value.schema().fields() ) {
				mapField( field.name(), value.get( field ), field.schema(), session, message );
			}
		}
		else if ( record.valueSchema() != null ) {
			// Message with primitive type.
			mapField( "payload", record.value(), record.valueSchema(), session, message );
		}
		else {
			// Unknown message type.
			message.setObject( "payload", record.value() );
		}
		return message;
	}

	private Message createObjectMessage(Session session, SinkRecord record) throws JMSException {
		final ObjectMessage message = session.createObjectMessage();
		message.setObject( (Serializable) record.value() );
		return message;
	}

	private Message createBytesMessage(Session session, SinkRecord record) throws JMSException {
		final BytesMessage message = session.createBytesMessage();
		message.writeBytes( (byte[]) record.value() );
		return message;
	}

	private Message createAvroMessage(Session session, SinkRecord record) throws JMSException {
		final BytesMessage message = session.createBytesMessage();
		message.writeBytes( lazyAvroConverter().fromConnectData( record.topic(), record.valueSchema(), record.value() ) );
		return message;
	}

	private void mapField(String name, Object value, Schema schema, Session session, MapMessage message) throws JMSException {
		switch ( schema.type() ) {
			case BYTES: message.setBytes( name, (byte[]) value ); break;
			case BOOLEAN: message.setBoolean( name, (Boolean) value ); break;
			case FLOAT32: message.setFloat( name, (Float) value ); break;
			case FLOAT64: message.setDouble( name, (Double) value ); break;
			case INT8: message.setByte( name, (Byte) value ); break;
			case INT16: message.setShort( name, (Short) value ); break;
			case INT32: message.setInt( name, (Integer) value ); break;
			case INT64: message.setLong( name, (Long) value ); break;
			case STRING: message.setString( name, (String) value ); break;
			case MAP:
			case ARRAY: message.setObject( name, value ); break;
			case STRUCT:
				final MapMessage nestedMessage = session.createMapMessage();
				final Struct struct = (Struct) value;
				for ( Field field : struct.schema().fields() ) {
					mapField( field.name(), struct.get( field ), field.schema(), session, nestedMessage );
				}
				message.setObject( name, nestedMessage );
				break;
		}
	}

	private void addCommonProperties(Message message, SinkRecord record) throws JMSException {
		final Object key = record.key();
		setMessageProperty( message, "KafkaKey", key );
		message.setStringProperty( "KafkaTopic", record.topic() );
		message.setIntProperty( "KafkaPartition", record.kafkaPartition() );
		message.setLongProperty( "KafkaOffset", record.kafkaOffset() );
		if ( ! TimestampType.NO_TIMESTAMP_TYPE.equals( record.timestampType() ) ) {
			message.setLongProperty( "KafkaTimestamp", record.timestamp() );
		}
	}

	private void addCustomPropertiesFromRecordHeaders(Message message, SinkRecord record) throws JMSException {
		for ( Header header : record.headers() ) {
			setMessageProperty( message, header.key(), header.value() );
		}
	}

	private void setMessageProperty(Message message, String propertyName, Object propertyValue) throws JMSException {
		if ( propertyValue instanceof Byte ) {
			message.setByteProperty( propertyName, (Byte) propertyValue );
		}
		else if ( propertyValue instanceof Short ) {
			message.setShortProperty( propertyName, (Short) propertyValue );
		}
		else if ( propertyValue instanceof Integer ) {
			message.setIntProperty( propertyName, (Integer) propertyValue );
		}
		else if ( propertyValue instanceof Long ) {
			message.setLongProperty( propertyName, (Long) propertyValue );
		}
		else if ( propertyValue instanceof Float ) {
			message.setFloatProperty( propertyName, (Float) propertyValue );
		}
		else if ( propertyValue instanceof Double ) {
			message.setDoubleProperty( propertyName, (Double) propertyValue );
		}
		else if ( propertyValue instanceof Boolean ) {
			message.setBooleanProperty( propertyName, (Boolean) propertyValue );
		}
		else if ( propertyValue != null ) {
			message.setStringProperty( propertyName, propertyValue.toString() );
		}
	}

	private Converter lazyJsonConverter() {
		if ( jsonConverter == null ) {
			synchronized ( this ) {
				if ( jsonConverter == null ) {
					final JsonConverter converter = new JsonConverter();
					Map<String, String> copy = new HashMap<>( configuration );
					copy.put( "converter.type", "value" );
					converter.configure( copy );
					jsonConverter = converter;
				}
			}
		}
		return jsonConverter;
	}

	private Converter lazyAvroConverter() {
		if ( avroConverter == null ) {
			synchronized (this) {
				if ( avroConverter == null ) {
					final AvroConverter converter = new AvroConverter();
					Map<String, String> copy = new HashMap<>( configuration );
					converter.configure( copy, false );
					avroConverter = converter;
				}
			}
		}
		return avroConverter;
	}

	@Override
	public SourceRecord messageToRecord(Message message, String topic, Map<String, ?> sourcePartition,
										Map<String, ?> sourceOffset) throws JMSException {
		return new SourceRecord(
				sourcePartition, sourceOffset,
				topic, Schema.STRING_SCHEMA, message.getJMSMessageID(),
				JmsMessage.SCHEMA_V1, new JmsMessage( message ).toStructV1()
		);
	}
}
