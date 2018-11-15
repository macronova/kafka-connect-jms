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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.junit.Assert;
import org.junit.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Verify structure of Kafka Connect source records generated from various JMS message types.
 */
public class SourceMessageTypeTest extends BaseSourceTest {
	@Override
	protected boolean isQueueTest() {
		return true;
	}

	@Test
	public void testMapMessage() throws Exception {
		final Map<String, Object> map = new HashMap<>();
		map.put( "key1", "value1" );
		map.put( "key2", Boolean.TRUE );
		map.put( "key3", -0.1F );
		map.put( "key4", 2.12D );
		map.put( "key5", (byte) 3 );
		map.put( "key6", (short) 5 );
		map.put( "key7", -13 );
		map.put( "key8", 145000L );
		final SourceRecord record = captureSourceRecord( configurationJndi(), new JmsMessageFactory() {
			@Override
			public Message createMessage(Session session) throws JMSException {
				final MapMessage message = session.createMapMessage();
				for ( Map.Entry<String, Object> value : map.entrySet() ) {
					message.setObject( value.getKey(), value.getValue() );
				}
				return message;
			}
		} );
		Assert.assertTrue( record.value() instanceof Struct );
		final Struct struct = (Struct) record.value();
		Assert.assertEquals( "map", struct.getString( "type" ) );
		assertPropertyValue( (Struct) struct.getMap( "payloadMap" ).get( "key1" ), "string", "value1" );
		assertPropertyValue( (Struct) struct.getMap( "payloadMap" ).get( "key2" ), "boolean", Boolean.TRUE );
		assertPropertyValue( (Struct) struct.getMap( "payloadMap" ).get( "key3" ), "float", -0.1F );
		assertPropertyValue( (Struct) struct.getMap( "payloadMap" ).get( "key4" ), "double", 2.12D );
		assertPropertyValue( (Struct) struct.getMap( "payloadMap" ).get( "key5" ), "byte", (byte) 3 );
		assertPropertyValue( (Struct) struct.getMap( "payloadMap" ).get( "key6" ), "short", (short) 5 );
		assertPropertyValue( (Struct) struct.getMap( "payloadMap" ).get( "key7" ), "integer", -13 );
		assertPropertyValue( (Struct) struct.getMap( "payloadMap" ).get( "key8" ), "long", 145000L );
	}

	@Test
	public void testBytesMessage() throws Exception {
		final byte[] bytes = new byte[] { 3, 4, -1 };
		final SourceRecord record = captureSourceRecord( configurationJndi(), new JmsMessageFactory() {
			@Override
			public Message createMessage(Session session) throws JMSException {
				final BytesMessage message = session.createBytesMessage();
				message.writeBytes( bytes );
				return message;
			}
		} );
		Assert.assertTrue( record.value() instanceof Struct );
		final Struct struct = (Struct) record.value();
		Assert.assertEquals( "bytes", struct.getString( "type" ) );
		Assert.assertArrayEquals( bytes, struct.getBytes( "payloadBytes" ) );
	}

	private SourceRecord captureSourceRecord(Map<String, String> configuration, JmsMessageFactory factory) throws Exception {
		final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory( providerUrl() );
		final Connection connection = connectionFactory.createConnection();
		connection.start();
		final Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
		final Destination destination = session.createQueue( jmsQueue() );
		final MessageProducer producer = session.createProducer( destination );

		producer.send( factory.createMessage( session ) );

		producer.close();
		session.close();
		connection.close();

		final List<SourceRecord> result = runSource( configuration, 1 );
		Assert.assertEquals( 1, result.size() );
		return result.get( 0 );
	}

	private void assertPropertyValue(Struct propertyValue, String type, Object value) {
		Assert.assertEquals( type, propertyValue.getString( "type" ) );
		Assert.assertEquals( value, propertyValue.get( type ) );
	}

	private interface JmsMessageFactory {
		Message createMessage(Session session) throws JMSException;
	}
}
