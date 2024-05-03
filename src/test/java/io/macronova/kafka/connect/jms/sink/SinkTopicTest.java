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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.junit.Assert;
import org.junit.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.kafka.common.record.TimestampType;
import io.macronova.kafka.connect.jms.TestCondition;
import io.macronova.kafka.connect.jms.TestUtils;
import org.apache.kafka.connect.sink.SinkRecord;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.BYTES_SCHEMA;
import static org.apache.kafka.connect.data.Schema.FLOAT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT16_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT8_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.map;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

/**
 * Basic tests for sending Kafka Connect records to JMS topic.
 */
public class SinkTopicTest extends BaseSinkTest {
	@Override
	protected boolean isQueueTest() {
		return false;
	}

	@Test
	public void testMessageDeliveryJndi() throws Exception {
		checkMessageDelivery(
				configurationJndi(), new String[][] {
						new String[] { "msg-1", "Bye bye, JMS!" },
						new String[] { "msg-2", "Hello, Kafka!" }
				}
		);
	}

	@Test
	public void testMessageDeliveryWithCustomHeadersJndi() throws Exception {
		checkMessageDeliveryWithCustomHeaders( configurationJndi() );
	}

	@Test
	public void testMessageDeliveryDirect() throws Exception {
		checkMessageDelivery(
				configurationDirect(), new String[][] {
						new String[] { "msg-1", "Bye bye, JMS!" },
						new String[] { "msg-2", "Hello, Kafka!" }
				}
		);
	}

	@Test
	public void testMessageDeliveryWithCustomHeadersDirect() throws Exception {
		checkMessageDeliveryWithCustomHeaders( configurationDirect() );
	}

	private void checkMessageDelivery(Map<String, String> configuration, String[][] msgs) throws Exception {
		final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory( providerUrl() );
		final Connection connection = connectionFactory.createConnection();
		connection.start();
		final Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
		final MessageConsumer consumer = session.createConsumer( session.createTopic( jmsTopic() ) );
		final List<Message> result = new LinkedList<>();

		// Create consumer to capture messages sent to JMS topic.
		consumer.setMessageListener( new MessageListener() {
			@Override
			public void onMessage(Message message) {
				result.add( message );
			}
		} );

		final List<SinkRecord> records = new LinkedList<>();
		int partition = 0;
		int offset = 0;
		for ( String[] msg : msgs ) {
			records.add( new SinkRecord( kafkaTopic(), ++partition, null, msg[0], null, msg[1], ++offset, System.currentTimeMillis(), TimestampType.CREATE_TIME ) );
		}
		runSink( configuration, records, 1 );

		TestUtils.waitForCondition( new TestCondition() {
			@Override
			public boolean conditionMet() {
				return msgs.length == result.size();
			}
		}, 5000, "Messages did not arrive." );

		consumer.close();
		session.close();
		connection.close();

		Assert.assertEquals( msgs.length, result.size() );
		for ( int i = 0; i < result.size(); ++i ) {
			Assert.assertEquals( msgs[i][0], result.get( i ).getStringProperty( "KafkaKey" ) );
			Assert.assertEquals( msgs[i][1], ( (TextMessage) result.get( i ) ).getText() );
			Assert.assertEquals( kafkaTopic(), result.get( i ).getStringProperty( "KafkaTopic" ) );
			Assert.assertTrue( result.get( i ).getIntProperty( "KafkaPartition" ) >= 0 );
			Assert.assertTrue( result.get( i ).getLongProperty( "KafkaOffset" ) >= 0 );
		}
	}

	private void checkMessageDeliveryWithCustomHeaders(Map<String, String> configuration) throws Exception {
		final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory( providerUrl() );
		final Connection connection = connectionFactory.createConnection();
		connection.start();
		final Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
		final MessageConsumer consumer = session.createConsumer( session.createTopic( jmsTopic() ) );
		final List<Message> result = new LinkedList<>();

		// Create consumer to capture messages sent to JMS topic.
		consumer.setMessageListener( new MessageListener() {
			@Override
			public void onMessage(Message message) {
				result.add( message );
			}
		} );

		final String key = "key";
		final String payload = "payload";

		final byte[] exampleByteArray = "test".getBytes( UTF_8 );

		final Map<String,String> exampleMap = new HashMap<>();
		exampleMap.put( "MapKey", "MapValue" );

		final Struct exampleStruct = new Struct( struct().field( "testField", STRING_SCHEMA ).build() )
				.put( "testField", "testFieldValue" );

		final Headers headers = new ConnectHeaders();
		headers.add( "ByteHeaderExample", Byte.MAX_VALUE, INT8_SCHEMA );
		headers.add( "ShortHeaderExample", Short.MAX_VALUE, INT16_SCHEMA );
		headers.add( "IntegerHeaderExample", Integer.MAX_VALUE, INT32_SCHEMA );
		headers.add( "LongHeaderExample", Long.MAX_VALUE, INT64_SCHEMA );
		headers.add( "FloatHeaderExample", Float.MAX_VALUE, FLOAT32_SCHEMA );
		headers.add( "DoubleHeaderExample", Double.MAX_VALUE, FLOAT64_SCHEMA );
		headers.add( "BooleanHeaderExample", true, BOOLEAN_SCHEMA );
		headers.add( "StringHeaderExample", "This is a test", STRING_SCHEMA );
		headers.add( "BytesHeaderExample", exampleByteArray, BYTES_SCHEMA );
		headers.add( "MapHeaderExample", exampleMap, map( STRING_SCHEMA, STRING_SCHEMA ) );
		headers.add( "StructHeaderExample", exampleStruct, exampleStruct.schema() );

		final List<SinkRecord> records = new LinkedList<>();
		records.add( new SinkRecord( kafkaTopic(), 0, null, key, null, payload, 0, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers ) );
		runSink( configuration, records, 1 );

		TestUtils.waitForCondition( new TestCondition() {
			@Override
			public boolean conditionMet() {
				return result.size() == 1;
			}
		}, 5000, "Message did not arrive." );

		consumer.close();
		session.close();
		connection.close();

		Assert.assertEquals( 1, result.size() );
		final ActiveMQTextMessage message = (ActiveMQTextMessage) result.get( 0 );
		Assert.assertEquals( key, message.getProperty( "KafkaKey" ) );
		Assert.assertEquals( payload, message.getText() );
		Assert.assertEquals( kafkaTopic(), message.getProperty( "KafkaTopic" ) );
		Assert.assertNotNull( message.getProperty( "KafkaPartition" ) );
		Assert.assertNotNull( message.getProperty( "KafkaOffset" ) );

		Assert.assertEquals( Byte.MAX_VALUE, message.getByteProperty( "ByteHeaderExample" ) );
		Assert.assertEquals( Short.MAX_VALUE, message.getShortProperty( "ShortHeaderExample" ) );
		Assert.assertEquals( Long.MAX_VALUE, message.getLongProperty( "LongHeaderExample" ) );
		Assert.assertEquals( Float.MAX_VALUE, message.getFloatProperty( "FloatHeaderExample" ), 0.0 );
		Assert.assertEquals( Double.MAX_VALUE, message.getDoubleProperty( "DoubleHeaderExample" ), 0.0 );
		Assert.assertTrue( message.getBooleanProperty( "BooleanHeaderExample" ) );
		Assert.assertEquals( "This is a test", message.getStringProperty( "StringHeaderExample" ) );
		Assert.assertEquals( exampleByteArray.toString(), message.getStringProperty( "BytesHeaderExample" ) );
		Assert.assertEquals( exampleMap.toString(), message.getStringProperty( "MapHeaderExample" ) );
		Assert.assertEquals( exampleStruct.toString(), message.getStringProperty( "StructHeaderExample" ) );
	}
}
