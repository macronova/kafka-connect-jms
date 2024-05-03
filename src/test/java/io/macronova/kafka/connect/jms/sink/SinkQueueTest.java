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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Session;

import org.junit.Assert;
import org.junit.Test;

import io.macronova.kafka.connect.jms.common.JmsConverter;
import io.macronova.kafka.connect.jms.common.StandardJmsConverter;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import io.macronova.kafka.connect.jms.CustomJmsDialect;

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
 * Basic tests for sending Kafka Connect records to JMS queue.
 */
public class SinkQueueTest extends BaseSinkTest {
	@Override
	protected boolean isQueueTest() {
		return true;
	}

	@Test
	public void testMessageDeliveryJndi() throws Exception {
		checkMessageDelivery(
				configurationJndi(), new String[] { "Bye bye, JMS!", "Hello, Kafka!" }, new Object[] { "msg-1", 1L }
		);
	}

	@Test
	public void testMessageDeliveryWithCustomHeadersJndi() throws Exception {
		checkMessageDeliveryWithCustomHeaders( configurationJndi() );
	}

	@Test
	public void testMessageDeliveryDirect() throws Exception {
		checkMessageDelivery(
				configurationDirect(), new String[] { "Bye bye, JMS!", "Hello, Kafka!" }, new Object[] { 13, null }
		);
	}

	@Test
	public void testMessageDeliveryWithCustomHeadersDirect() throws Exception {
		checkMessageDeliveryWithCustomHeaders( configurationDirect() );
	}

	private void checkMessageDelivery(Map<String, String> configuration, String[] payload, Object[] id) throws Exception {
		List<SinkRecord> records = new LinkedList<>();
		int partition = 0;
		int offset = 0;
		for ( int i = 0; i < payload.length; ++i ) {
			records.add( createSinkRecord( ++partition, ++offset, id[i], payload[i] ) );
		}
		runSink( configuration, records, 1 );

		final Message[] messages = broker.getDestination( new ActiveMQQueue( jmsQueue() ) ).browse();
		Assert.assertEquals( payload.length, messages.length );
		for ( int i = 0; i < messages.length; ++i ) {
			Assert.assertEquals( id[i], messages[i].getProperty( "KafkaKey" ) );
			Assert.assertEquals( payload[i], ( (ActiveMQTextMessage) messages[i] ).getText() );
			Assert.assertEquals( kafkaTopic(), messages[i].getProperty( "KafkaTopic" ) );
			Assert.assertNotNull( messages[i].getProperty( "KafkaPartition" ) );
			Assert.assertNotNull( messages[i].getProperty( "KafkaOffset" ) );
		}
	}

	private void checkMessageDeliveryWithCustomHeaders(Map<String, String> configuration) throws Exception {
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

		List<SinkRecord> records = new LinkedList<>();
		records.add( createSinkRecord( 0, 0, key, payload, headers ) );
		runSink( configuration, records, 1 );

		final Message[] messages = broker.getDestination( new ActiveMQQueue( jmsQueue() ) ).browse();
		Assert.assertEquals( 1, messages.length );
		final ActiveMQTextMessage message = (ActiveMQTextMessage) messages[0];
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

	@Test
	public void testReconnectionSuccessful() throws Exception {
		final Integer retries = 10;
		final Integer failedAttempts = 3;
		final JmsSinkTask task = startRetryTask( retries );
		task.put( Arrays.asList( createSinkRecord( 0, 0, "Key1", "Value1" ) ) );
		stopBroker();
		for ( int i = 0; i < failedAttempts; ++i ) {
			try {
				task.put( Arrays.asList( createSinkRecord( 0, 1, "Key2", "Value2" ) ) );
			}
			catch (Exception e) {
				Assert.assertTrue( e instanceof RetriableException );
			}
		}
		Assert.assertEquals( retries - failedAttempts, task.getRemainingRetries() );
		startBroker();
		task.put( Arrays.asList( createSinkRecord( 0, 1, "Key2", "Value2" ) ) );
		task.stop();
	}

	@Test
	public void testRetryFailure() throws Exception {
		final Integer retries = 3;
		final JmsSinkTask task = startRetryTask( retries );
		task.put( Arrays.asList( createSinkRecord( 0, 0, "Key1", "Value1" ) ) );
		stopBroker();
		// Exceed number of retries.
		for ( int i = 0; i < retries; ++i ) {
			try {
				task.put( Arrays.asList( createSinkRecord( 0, 1, "Key2", "Value2" ) ) );
				Assert.fail( "Exception expected." );
			}
			catch (Exception e) {
				Assert.assertTrue( e instanceof RetriableException );
			}
		}
		try {
			task.put( Arrays.asList( createSinkRecord( 0, 1, "Key2", "Value2" ) ) );
			Assert.fail( "Exception expected." );
		}
		catch (Exception e) {
			Assert.assertTrue( e instanceof ConnectException );
		}
		task.stop();
	}

	private JmsSinkTask startRetryTask(Integer maxRetries) throws Exception {
		final Map<String, String> configuration = configurationJndi();
		configuration.put( "max.retries", maxRetries.toString() );
		return startRetryTask( configuration );
	}

	private JmsSinkTask startRetryTask(Map<String, String> configuration) throws Exception {
		final JmsSinkTask task = new JmsSinkTask();
		Field taskContext = task.getClass().getSuperclass().getDeclaredField( "context" );
		taskContext.setAccessible( true );
		taskContext.set( task, new NoOpSinkTaskContext() );
		task.start( configuration );
		return task;
	}

	private SinkRecord createSinkRecord(int partition, long offset, Object key, Object value, Iterable<Header> headers) {
		return new SinkRecord( kafkaTopic(), partition, null, key, null, value, offset, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
	}

	private SinkRecord createSinkRecord(int partition, long offset, Object key, Object value) {
		return createSinkRecord( partition, offset, key, value, null );
	}

	@Test
	public void testDialectFactoryCreated() throws Exception {
		final Map<String, String> configuration = configurationDirect();
		configuration.put( "jms.dialect.class", CustomJmsDialect.class.getName() );
		checkMessageDelivery(
				configuration, new String[] { "Bye bye, JMS!", "Hello, Kafka!" }, new Object[] { "msg-1", 1L }
		);
		Assert.assertTrue( CustomJmsDialect.hasCreatedFactory() );
	}

	@Test
	public void testDialectExceptionReported() throws Exception {
		final Map<String, String> configuration = configurationDirect();
		configuration.put( "max.retries", "3" );
		configuration.put( "jms.dialect.class", CustomJmsDialect.class.getName() );
		final JmsSinkTask task = startRetryTask( configuration );
		task.setConverter( new StandardJmsConverter() {
			@Override
			public javax.jms.Message recordToMessage(Session session, SinkRecord record) throws JMSException {
				throw new RuntimeException( "Failed!" );
			}
		} );
		try {
			task.put( Arrays.asList( createSinkRecord( 0, 0, "Key1", "Value1" ) ) );
			Assert.fail( "Exception expected." );
		}
		catch (Exception e) {
			Assert.assertTrue( CustomJmsDialect.wasExceptionReported() );
		}
		task.stop();
	}

	@Test
	public void testTransactionalSend() throws Exception {
		final JmsSinkTask task = startRetryTask( 0 );
		final AtomicInteger counter = new AtomicInteger( 0 );
		final int failMessageSeq = 2;
		final JmsConverter failingConverter = new StandardJmsConverter() {
			@Override
			public javax.jms.Message recordToMessage(Session session, SinkRecord record) throws JMSException {
				if ( counter.incrementAndGet() == failMessageSeq ) {
					// Fail while publishing second message.
					// As a result non of messages should be available for consumers.
					throw new RuntimeException( "Failed!" );
				}
				return super.recordToMessage( session, record );
			}
		};
		failingConverter.configure( configurationJndi() );
		task.setConverter( failingConverter );
		try {
			task.put(
					Arrays.asList(
							createSinkRecord( 0, 0, "Key1", "Value1" ), createSinkRecord( 0, 1, "Key2", "Value2" ),
							createSinkRecord( 0, 0, "Key3", "Value3" )
					)
			);
			Assert.fail( "Exception expected." );
		}
		catch (Exception e) {
			// Ignore.
		}
		final Message[] messages = broker.getDestination( new ActiveMQQueue( jmsQueue() ) ).browse();
		Assert.assertEquals( 0, messages.length );
		task.stop();
	}
}
