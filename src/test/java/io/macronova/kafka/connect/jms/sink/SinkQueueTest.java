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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import io.macronova.kafka.connect.jms.CustomJmsDialect;

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
	public void testMessageDeliveryDirect() throws Exception {
		checkMessageDelivery(
				configurationDirect(), new String[] { "Bye bye, JMS!", "Hello, Kafka!" }, new Object[] { 13, null }
		);
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

	private SinkRecord createSinkRecord(int partition, long offset, Object key, Object value) {
		return new SinkRecord( kafkaTopic(), partition, null, key, null, value, offset, System.currentTimeMillis(), TimestampType.CREATE_TIME );
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
