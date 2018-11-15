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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;

import org.junit.Assert;
import org.junit.Test;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import io.macronova.kafka.connect.jms.CustomJmsDialect;
import io.macronova.kafka.connect.jms.common.JmsConverter;
import io.macronova.kafka.connect.jms.common.StandardJmsConverter;

/**
 * Basic tests for receiving messages from JMS queue.
 */
public class SourceQueueTest extends BaseSourceTest {
	@Override
	protected boolean isQueueTest() {
		return true;
	}

	@Test
	public void testMessagePollJndi() throws Exception {
		checkMessagePoll( configurationJndi(), new String[] { "Bye bye, JMS!", "Hello, Kafka!", "NoSQL is great..." } );
	}

	@Test
	public void testMessagePollDirect() throws Exception {
		checkMessagePoll( configurationDirect(), new String[] { "Bye bye, JMS!", "Hello, Kafka!", "NoSQL is great..." } );
	}

	@Test
	public void testMessageSelector() throws Exception {
		final Map<String, String> configuration = configurationJndi();
		configuration.put( "jms.selector", "MySequence = 1" ); // We should only receive second message.
		sendMessages( new String[] { "Bye bye, JMS!", "Hello, Kafka!", "NoSQL is great..." }, true );
		final List<SourceRecord> result = runSource( configuration, 1 );
		Assert.assertEquals( 1, result.size() );
		final Struct struct = (Struct) result.get( 0 ).value();
		Assert.assertEquals( "Hello, Kafka!", struct.getString( "payloadText" ) );
		Assert.assertTrue( struct.getMap( "properties" ).get( "MySequence" ) instanceof Struct );
		final Struct sequenceProperty = (Struct) struct.getMap( "properties" ).get( "MySequence" );
		Assert.assertEquals( 1, sequenceProperty.get( "integer" ) );
	}

	private void checkMessagePoll(Map<String, String> configuration, String[] msgs) throws Exception {
		sendMessages( msgs, false );

		final List<SourceRecord> result = runSource( configuration, 1 );

		Assert.assertEquals( msgs.length, result.size() );
		for ( int i = 0; i < result.size(); ++i ) {
			final Struct value = (Struct) result.get( i ).value();
			Assert.assertEquals( "text", value.getString( "type" ) );
			Assert.assertEquals( msgs[i], value.getString( "payloadText" ) );
			Assert.assertNotNull( value.getString( "messageId" ) );
			Assert.assertFalse( value.getBoolean( "redelivered" ) );
			Assert.assertEquals( value.getString( "messageId" ), result.get( i ).key() );
		}
	}

	@Test
	public void testReconnectionSuccessful() throws Exception {
		final String[] msgs = new String[] { "Bye bye, JMS!", "Hello, Kafka!", "NoSQL is great..." };
		final Map<String, String> configuration = configurationJndi();
		configuration.put( "max.retries", "3" );
		configuration.put( "retry.backoff.ms", "1000" );
		sendMessages( msgs, false );
		final JmsSourceTask task = new JmsSourceTask();
		task.start( configuration );
		task.setConverter( new TestConverter( BrokerDisruption.ONCE, msgs ) ); // Restart broker while polling messages.
		final List<SourceRecord> result = task.poll();
		Assert.assertEquals( 2, result.size() );
		for ( SourceRecord record : result ) {
			task.commitRecord( record );
		}
		task.stop();
	}

	@Test(expected = ConnectException.class)
	public void testRetryFailure() throws Exception {
		JmsSourceTask task = null;
		try {
			final Map<String, String> configuration = configurationJndi();
			configuration.put( "max.retries", "3" );
			configuration.put( "retry.backoff.ms", "1000" );
			sendMessages( new String[] { "Bye bye, JMS!", "Hello, Kafka!", "NoSQL is great..." }, false );
			task = new JmsSourceTask();
			task.start( configuration );
			task.setConverter( new TestConverter( BrokerDisruption.PERMANENT, null ) ); // Permanently shutdown JMS server.
			task.poll();
		}
		finally {
			if ( task != null ) {
				task.stop();
			}
		}
	}

	@Test
	public void testDialectFactoryCreated() throws Exception {
		final Map<String, String> configuration = configurationDirect();
		configuration.put( "jms.dialect.class", CustomJmsDialect.class.getName() );
		checkMessagePoll( configuration, new String[] { "Bye bye, JMS!", "Hello, Kafka!", "NoSQL is great..." } );
		Assert.assertTrue( CustomJmsDialect.hasCreatedFactory() );
	}

	@Test
	public void testDialectExceptionReported() throws Exception {
		final String[] msgs = new String[] { "Bye bye, JMS!", "Hello, Kafka!", "NoSQL is great..." };
		final Map<String, String> configuration = configurationDirect();
		configuration.put( "jms.dialect.class", CustomJmsDialect.class.getName() );
		configuration.put( "max.retries", "3" );
		configuration.put( "retry.backoff.ms", "1000" );
		sendMessages( msgs, false );
		final JmsSourceTask task = new JmsSourceTask();
		task.start( configuration );
		task.setConverter( new TestConverter( BrokerDisruption.ONCE, msgs ) ); // Restart broker while polling messages.
		final List<SourceRecord> result = task.poll();
		Assert.assertTrue( CustomJmsDialect.wasExceptionReported() );
		Assert.assertEquals( 2, result.size() );
		for ( SourceRecord record : result ) {
			task.commitRecord( record );
		}
		task.stop();
	}

	@Test
	public void testRedeliveryOnError() throws Exception {
		final String[] msgs = new String[] { "Bye bye, JMS!", "Hello, Kafka!", "NoSQL is great..." };
		final Map<String, String> configuration = configurationJndi();
		configuration.put( "max.poll.records", "3" );
		sendMessages( msgs, false );
		final JmsSourceTask task = new JmsSourceTask();
		final AtomicInteger counter = new AtomicInteger( 0 );
		final int failMessageSeq = 2;
		final JmsConverter failingConverter = new StandardJmsConverter() {
			@Override
			public SourceRecord messageToRecord(Message message, String topic, Map<String, ?> sourcePartition, Map<String, ?> sourceOffset) throws JMSException {
				if ( counter.incrementAndGet() == failMessageSeq ) {
					// Fail while polling second message. First and second will be redelivered.
					throw new RuntimeException( "Failed!" );
				}
				return super.messageToRecord( message, topic, sourcePartition, sourceOffset );
			}
		};
		task.start( configuration );
		task.setConverter( failingConverter );
		try {
			task.poll();
			Assert.fail( "Exception expected." );
		}
		catch (Exception e) {
			Assert.assertTrue( e instanceof ConnectException );
		}
		task.stop();
		final org.apache.activemq.command.Message[] messages = broker.getDestination( new ActiveMQQueue( jmsQueue() ) ).browse();
		int redeliveryHappened = 0;
		for ( org.apache.activemq.command.Message message : messages ) {
			if ( message.getRedeliveryCounter() > 0 ) {
				++redeliveryHappened;
			}
		}
		Assert.assertEquals( 2, redeliveryHappened ); // Two first messages have been redelivered.
	}

	enum BrokerDisruption {
		/**
		 * Restart broker once.
		 */
		ONCE,

		/**
		 * Permanently stop broker.
		 */
		PERMANENT,

		/**
		 * Do nothing.
		 */
		NONE
	}

	/**
	 * Stubbed converted used to simulate JMS broker failure. Depending on use-case, JMS server can be either
	 * restarted or turned permanently unavailable.
	 */
	private class TestConverter extends StandardJmsConverter {
		private BrokerDisruption action = null;
		private String[] msgs = null;

		private TestConverter(BrokerDisruption action, String[] msgs) {
			this.action = action;
			this.msgs = msgs;
		}

		@Override
		public SourceRecord messageToRecord(
				Message message, String topic, Map<String, ?> sourcePartition, Map<String, ?> sourceOffset)
				throws JMSException {
			final SourceRecord record = super.messageToRecord( message, topic, sourcePartition, sourceOffset );
			switch ( action ) {
				case PERMANENT:
					stopBroker();
					break;
				case ONCE:
					try {
						stopBroker();
						startBroker();
						// Implementation hack: We use non-persistent broker, so have to replay the messages.
						if ( msgs != null ) {
							sendMessages( msgs, false );
						}
						action = BrokerDisruption.NONE;
					}
					catch ( Exception e ) {
						throw new RuntimeException( "Test failed to bounce JMS server: " + e.getMessage() + ".", e );
					}
					break;
				case NONE:
					break;
			}
			return record;
		}
	}
}
