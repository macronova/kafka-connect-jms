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

import org.junit.Assert;
import org.junit.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.kafka.common.record.TimestampType;
import io.macronova.kafka.connect.jms.TestCondition;
import io.macronova.kafka.connect.jms.TestUtils;
import org.apache.kafka.connect.sink.SinkRecord;

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
	public void testMessageDeliveryDirect() throws Exception {
		checkMessageDelivery(
				configurationDirect(), new String[][] {
						new String[] { "msg-1", "Bye bye, JMS!" },
						new String[] { "msg-2", "Hello, Kafka!" }
				}
		);
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
}
