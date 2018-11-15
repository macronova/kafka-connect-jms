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

import org.junit.Assert;
import org.junit.Test;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Basic tests for receiving messages from JMS topic.
 */
public class SourceTopicTest extends BaseSourceTest {
	@Override
	protected boolean isQueueTest() {
		return false;
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
		final String messageSelector = "MySequence = 1";
		// Register durable subscriber so that messages remain on the topic once we start source connector.
		registerDurableSubscription( messageSelector );
		final Map<String, String> configuration = configurationJndi();
		configuration.put( "jms.selector", messageSelector ); // We should only receive second message.
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
		// Register durable subscriber so that messages remain on the topic once we start source connector.
		registerDurableSubscription( null );
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
}
