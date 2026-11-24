/*
 * Copyright 2018 Macronova.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.macronova.kafka.connect.jms.sink;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import io.confluent.connect.avro.AvroConverter;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Verify the Apache Avro output format of the sink connector (Confluent Avro converter and Schema Registry wire
 * format). An in-process mock schema registry is used, so no server is required.
 */
public class SinkAvroTest extends BaseSinkTest {
	private static final Schema SCHEMA = SchemaBuilder.struct().name( "io.macronova.test.AvroPayload" )
			.field( "booleanField", Schema.BOOLEAN_SCHEMA )
			.field( "int32Field", Schema.INT32_SCHEMA )
			.field( "int64Field", Schema.INT64_SCHEMA )
			.field( "float64Field", Schema.FLOAT64_SCHEMA )
			.field( "stringField", Schema.STRING_SCHEMA )
			.field( "bytesField", Schema.BYTES_SCHEMA )
			.field( "optionalField", Schema.OPTIONAL_STRING_SCHEMA )
			.build();

	@Override
	protected boolean isQueueTest() {
		return true;
	}

	@Test
	public void testAvroMessage() throws Exception {
		final String registry = "mock://" + UUID.randomUUID();
		final Map<String, String> configuration = configurationJndi();
		configuration.put( "jms.message.converter.output.format", "avro" );
		configuration.put( "schema.registry.url", registry );

		final Struct payload = new Struct( SCHEMA )
				.put( "booleanField", true )
				.put( "int32Field", 123 )
				.put( "int64Field", -123L )
				.put( "float64Field", 0.5D )
				.put( "stringField", "Hello, Kafka!" )
				.put( "bytesField", new byte[] { 1, 2, 3 } )
				.put( "optionalField", null );
		final SinkRecord record = new SinkRecord(
				kafkaTopic(), 0, null, null, SCHEMA, payload, 0, System.currentTimeMillis(), TimestampType.CREATE_TIME
		);
		runSink( configuration, Arrays.asList( record ), 1 );

		final Message[] messages = broker.getDestination( new ActiveMQQueue( jmsQueue() ) ).browse();
		Assert.assertEquals( 1, messages.length );
		Assert.assertTrue( messages[0] instanceof ActiveMQBytesMessage );
		final ActiveMQBytesMessage bytesMessage = (ActiveMQBytesMessage) messages[0];
		bytesMessage.reset();
		final byte[] serialized = new byte[(int) bytesMessage.getBodyLength()];
		bytesMessage.readBytes( serialized );
		Assert.assertEquals( "Confluent wire format starts with the magic byte.", 0, serialized[0] );

		// Decode it the way a consumer would, using the very same (mock) schema registry.
		final AvroConverter reader = new AvroConverter();
		reader.configure( Collections.singletonMap( "schema.registry.url", registry ), false );
		final SchemaAndValue decoded = reader.toConnectData( kafkaTopic(), serialized );
		final Struct struct = (Struct) decoded.value();
		Assert.assertEquals( "io.macronova.test.AvroPayload", decoded.schema().name() );
		Assert.assertEquals( true, struct.get( "booleanField" ) );
		Assert.assertEquals( 123, struct.get( "int32Field" ) );
		Assert.assertEquals( -123L, struct.get( "int64Field" ) );
		Assert.assertEquals( 0.5D, (Double) struct.get( "float64Field" ), 0.0001D );
		Assert.assertEquals( "Hello, Kafka!", struct.get( "stringField" ) );
		final Object bytes = struct.get( "bytesField" ); // Connect may represent BYTES as byte[] or ByteBuffer.
		final byte[] bytesValue = bytes instanceof java.nio.ByteBuffer ? java.nio.ByteBuffer.class.cast( bytes ).array() : (byte[]) bytes;
		Assert.assertArrayEquals( new byte[] { 1, 2, 3 }, bytesValue );
		Assert.assertNull( struct.get( "optionalField" ) );
	}
}
