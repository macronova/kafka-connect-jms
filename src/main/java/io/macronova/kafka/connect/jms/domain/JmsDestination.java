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

import javax.jms.Destination;
import javax.jms.JMSException;

import io.macronova.kafka.connect.jms.util.JmsUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Domain object representing JMS destination.
 */
public class JmsDestination {
	public static final Schema SCHEMA_V1 = SchemaBuilder.struct().name( "JMSDestination" ).version( 1 )
			.field( "type", Schema.STRING_SCHEMA )
			.field( "name", Schema.STRING_SCHEMA )
			.optional()
			.build();

	private final String type;
	private final String name;

	public JmsDestination(Destination destination) throws JMSException {
		this.type = JmsUtils.destinationType( destination );
		this.name = JmsUtils.destinationName( destination );
	}

	public Struct toStructV1() {
		return new Struct( SCHEMA_V1 )
				.put( "type", type )
				.put( "name", name );
	}
}
