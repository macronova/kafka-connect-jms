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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Domain object representing typed JMS property value.
 */
public class PropertyValue {
	public static final Schema SCHEMA_V1 = SchemaBuilder.struct().name( "PropertyValue" ).version( 1 )
			.field( "type", Schema.STRING_SCHEMA )
			.field( "boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA ) // Populated only for boolean value.
			.field( "byte", Schema.OPTIONAL_INT8_SCHEMA ) // Populated only for byte value.
			.field( "short", Schema.OPTIONAL_INT16_SCHEMA ) // Populated only for short value.
			.field( "integer", Schema.OPTIONAL_INT32_SCHEMA ) // Populated only for integer value.
			.field( "long", Schema.OPTIONAL_INT64_SCHEMA ) // Populated only for long value.
			.field( "float", Schema.OPTIONAL_FLOAT32_SCHEMA ) // Populated only for float value.
			.field( "double", Schema.OPTIONAL_FLOAT64_SCHEMA ) // Populated only for double value.
			.field( "string", Schema.OPTIONAL_STRING_SCHEMA ) // Populated only for string value.
			.build();

	private final String type;
	private final Object value;

	public PropertyValue(Object value) {
		this.value = value;
		if ( value instanceof Boolean ) {
			type = "boolean";
		}
		else if ( value instanceof Byte ) {
			type = "byte";
		}
		else if ( value instanceof Short ) {
			type = "short";
		}
		else if ( value instanceof Integer ) {
			type = "integer";
		}
		else if ( value instanceof Long ) {
			type = "long";
		}
		else if ( value instanceof Float ) {
			type = "float";
		}
		else if ( value instanceof Double ) {
			type = "double";
		}
		else if ( value instanceof String ) {
			type = "string";
		}
		else {
			throw new UnsupportedOperationException( "Unsupported value type: " + value.getClass().getName() + "." );
		}
	}

	public Struct toStructV1() {
		return new Struct( SCHEMA_V1 )
				.put( "type", type )
				.put( type, value );
	}
}
