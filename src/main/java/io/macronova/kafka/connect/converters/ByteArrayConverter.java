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
package io.macronova.kafka.connect.converters;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

// TODO: Travis CI. Publish on GitHub and Maven. Other status icons like: https://github.com/lettuce-io/lettuce-core.
// TODO: https://docs.confluent.io/current/confluent-hub/contributing.html?_ga=2.100470750.1982671614.1535396002-1114038087.1529070633
// TODO: Macronova logo in docs folder. https://d1o50x50snmhul.cloudfront.net/wp-content/uploads/2018/03/26151541/gettyimages-584484540-800x533.jpg
/**
 * Simple byte array converter for Kafka Connect.
 */
public class ByteArrayConverter implements Converter {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		if ( value != null && ! ( value instanceof byte[] ) ) {
			throw new DataException( "Incompatible value type: " + value.getClass() + "." );
		}
		return (byte[]) value;
	}

	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		return new SchemaAndValue( Schema.OPTIONAL_BYTES_SCHEMA, value );
	}
}
