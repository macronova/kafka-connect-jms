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
package io.macronova.kafka.connect.jms.util;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Version {
	private static final Logger log = LoggerFactory.getLogger( Version.class );
	private static String version = "unknown";

	static {
		try {
			final Properties props = new Properties();
			props.load( Version.class.getResourceAsStream( "/kafka-connect-jms-version.properties" ) );
			version = props.getProperty( "version", version ).trim();
		}
		catch (Exception e) {
			log.warn( "Error while loading version: " + e.getMessage() + ".", e );
		}
	}

	public static String getVersion() {
		return version;
	}
}