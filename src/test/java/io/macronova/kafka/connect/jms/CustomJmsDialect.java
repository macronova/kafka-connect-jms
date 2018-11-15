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
package io.macronova.kafka.connect.jms;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import io.macronova.kafka.connect.jms.common.JmsDialect;

/**
 * Custom JMS dialect for unit-testing purpose.
 */
public class CustomJmsDialect implements JmsDialect {
	private static final AtomicBoolean factoryCalled = new AtomicBoolean( false );
	private static final AtomicBoolean exceptionCalled = new AtomicBoolean( false );

	@Override
	public ConnectionFactory createConnectionFactory(Map<String, String> config) throws Exception {
		factoryCalled.set( true );
		return new ActiveMQConnectionFactory( config.get( "jms.url" ) );
	}

	@Override
	public boolean reconnectOnError(Exception e) {
		exceptionCalled.set( true );
		return false;
	}

	public static boolean hasCreatedFactory() {
		final boolean value = factoryCalled.get();
		factoryCalled.set( false );
		return value;
	}

	public static boolean wasExceptionReported() {
		final boolean value = exceptionCalled.get();
		exceptionCalled.set( false );
		return value;
	}
}
