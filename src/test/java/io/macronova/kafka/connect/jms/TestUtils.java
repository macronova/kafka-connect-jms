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

public abstract class TestUtils {
	/**
	 * Useful test method allowing to efficiently wait for action executed by another thread to complete.
	 * Adopted from Apache Kafka tests.
	 *
	 * @param testCondition Test condition.
	 * @param maxWaitMs Maximum time to wait for condition to be satisfied.
	 * @param conditionDetails Text appended to assertion error.
	 * @throws InterruptedException Indicates that sleep was interrupted.
	 */
	public static void waitForCondition(final TestCondition testCondition, final long maxWaitMs,
										String conditionDetails) throws InterruptedException {
		final long startTime = System.currentTimeMillis();
		boolean testConditionMet = false;
		while ( !( testConditionMet = testCondition.conditionMet() ) && ( ( System.currentTimeMillis() - startTime ) < maxWaitMs ) ) {
			Thread.sleep( Math.min( maxWaitMs, 100L ) );
		}
		if ( ! testConditionMet ) {
			conditionDetails = conditionDetails != null ? conditionDetails : "";
			throw new AssertionError( "Condition not met within timeout " + maxWaitMs + ". " + conditionDetails );
		}
	}
}
