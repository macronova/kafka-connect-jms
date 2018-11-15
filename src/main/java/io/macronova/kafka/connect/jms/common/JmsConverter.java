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
package io.macronova.kafka.connect.jms.common;

import java.util.Map;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Convert Kafka Connect records to JMS messages and vice-versa.
 */
public interface JmsConverter {
	/**
	 * Configure JMS message converter.
	 *
	 * @param properties Configuration properties.
	 */
	void configure(Map<String, String> properties);

	/**
	 * Convert sink record to JMS message.
	 *
	 * @param session Active JMS session.
	 * @param record Connect sink record.
	 * @return JMS message
	 * @throws JMSException Report error.
	 */
	Message recordToMessage(Session session, SinkRecord record) throws JMSException;

	/**
	 * Convert JMS message to source record.
	 *
	 * @param message JMS message.
	 * @param topic Target Kafka topic.
	 * @param sourcePartition Target partition.
	 * @param sourceOffset Target offset.
	 * @return Connect source record.
	 * @throws JMSException Report error.
	 */
	SourceRecord messageToRecord(Message message, String topic, Map<String, ?> sourcePartition,
								 Map<String, ?> sourceOffset) throws JMSException;
}
