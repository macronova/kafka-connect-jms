RabbitMQ dialect implementation allowing to instantiate custom connection factory:

```java
package io.macronova.kafka.connect.jms.common;

import java.util.Map;
import javax.jms.ConnectionFactory;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

public class RabbitMqDialect implements JmsDialect {
    @Override
    public ConnectionFactory createConnectionFactory(Map<String, String> config) throws Exception {
        final RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
        connectionFactory.setUsername( config.get( BaseConnectorConfig.JMS_USERNAME_CONFIG ) );
        connectionFactory.setPassword( config.get( BaseConnectorConfig.JMS_PASSWORD_CONFIG ) );
        connectionFactory.setVirtualHost( "/" );
        connectionFactory.setHost( config.get( "jms.dialect.host" ) );
        connectionFactory.setPort( Integer.valueOf( config.get( "jms.dialect.port" ) ) );
        return connectionFactory;
    }

    @Override
    public boolean reconnectOnError(Exception e) {
        return false;
    }
}

```