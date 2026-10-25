package pt.paulotrevizan.orderpacking.support;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.testcontainers.kafka.KafkaContainer;
import pt.paulotrevizan.orderpacking.common.config.OrderPackingKafkaProperties;

/**
 * Topics belong to the platform in production; only tests (and docker-compose) create the topic.
 */
@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestConfiguration {

  private static final int TOPIC_PARTITIONS = 10;

  @Bean
  NewTopic pickingEventsTopic(OrderPackingKafkaProperties properties) {
    return TopicBuilder.name(properties.topic()).partitions(TOPIC_PARTITIONS).build();
  }

  @Bean
  KafkaTestClient kafkaTestClient(
      KafkaContainer kafkaContainer,
      OrderPackingKafkaProperties properties,
      KafkaProperties kafkaProperties,
      KafkaListenerEndpointRegistry listenerRegistry) {
    return new KafkaTestClient(
        kafkaContainer.getBootstrapServers(),
        properties.topic(),
        kafkaProperties.getConsumer().getGroupId(),
        TOPIC_PARTITIONS,
        listenerRegistry);
  }
}
