package pt.paulotrevizan.orderpacking.inbox.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import pt.paulotrevizan.orderpacking.inbox.application.InboxMetrics;

@Slf4j
@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
class ListenerConfiguration implements KafkaListenerConfigurer {

  private static final long INITIAL_RETRY_INTERVAL_MS = 1_000;
  private static final double RETRY_MULTIPLIER = 2.0;
  private static final long MAX_RETRY_INTERVAL_MS = 30_000;

  private final LocalValidatorFactoryBean validator;

  @Override
  public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
    registrar.setValidator(validator);
  }

  /**
   * Deserialization and validation errors are not retryable for {@link DefaultErrorHandler}, so an
   * invalid record is logged, counted and skipped at once. Anything else (for example, the database
   * is down) is retried forever and never acknowledged, so the record is not lost.
   */
  @Bean
  DefaultErrorHandler kafkaErrorHandler(InboxMetrics metrics) {
    ConsumerRecordRecoverer skipInvalidRecord =
        (record, exception) -> {
          // the position is enough to read it again from Kafka while the topic retains it.
          log.error(
              "Invalid record skipped: position={}-{}@{}, cause={}",
              record.topic(),
              record.partition(),
              record.offset(),
              NestedExceptionUtils.getMostSpecificCause(exception).getClass().getSimpleName());
          metrics.invalidRecordSkipped();
        };

    var backOff = new ExponentialBackOff(INITIAL_RETRY_INTERVAL_MS, RETRY_MULTIPLIER);
    backOff.setMaxInterval(MAX_RETRY_INTERVAL_MS);

    var errorHandler = new DefaultErrorHandler(skipInvalidRecord, backOff);
    // with MANUAL_IMMEDIATE, a skipped record is only committed if this is set
    // otherwise the offset waits for the next valid record on the partition.
    errorHandler.setCommitRecovered(true);

    return errorHandler;
  }
}
