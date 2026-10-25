package pt.paulotrevizan.orderpacking.support;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

/**
 * Full application context against Testcontainers PostgreSQL and Kafka, with WireMock as the
 * carrier. Every scenario test uses the same configuration, so the context and the containers are
 * shared between them.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest
@ActiveProfiles("test")
@Import({
  TestcontainersConfiguration.class,
  KafkaTestConfiguration.class,
  CarrierTestConfiguration.class,
  ScenarioDatabase.class,
  ScenarioMetrics.class
})
public @interface IntegrationTest {}
