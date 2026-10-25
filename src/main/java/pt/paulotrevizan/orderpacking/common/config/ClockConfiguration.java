package pt.paulotrevizan.orderpacking.common.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class ClockConfiguration {

  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }
}
