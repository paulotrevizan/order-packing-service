package pt.paulotrevizan.orderpacking.support;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;

@TestConfiguration(proxyBeanMethods = false)
public class CarrierTestConfiguration {

  @Bean
  CarrierStub carrierStub() {
    return new CarrierStub();
  }

  @Bean
  DynamicPropertyRegistrar carrierBaseUrl(CarrierStub carrierStub) {
    return registry ->
        registry.add("spring.http.serviceclient.carrier.base-url", carrierStub::baseUrl);
  }
}
