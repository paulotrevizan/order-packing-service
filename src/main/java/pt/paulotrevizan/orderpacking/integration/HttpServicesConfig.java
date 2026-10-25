package pt.paulotrevizan.orderpacking.integration;

import java.net.http.HttpClient;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.service.registry.ImportHttpServices;
import pt.paulotrevizan.orderpacking.integration.carrier.CarrierClient;

/**
 * HTTP service clients for the external systems. Base URLs come from {@code
 * spring.http.serviceclient.<group>}; timeouts from {@code spring.http.clients}.
 */
@Configuration(proxyBeanMethods = false)
@ImportHttpServices(group = "carrier", types = CarrierClient.class)
class HttpServicesConfig {

  /** Using HTTP/1.1, as the JDK client default HTTP/2 upgrade is dropped by many servers. */
  @Bean
  ClientHttpRequestFactoryBuilder<?> clientHttpRequestFactoryBuilder() {
    return ClientHttpRequestFactoryBuilder.jdk()
        .withHttpClientCustomizer(builder -> builder.version(HttpClient.Version.HTTP_1_1));
  }
}
