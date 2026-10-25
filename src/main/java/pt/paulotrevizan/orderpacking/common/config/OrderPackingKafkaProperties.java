package pt.paulotrevizan.orderpacking.common.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "order-packing.kafka")
public record OrderPackingKafkaProperties(
    @NotBlank(message = "order-packing.kafka.topic is required") String topic) {}
