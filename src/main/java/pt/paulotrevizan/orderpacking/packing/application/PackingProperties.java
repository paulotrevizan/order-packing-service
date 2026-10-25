package pt.paulotrevizan.orderpacking.packing.application;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "order-packing.packing")
record PackingProperties(
    @NotNull(message = "order-packing.packing.poll-interval is required") Duration pollInterval,
    @Min(value = 1, message = "order-packing.packing.batch-size must be at least 1") int batchSize,
    @NotNull(message = "order-packing.packing.initial-backoff is required") Duration initialBackoff,
    @NotNull(message = "order-packing.packing.max-backoff is required") Duration maxBackoff,
    @DecimalMin(value = "0.0", message = "order-packing.packing.jitter must be between 0 and 1")
        @DecimalMax(value = "1.0", message = "order-packing.packing.jitter must be between 0 and 1")
        double jitter,
    @Min(value = 1, message = "order-packing.packing.max-attempts must be at least 1")
        int maxAttempts,
    @NotNull(message = "order-packing.packing.label-timeout is required") Duration labelTimeout) {}
