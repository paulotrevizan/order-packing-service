package pt.paulotrevizan.orderpacking.packing.application;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
class RetryPolicy {

  private static final double MULTIPLIER = 2.0;

  private final PackingProperties properties;

  boolean isExhausted(int attemptsMade) {
    return attemptsMade >= properties.maxAttempts();
  }

  Instant nextAttemptAt(int attemptsMade, Instant now) {
    var exponential =
        properties.initialBackoff().toMillis() * Math.pow(MULTIPLIER, attemptsMade - 1);
    var capped = Math.min(exponential, properties.maxBackoff().toMillis());
    var jitter = (2 * ThreadLocalRandom.current().nextDouble() - 1) * properties.jitter();

    return now.plus(Duration.ofMillis(Math.round(capped * (1 + jitter))));
  }
}
