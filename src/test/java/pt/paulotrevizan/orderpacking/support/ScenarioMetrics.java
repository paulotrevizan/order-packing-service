package pt.paulotrevizan.orderpacking.support;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

/** Reads counters as the metrics endpoint exposes them; a counter never incremented reads 0. */
@RequiredArgsConstructor
public class ScenarioMetrics {

  private final MeterRegistry meterRegistry;

  public double count(String name, String... tags) {
    return Optional.ofNullable(meterRegistry.find(name).tags(tags).counter())
        .map(Counter::count)
        .orElse(0.0);
  }
}
