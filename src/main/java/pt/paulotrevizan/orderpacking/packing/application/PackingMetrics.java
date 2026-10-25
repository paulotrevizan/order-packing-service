package pt.paulotrevizan.orderpacking.packing.application;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import pt.paulotrevizan.orderpacking.inbox.domain.FailureReason;

@Component
@RequiredArgsConstructor
class PackingMetrics {

  private static final String ORDERS = "order_packing_orders_total";
  private static final String CARRIER_CALLS = "order_packing_carrier_calls_total";

  private final MeterRegistry meterRegistry;

  void orderProcessed() {
    meterRegistry.counter(ORDERS, "outcome", "processed", "reason", "NONE").increment();
  }

  void orderFailed(FailureReason reason) {
    meterRegistry.counter(ORDERS, "outcome", "failed", "reason", reason.name()).increment();
  }

  void carrierCall(CarrierCallOutcome outcome) {
    meterRegistry
        .counter(CARRIER_CALLS, "outcome", outcome.name().toLowerCase(Locale.ROOT))
        .increment();
  }
}
