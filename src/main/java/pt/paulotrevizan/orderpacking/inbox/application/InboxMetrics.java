package pt.paulotrevizan.orderpacking.inbox.application;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import org.springframework.stereotype.Component;
import pt.paulotrevizan.orderpacking.inbox.domain.MessageType;
import pt.paulotrevizan.orderpacking.inbox.persistence.InboxMessageRepository;

@Component
public class InboxMetrics {

  private static final String MESSAGES = "order_packing_inbox_messages_total";
  private static final String INVALID_RECORDS = "order_packing_invalid_records_total";
  private static final String BACKLOG = "order_packing_inbox_backlog";

  private final MeterRegistry meterRegistry;

  InboxMetrics(
      MeterRegistry meterRegistry, InboxMessageRepository inboxMessageRepository, Clock clock) {
    this.meterRegistry = meterRegistry;

    // read from the database on every scrape, the same rows the reconciler would claim.
    Gauge.builder(BACKLOG, () -> inboxMessageRepository.countDueOrderCompleted(clock.instant()))
        .description("Due ORDER_COMPLETED records waiting for the reconciler")
        .register(meterRegistry);
  }

  public void invalidRecordSkipped() {
    meterRegistry.counter(INVALID_RECORDS).increment();
  }

  void messageStored(MessageType type) {
    countMessage(type, "stored");
  }

  void duplicateIgnored(MessageType type) {
    countMessage(type, "duplicate");
  }

  private void countMessage(MessageType type, String result) {
    meterRegistry.counter(MESSAGES, "type", type.name(), "result", result).increment();
  }
}
