package pt.paulotrevizan.orderpacking.packing.application;

import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import pt.paulotrevizan.orderpacking.inbox.domain.FailureReason;
import pt.paulotrevizan.orderpacking.inbox.domain.InboxMessage;
import pt.paulotrevizan.orderpacking.inbox.domain.MessageType;
import pt.paulotrevizan.orderpacking.inbox.persistence.InboxMessageRepository;

/**
 * The ORDER_COMPLETED message is marked FAILED with the reason, and the order is logged at ERROR
 * (the alert) and counted. The original solution also sends it to a DLQ for replay. That path is
 * out of scope here.
 */
@Slf4j
@Service
@RequiredArgsConstructor
class MessageFailure {

  private final InboxMessageRepository inboxMessageRepository;
  private final PackingMetrics metrics;

  void fail(InboxMessage message, FailureReason reason, Instant now) {
    var receivedRows =
        inboxMessageRepository
            .findByOrderIdAndType(message.getOrderId(), MessageType.ROW_COMPLETED)
            .stream()
            .map(InboxMessage::getRowNumber)
            .sorted()
            .toList();

    message.markFailed(reason, now);

    log.error(
        "Order failed: orderId={}, reason={}, receivedRows={}",
        message.getOrderId(),
        reason,
        receivedRows);
    metrics.orderFailed(reason);
  }
}
