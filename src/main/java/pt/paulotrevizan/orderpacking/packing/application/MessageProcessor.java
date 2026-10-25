package pt.paulotrevizan.orderpacking.packing.application;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pt.paulotrevizan.orderpacking.inbox.domain.FailureReason;
import pt.paulotrevizan.orderpacking.inbox.domain.InboxMessage;
import pt.paulotrevizan.orderpacking.inbox.domain.MessageType;
import pt.paulotrevizan.orderpacking.inbox.persistence.InboxMessageRepository;

/**
 * Processes ORDER_COMPLETED messages, each in one short transaction. The claimed row stays locked
 * until it commits, so no other pod processes the same message.
 */
@Slf4j
@Service
@RequiredArgsConstructor
class MessageProcessor {

  private final InboxMessageRepository inboxMessageRepository;
  private final PackedOrders packedOrders;
  private final MessageFailure messageFailure;
  private final RetryPolicy retryPolicy;
  private final PackingProperties properties;
  private final Clock clock;

  /** Claims the next due message and processes it. Returns empty when none is due. */
  @Transactional
  Optional<InboxMessage> processNext() {
    var now = clock.instant();

    var nextMessage = inboxMessageRepository.claimDueOrderCompleted(now, 1).stream().findFirst();
    if (nextMessage.isPresent()) {
      process(nextMessage.get(), now);
    }

    return nextMessage;
  }

  /**
   * A label request with no outcome after the label timeout means that the pod died mid-call. The
   * label may exist, so the order needs to be checked by a person, never retried in another call.
   */
  @Transactional
  void failAbandonedLabelRequests() {
    var now = clock.instant();
    var abandonedMessages =
        inboxMessageRepository.claimAbandonedLabelRequests(
            now.minus(properties.labelTimeout()), properties.batchSize());

    for (var message : abandonedMessages) {
      messageFailure.fail(message, FailureReason.LABEL_OUTCOME_UNKNOWN, now);
    }
  }

  private void process(InboxMessage message, Instant now) {
    var rowMessages =
        inboxMessageRepository.findByOrderIdAndType(
            message.getOrderId(), MessageType.ROW_COMPLETED);
    var attemptsMade = message.getAttempts() + 1;

    if (hasAllRows(message, rowMessages)) {
      packedOrders.createIfAbsent(message.getOrderId(), rowMessages, now);
      message.markLabelRequested(now);
      log.info("Order complete, label requested: orderId={}", message.getOrderId());
    } else if (retryPolicy.isExhausted(attemptsMade)) {
      messageFailure.fail(message, FailureReason.MISSING_ROWS, now);
    } else {
      message.markWaiting(retryPolicy.nextAttemptAt(attemptsMade, now), now);
      log.debug(
          "Order incomplete, waiting: orderId={}, attempts={}", message.getOrderId(), attemptsMade);
    }
  }

  // the row numbers received are exactly 1..expectedRows
  private boolean hasAllRows(InboxMessage message, List<InboxMessage> rowMessages) {
    var expected = IntStream.rangeClosed(1, message.getExpectedRows()).boxed().toList();
    var received =
        rowMessages.stream().map(InboxMessage::getRowNumber).distinct().sorted().toList();

    return received.equals(expected);
  }
}
