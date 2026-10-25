package pt.paulotrevizan.orderpacking.packing.application;

import java.time.Clock;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pt.paulotrevizan.orderpacking.inbox.domain.FailureReason;
import pt.paulotrevizan.orderpacking.inbox.domain.MessageType;
import pt.paulotrevizan.orderpacking.inbox.persistence.InboxMessageRepository;

/**
 * Records what a carrier call meant for the ORDER_COMPLETED message, one method by {@link
 * CarrierCallOutcome} ({@code failed} covers REJECTED and UNKNOWN), each in its own short
 * transaction after the call returned. Each method loads the message by id, because the one the
 * caller holds was committed before the call.
 */
@Service
@RequiredArgsConstructor
class LabelOutcomes {

  private final InboxMessageRepository inboxMessageRepository;
  private final PackedOrders packedOrders;
  private final MessageFailure messageFailure;
  private final RetryPolicy retryPolicy;
  private final Clock clock;

  @Transactional
  void created(UUID messageId, String labelId) {
    var now = clock.instant();
    var message = inboxMessageRepository.findById(messageId).orElseThrow();
    var rowMessages =
        inboxMessageRepository.findByOrderIdAndType(
            message.getOrderId(), MessageType.ROW_COMPLETED);

    packedOrders.markLabelled(message.getOrderId(), labelId, now);
    message.markProcessed(now);
    for (var rowMessage : rowMessages) {
      rowMessage.markProcessed(now);
    }
  }

  /**
   * The carrier certainly did not create the label, so the message goes back to WAITING and is
   * processed again later.
   */
  @Transactional
  void refused(UUID messageId) {
    var now = clock.instant();
    var message = inboxMessageRepository.findById(messageId).orElseThrow();

    message.markWaiting(retryPolicy.nextAttemptAt(message.getAttempts() + 1, now), now);
  }

  @Transactional
  void failed(UUID messageId, FailureReason reason) {
    var message = inboxMessageRepository.findById(messageId).orElseThrow();

    messageFailure.fail(message, reason, clock.instant());
  }
}
