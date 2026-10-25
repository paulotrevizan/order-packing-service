package pt.paulotrevizan.orderpacking.inbox.application;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import pt.paulotrevizan.orderpacking.inbox.domain.InboxMessage;
import pt.paulotrevizan.orderpacking.inbox.messaging.PickingEvent;
import pt.paulotrevizan.orderpacking.inbox.persistence.InboxMessageRepository;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Service
@RequiredArgsConstructor
public class InboxWriter {

  private static final Set<String> DUPLICATE_CONSTRAINTS =
      Set.of("inbox_event_id_unique", "inbox_order_row_unique", "inbox_order_completed_unique");

  private final InboxMessageRepository inboxMessageRepository;
  private final TransactionTemplate transactionTemplate;
  private final JsonMapper jsonMapper;
  private final InboxMetrics metrics;
  private final Clock clock;

  public void store(PickingEvent event) {
    var message = toInboxMessage(event, clock.instant());

    try {
      transactionTemplate.executeWithoutResult(_ -> inboxMessageRepository.saveAndFlush(message));
    } catch (DataIntegrityViolationException exception) {
      if (!isDuplicate(exception)) {
        throw exception;
      }
      // a redelivery or a re-send is an expected outcome, not an error.
      log.info(
          "Duplicate {} ignored: orderId={}, eventId={}",
          event.type(),
          event.orderId(),
          event.eventId());
      metrics.duplicateIgnored(event.type());
      return;
    }

    log.debug("Stored {}: orderId={}, eventId={}", event.type(), event.orderId(), event.eventId());
    metrics.messageStored(event.type());
  }

  private InboxMessage toInboxMessage(PickingEvent event, Instant now) {
    var payload = jsonMapper.writeValueAsString(event);

    return switch (event.type()) {
      case ROW_COMPLETED ->
          InboxMessage.rowCompleted(
              event.eventId(), event.orderId(), event.rowNumber(), payload, now);
      case ORDER_COMPLETED ->
          InboxMessage.orderCompleted(
              event.eventId(), event.orderId(), event.expectedRows(), payload, now);
    };
  }

  private static boolean isDuplicate(DataIntegrityViolationException exception) {
    return Optional.ofNullable(exception.getCause())
        .filter(ConstraintViolationException.class::isInstance)
        .map(cause -> ((ConstraintViolationException) cause).getConstraintName())
        .filter(DUPLICATE_CONSTRAINTS::contains)
        .isPresent();
  }
}
