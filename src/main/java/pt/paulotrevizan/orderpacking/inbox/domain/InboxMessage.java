package pt.paulotrevizan.orderpacking.inbox.domain;

import static pt.paulotrevizan.orderpacking.inbox.domain.InboxStatus.FAILED;
import static pt.paulotrevizan.orderpacking.inbox.domain.InboxStatus.LABEL_REQUESTED;
import static pt.paulotrevizan.orderpacking.inbox.domain.InboxStatus.PROCESSED;
import static pt.paulotrevizan.orderpacking.inbox.domain.InboxStatus.RECEIVED;
import static pt.paulotrevizan.orderpacking.inbox.domain.InboxStatus.WAITING;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import pt.paulotrevizan.orderpacking.common.persistence.BaseEntity;

/**
 * Every received message, with processing status. The reconciler processes ORDER_COMPLETED
 * messages. The ROW_COMPLETED messages of the same order are marked PROCESSED with it.
 */
@Entity
@Table(name = "inbox_message")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class InboxMessage extends BaseEntity {

  private UUID eventId;

  private String orderId;

  @Enumerated(EnumType.STRING)
  private MessageType type;

  @JdbcTypeCode(SqlTypes.SMALLINT)
  private Integer rowNumber;

  @JdbcTypeCode(SqlTypes.SMALLINT)
  private Integer expectedRows;

  @JdbcTypeCode(SqlTypes.JSON)
  private String payload;

  @Enumerated(EnumType.STRING)
  private InboxStatus status;

  private int attempts;

  private Instant nextAttemptAt;

  private Instant labelRequestedAt;

  @Enumerated(EnumType.STRING)
  private FailureReason failureReason;

  private Instant receivedAt;

  private Instant updatedAt;

  private InboxMessage(
      UUID eventId,
      String orderId,
      MessageType type,
      Integer rowNumber,
      Integer expectedRows,
      String payload,
      Instant receivedAt) {
    this.eventId = eventId;
    this.orderId = orderId;
    this.type = type;
    this.rowNumber = rowNumber;
    this.expectedRows = expectedRows;
    this.payload = payload;
    this.status = RECEIVED;
    this.attempts = 0;
    this.nextAttemptAt = receivedAt;
    this.receivedAt = receivedAt;
    this.updatedAt = receivedAt;
  }

  public static InboxMessage rowCompleted(
      UUID eventId, String orderId, int rowNumber, String payload, Instant receivedAt) {
    return new InboxMessage(
        eventId, orderId, MessageType.ROW_COMPLETED, rowNumber, null, payload, receivedAt);
  }

  public static InboxMessage orderCompleted(
      UUID eventId, String orderId, int expectedRows, String payload, Instant receivedAt) {
    return new InboxMessage(
        eventId, orderId, MessageType.ORDER_COMPLETED, null, expectedRows, payload, receivedAt);
  }

  public boolean isLabelRequested() {
    return status == LABEL_REQUESTED;
  }

  public void markWaiting(Instant nextAttemptAt, Instant now) {
    this.status = WAITING;
    this.attempts++;
    this.nextAttemptAt = nextAttemptAt;
    this.labelRequestedAt = null;
    this.updatedAt = now;
  }

  public void markLabelRequested(Instant now) {
    this.status = LABEL_REQUESTED;
    this.labelRequestedAt = now;
    this.updatedAt = now;
  }

  public void markProcessed(Instant now) {
    this.status = PROCESSED;
    this.updatedAt = now;
  }

  public void markFailed(FailureReason reason, Instant now) {
    this.status = FAILED;
    this.failureReason = reason;
    this.updatedAt = now;
  }
}
