package pt.paulotrevizan.orderpacking.inbox.messaging;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import pt.paulotrevizan.orderpacking.inbox.domain.MessageType;

/**
 * One record for both message type. The fields of each type are checked by type. The size checks
 * match the database columns, so a record that passes validation can always be inserted.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PickingEvent(
    @NotNull(message = "eventId is required") UUID eventId,
    @NotNull(message = "type is required") MessageType type,
    @NotBlank(message = "orderId is required")
        @Size(max = 64, message = "orderId must have at most 64 characters")
        String orderId,
    @NotNull(message = "occurredAt is required") Instant occurredAt,
    @Min(value = 1, message = "rowNumber must be between 1 and 4")
        @Max(value = ROWS_PER_ORDER, message = "rowNumber must be between 1 and 4")
        Integer rowNumber,
    List<@NotNull(message = "items must not contain null") @Valid Item> items,
    @Min(value = 1, message = "expectedRows must be between 1 and 4")
        @Max(value = ROWS_PER_ORDER, message = "expectedRows must be between 1 and 4")
        Integer expectedRows) {

  // a business limit: an order is always picked in 4 rows.
  private static final int ROWS_PER_ORDER = 4;

  public PickingEvent {
    if (type == MessageType.ROW_COMPLETED
        && (rowNumber == null || items == null || items.isEmpty())) {
      throw new IllegalArgumentException(
          "ROW_COMPLETED requires rowNumber and a non-empty items list");
    }
    if (type == MessageType.ORDER_COMPLETED && expectedRows == null) {
      throw new IllegalArgumentException("ORDER_COMPLETED requires expectedRows");
    }
  }

  public record Item(
      @NotBlank(message = "sku is required")
          @Size(max = 64, message = "sku must have at most 64 characters")
          String sku,
      @NotNull(message = "quantity is required")
          @Min(value = 1, message = "quantity must be at least 1")
          Integer quantity) {}
}
