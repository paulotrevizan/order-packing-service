package pt.paulotrevizan.orderpacking.support;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Records as the producer sends them (HLD 7.1). */
public final class PickingEvents {

  private static final int ITEMS_PER_ROW = 16;

  private PickingEvents() {}

  public static String newOrderId() {
    return "ORD-" + UUID.randomUUID();
  }

  /** "Order completed" followed by its 4 rows, each with a new eventId. */
  public static List<String> completeOrder(String orderId) {
    return Stream.concat(
            Stream.of(orderCompleted(UUID.randomUUID(), orderId)),
            IntStream.rangeClosed(1, 4)
                .mapToObj(row -> rowCompleted(UUID.randomUUID(), orderId, row)))
        .toList();
  }

  public static String rowCompleted(UUID eventId, String orderId, int rowNumber) {
    return """
            {
              "eventId": "%s",
              "type": "ROW_COMPLETED",
              "orderId": "%s",
              "occurredAt": "2026-10-15T10:00:00Z",
              "rowNumber": %d,
              "items": [%s]
            }
            """
        .formatted(eventId, orderId, rowNumber, items(rowNumber));
  }

  public static String orderCompleted(UUID eventId, String orderId) {
    return """
            {
              "eventId": "%s",
              "type": "ORDER_COMPLETED",
              "orderId": "%s",
              "occurredAt": "2026-10-15T10:00:05Z",
              "expectedRows": 4
            }
            """
        .formatted(eventId, orderId);
  }

  private static String items(int rowNumber) {
    return IntStream.rangeClosed(1, ITEMS_PER_ROW)
        .mapToObj(
            item ->
                """
                {"sku": "SKU-%d-%d", "quantity": 1}"""
                    .formatted(rowNumber, item))
        .collect(Collectors.joining(", "));
  }
}
