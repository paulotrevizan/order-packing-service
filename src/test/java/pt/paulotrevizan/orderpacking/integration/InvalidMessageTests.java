package pt.paulotrevizan.orderpacking.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.newOrderId;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.rowCompleted;

import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import pt.paulotrevizan.orderpacking.support.IntegrationTest;
import pt.paulotrevizan.orderpacking.support.KafkaTestClient;
import pt.paulotrevizan.orderpacking.support.ScenarioDatabase;
import pt.paulotrevizan.orderpacking.support.ScenarioMetrics;

@IntegrationTest
class InvalidMessageTests {

  private static final int PARTITION = 3;
  private static final String INVALID_RECORDS_COUNTER = "order_packing_invalid_records_total";

  @Autowired private KafkaTestClient kafka;

  @Autowired private ScenarioDatabase database;

  @Autowired private ScenarioMetrics metrics;

  @Test
  @DisplayName(
      "D-008: a malformed record is skipped and counted, and the next record on its partition is still stored")
  void shouldSkipMalformedRecordWhenNextRecordFollows() {
    var malformedOrderId = newOrderId();
    var validOrderId = newOrderId();
    var invalidBefore = invalidRecords();

    kafka.publish(
        PARTITION,
        """
            {"eventId": "not-even-json, "orderId": "%s"
            """
            .formatted(malformedOrderId));
    kafka.awaitCommitted(
        kafka.publish(PARTITION, rowCompleted(UUID.randomUUID(), validOrderId, 1)));

    assertThat(invalidRecords()).isEqualTo(invalidBefore + 1);
    assertThat(database.inboxRows(malformedOrderId)).isZero();
    assertThat(database.inboxRows(validOrderId)).isEqualTo(1);
  }

  @Test
  @DisplayName(
      "D-008: a row without rowNumber fails validation, is skipped at once with its offset committed, and is not stored")
  void shouldSkipRowWhenRowNumberIsMissing() {
    var orderId = newOrderId();
    var invalidBefore = invalidRecords();

    kafka.awaitCommitted(
        kafka.publish(
            PARTITION,
            """
            {
              "eventId": "%s",
              "type": "ROW_COMPLETED",
              "orderId": "%s",
              "occurredAt": "2026-10-15T10:00:00Z",
              "items": [{"sku": "SKU-1", "quantity": 1}]
            }
            """
                .formatted(UUID.randomUUID(), orderId)));

    assertThat(invalidRecords()).isEqualTo(invalidBefore + 1);
    assertThat(database.inboxRows(orderId)).isZero();
  }

  @Test
  @DisplayName(
      "D-008, D-016: a row numbered outside 1..4 is a producer bug: skipped and counted at once, not stored")
  void shouldSkipRowWhenRowNumberIsOutOfRange() {
    var orderId = newOrderId();
    var invalidBefore = invalidRecords();

    kafka.awaitCommitted(kafka.publish(PARTITION, rowCompleted(UUID.randomUUID(), orderId, 5)));

    assertThat(invalidRecords()).isEqualTo(invalidBefore + 1);
    assertThat(database.inboxRows(orderId)).isZero();
  }

  @Test
  @DisplayName(
      "D-008: an \"order completed\" without expectedRows fails validation, is skipped at once with its offset committed, and is not stored")
  void shouldSkipOrderCompletedWhenExpectedRowsIsMissing() {
    var orderId = newOrderId();
    var invalidBefore = invalidRecords();

    kafka.awaitCommitted(
        kafka.publish(
            PARTITION,
            """
            {
              "eventId": "%s",
              "type": "ORDER_COMPLETED",
              "orderId": "%s",
              "occurredAt": "2026-10-15T10:00:05Z"
            }
            """
                .formatted(UUID.randomUUID(), orderId)));

    assertThat(invalidRecords()).isEqualTo(invalidBefore + 1);
    assertThat(database.inboxRows(orderId)).isZero();
  }

  private double invalidRecords() {
    return metrics.count(INVALID_RECORDS_COUNTER);
  }
}
