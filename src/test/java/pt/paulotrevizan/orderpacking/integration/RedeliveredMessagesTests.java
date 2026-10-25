package pt.paulotrevizan.orderpacking.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.completeOrder;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.newOrderId;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.orderCompleted;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.rowCompleted;

import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import pt.paulotrevizan.orderpacking.support.CarrierStub;
import pt.paulotrevizan.orderpacking.support.IntegrationTest;
import pt.paulotrevizan.orderpacking.support.KafkaTestClient;
import pt.paulotrevizan.orderpacking.support.ScenarioDatabase;
import pt.paulotrevizan.orderpacking.support.ScenarioMetrics;

@IntegrationTest
class RedeliveredMessagesTests {

  @Autowired private KafkaTestClient kafka;

  @Autowired private JdbcClient jdbcClient;

  @Autowired private ScenarioDatabase database;

  @Autowired private ScenarioMetrics metrics;

  @Autowired private CarrierStub carrier;

  @Test
  @DisplayName(
      "I4: a valid row is stored as RECEIVED with its payload, and its offset is committed")
  void shouldStoreRowWhenRecordIsValid() throws Exception {
    var orderId = newOrderId();
    var record = rowCompleted(UUID.randomUUID(), orderId, 2);

    kafka.awaitCommitted(kafka.publish(record));

    var row =
        jdbcClient
            .sql(
                """
                select type, status, row_number, payload::text as payload
                from inbox_message
                where order_id = :orderId
                """)
            .param("orderId", orderId)
            .query()
            .singleRow();
    assertThat(row)
        .containsEntry("type", "ROW_COMPLETED")
        .containsEntry("status", "RECEIVED")
        .containsEntry("row_number", 2);
    JSONAssert.assertEquals(record, (String) row.get("payload"), true);
  }

  @Test
  @DisplayName(
      "I3: a record redelivered with the same eventId is stored once and counted as a duplicate")
  void shouldStoreOnceWhenSameRecordIsPublishedTwice() {
    var orderId = newOrderId();
    var record = rowCompleted(UUID.randomUUID(), orderId, 1);
    var duplicatesBefore = duplicates("ROW_COMPLETED");

    kafka.awaitCommitted(kafka.publish(record));
    kafka.awaitCommitted(kafka.publish(record));

    assertThat(database.inboxRows(orderId)).isEqualTo(1);
    assertThat(duplicates("ROW_COMPLETED")).isEqualTo(duplicatesBefore + 1);
  }

  @Test
  @DisplayName("I3, D-004: a row re-sent with a new eventId is stored once")
  void shouldStoreOnceWhenRowIsResentWithNewEventId() {
    var orderId = newOrderId();

    kafka.awaitCommitted(kafka.publish(rowCompleted(UUID.randomUUID(), orderId, 3)));
    kafka.awaitCommitted(kafka.publish(rowCompleted(UUID.randomUUID(), orderId, 3)));

    assertThat(database.inboxRows(orderId)).isEqualTo(1);
  }

  @Test
  @DisplayName("I3, D-004: a second \"order completed\" for the same order is stored once")
  void shouldStoreOnceWhenOrderCompletedIsResent() {
    var orderId = newOrderId();

    kafka.awaitCommitted(kafka.publish(orderCompleted(UUID.randomUUID(), orderId)));
    kafka.awaitCommitted(kafka.publish(orderCompleted(UUID.randomUUID(), orderId)));

    assertThat(database.inboxRows(orderId)).isEqualTo(1);
  }

  @Test
  @DisplayName(
      "I2, I3: every message of an order published twice, including \"order completed\", gives one carrier request")
  void shouldCallCarrierOnceWhenEveryMessageIsRedelivered() {
    var orderId = newOrderId();
    var records = completeOrder(orderId);

    records.forEach(kafka::publish);
    records.forEach(kafka::publish);

    database.awaitOrderCompletedStatus(orderId, "PROCESSED");
    assertThat(database.inboxRows(orderId)).isEqualTo(5);
    assertThat(carrier.requests(orderId)).hasSize(1);
  }

  private double duplicates(String type) {
    return metrics.count("order_packing_inbox_messages_total", "type", type, "result", "duplicate");
  }
}
