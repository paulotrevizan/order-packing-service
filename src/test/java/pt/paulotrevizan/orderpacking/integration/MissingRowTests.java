package pt.paulotrevizan.orderpacking.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.newOrderId;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.orderCompleted;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.rowCompleted;

import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import pt.paulotrevizan.orderpacking.support.CarrierStub;
import pt.paulotrevizan.orderpacking.support.IntegrationTest;
import pt.paulotrevizan.orderpacking.support.KafkaTestClient;
import pt.paulotrevizan.orderpacking.support.ScenarioDatabase;
import pt.paulotrevizan.orderpacking.support.ScenarioMetrics;

@IntegrationTest
class MissingRowTests {

  private static final String ORDERS_COUNTER = "order_packing_orders_total";

  @Autowired private KafkaTestClient kafka;

  @Autowired private ScenarioDatabase database;

  @Autowired private ScenarioMetrics metrics;

  @Autowired private CarrierStub carrier;

  @Test
  @DisplayName(
      "I1, I5: an order whose row never arrives is never labelled; it fails with MISSING_ROWS and is counted")
  void shouldFailOrderWhenRowNeverArrives() {
    var orderId = newOrderId();
    var orderCompletedRecord = orderCompleted(UUID.randomUUID(), orderId);
    var failuresBefore = failures();

    kafka.publish(orderCompletedRecord);
    IntStream.of(1, 2, 4)
        .forEach(row -> kafka.publish(rowCompleted(UUID.randomUUID(), orderId, row)));

    database.awaitOrderCompletedStatus(orderId, "FAILED");
    assertThat(database.orderCompletedFailureReason(orderId)).contains("MISSING_ROWS");
    assertThat(database.isPacked(orderId)).isFalse();
    assertThat(carrier.requests(orderId)).isEmpty();
    assertThat(failures()).isEqualTo(failuresBefore + 1);
  }

  private double failures() {
    return metrics.count(ORDERS_COUNTER, "outcome", "failed", "reason", "MISSING_ROWS");
  }
}
