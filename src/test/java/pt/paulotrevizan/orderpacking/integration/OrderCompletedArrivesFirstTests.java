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

/**
 * The original bug: "order completed" arrived before the last row and the order ended in the DLQ.
 */
@IntegrationTest
class OrderCompletedArrivesFirstTests {

  @Autowired private KafkaTestClient kafka;

  @Autowired private ScenarioDatabase database;

  @Autowired private CarrierStub carrier;

  @Test
  @DisplayName(
      "I1: \"order completed\" before its rows waits; once the 4 rows arrive the order is labelled once")
  void shouldPackOrderWhenRowsArriveAfterOrderCompleted() {
    var orderId = newOrderId();

    kafka.publish(orderCompleted(UUID.randomUUID(), orderId));

    database.awaitOrderCompletedStatus(orderId, "WAITING");
    assertThat(database.isPacked(orderId)).isFalse();
    assertThat(carrier.requests(orderId)).isEmpty();

    IntStream.rangeClosed(1, 4)
        .forEach(row -> kafka.publish(rowCompleted(UUID.randomUUID(), orderId, row)));

    database.awaitOrderCompletedStatus(orderId, "PROCESSED");
    assertThat(database.packedItems(orderId)).isEqualTo(64);
    assertThat(carrier.requests(orderId)).hasSize(1);
  }
}
