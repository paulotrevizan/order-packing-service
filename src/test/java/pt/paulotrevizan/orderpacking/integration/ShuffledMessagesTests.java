package pt.paulotrevizan.orderpacking.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.completeOrder;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.newOrderId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import pt.paulotrevizan.orderpacking.support.CarrierStub;
import pt.paulotrevizan.orderpacking.support.IntegrationTest;
import pt.paulotrevizan.orderpacking.support.KafkaTestClient;
import pt.paulotrevizan.orderpacking.support.ScenarioDatabase;

/** Both original bugs at once: any arrival order across partitions, and redeliveries. */
@IntegrationTest
class ShuffledMessagesTests {

  private static final int ORDERS = 20;
  private static final int DUPLICATE_EVERY = 3;
  private static final long SEED = 42;

  @Autowired private KafkaTestClient kafka;

  @Autowired private ScenarioDatabase database;

  @Autowired private CarrierStub carrier;

  @Test
  @DisplayName(
      "I1, I2, I3: 20 orders, every message shuffled and some redelivered, each labelled once")
  void shouldLabelEveryOrderOnceWhenMessagesAreShuffledAndRedelivered() {
    var orderIds = IntStream.range(0, ORDERS).mapToObj(order -> newOrderId()).toList();
    var records = new ArrayList<String>();
    orderIds.forEach(orderId -> records.addAll(completeOrder(orderId)));
    IntStream.range(0, records.size())
        .filter(index -> index % DUPLICATE_EVERY == 0)
        .mapToObj(records::get)
        .toList()
        .forEach(records::add);
    Collections.shuffle(records, new Random(SEED));

    records.forEach(kafka::publish);

    orderIds.forEach(orderId -> database.awaitOrderCompletedStatus(orderId, "PROCESSED"));
    orderIds.forEach(
        orderId -> {
          assertThat(database.inboxRows(orderId)).isEqualTo(5);
          assertThat(carrier.requests(orderId)).hasSize(1);
        });
  }
}
