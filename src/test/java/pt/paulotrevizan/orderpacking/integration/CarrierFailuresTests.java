package pt.paulotrevizan.orderpacking.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.completeOrder;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.newOrderId;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.orderCompleted;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import pt.paulotrevizan.orderpacking.support.CarrierStub;
import pt.paulotrevizan.orderpacking.support.IntegrationTest;
import pt.paulotrevizan.orderpacking.support.KafkaTestClient;
import pt.paulotrevizan.orderpacking.support.ScenarioDatabase;
import pt.paulotrevizan.orderpacking.support.ScenarioMetrics;
import tools.jackson.databind.json.JsonMapper;

/** HLD 6.3 and 6.4: every carrier outcome, and the guarantee that the carrier is called once. */
@IntegrationTest
class CarrierFailuresTests {

  private static final Duration TIMEOUT = Duration.ofSeconds(20);
  private static final String CARRIER_CALLS_COUNTER = "order_packing_carrier_calls_total";

  @Autowired private KafkaTestClient kafka;

  @Autowired private ScenarioDatabase database;

  @Autowired private ScenarioMetrics metrics;

  @Autowired private CarrierStub carrier;

  @Autowired private JsonMapper jsonMapper;

  @Test
  @DisplayName(
      "I2: a created label marks the order and its rows PROCESSED, after one request with the 64 items")
  void shouldProcessOrderWhenCarrierCreatesLabel() {
    var orderId = newOrderId();

    completeOrder(orderId).forEach(kafka::publish);

    database.awaitOrderCompletedStatus(orderId, "PROCESSED");
    assertThat(database.carrierLabelId(orderId)).contains(CarrierStub.LABEL_ID);
    assertThat(database.rowStatuses(orderId)).hasSize(4).containsOnly("PROCESSED");
    assertThat(carrier.requests(orderId))
        .singleElement()
        .satisfies(
            request ->
                assertThat(jsonMapper.readTree(request.getBodyAsString()).get("items"))
                    .hasSize(64));
  }

  @Test
  @DisplayName(
      "D-005, I2: a refused connection waits, and once the carrier is back the label takes one request")
  void shouldCreateLabelOnceWhenCarrierComesBack() {
    var orderId = newOrderId();
    var refusedBefore = calls("refused");

    carrier.stop();
    try {
      completeOrder(orderId).forEach(kafka::publish);

      await().atMost(TIMEOUT).until(() -> calls("refused") > refusedBefore);
      assertThat(database.orderCompletedStatus(orderId)).isPresent().get().isNotEqualTo("FAILED");
    } finally {
      carrier.start();
    }

    database.awaitOrderCompletedStatus(orderId, "PROCESSED");
    assertThat(carrier.requests(orderId)).hasSize(1);
  }

  @Test
  @DisplayName("D-005: a 4xx fails the order with CARRIER_REJECTED")
  void shouldFailOrderWhenCarrierRejectsIt() {
    var orderId = newOrderId();
    carrier.rejects(orderId);

    completeOrder(orderId).forEach(kafka::publish);

    database.awaitOrderCompletedStatus(orderId, "FAILED");
    assertThat(database.orderCompletedFailureReason(orderId)).contains("CARRIER_REJECTED");
    assertThat(carrier.requests(orderId)).hasSize(1);
  }

  @Test
  @DisplayName(
      "I2, I5: a read timeout leaves the outcome unknown: FAILED for a person to check, never called again")
  void shouldFailOrderWhenCarrierTimesOut() {
    var orderId = newOrderId();
    carrier.respondsAfter(orderId, Duration.ofMillis(1_500));

    assertUnknownOutcomeFails(orderId);
  }

  @Test
  @DisplayName(
      "I2, I5: a 5xx leaves the outcome unknown: FAILED for a person to check, never called again")
  void shouldFailOrderWhenCarrierFailsWithServerError() {
    var orderId = newOrderId();
    carrier.failsWithServerError(orderId);

    assertUnknownOutcomeFails(orderId);
  }

  @Test
  @DisplayName(
      "I2, I5: a label request abandoned by a pod that died mid-call fails after the label timeout, never called again")
  void shouldFailOrderWhenPodDiesMidCall() {
    var orderId = newOrderId();
    kafka.awaitCommitted(kafka.publish(orderCompleted(UUID.randomUUID(), orderId)));

    database.simulateCrashMidCall(orderId);

    database.awaitOrderCompletedStatus(orderId, "FAILED");
    assertThat(database.orderCompletedFailureReason(orderId)).contains("LABEL_OUTCOME_UNKNOWN");
    assertThat(carrier.requests(orderId)).isEmpty();
  }

  private void assertUnknownOutcomeFails(String orderId) {
    var unknownBefore = calls("unknown");

    completeOrder(orderId).forEach(kafka::publish);

    database.awaitOrderCompletedStatus(orderId, "FAILED");
    assertThat(calls("unknown")).isEqualTo(unknownBefore + 1);
    assertThat(database.orderCompletedFailureReason(orderId)).contains("LABEL_OUTCOME_UNKNOWN");
    assertThat(carrier.requests(orderId)).hasSize(1);
  }

  private double calls(String outcome) {
    return metrics.count(CARRIER_CALLS_COUNTER, "outcome", outcome);
  }
}
