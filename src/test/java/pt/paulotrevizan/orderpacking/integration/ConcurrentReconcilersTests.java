package pt.paulotrevizan.orderpacking.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.newOrderId;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.orderCompleted;
import static pt.paulotrevizan.orderpacking.support.PickingEvents.rowCompleted;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import pt.paulotrevizan.orderpacking.packing.application.PackingReconciler;
import pt.paulotrevizan.orderpacking.support.CarrierStub;
import pt.paulotrevizan.orderpacking.support.IntegrationTest;
import pt.paulotrevizan.orderpacking.support.KafkaTestClient;
import pt.paulotrevizan.orderpacking.support.ScenarioDatabase;

/** Several pods reconciling at once (NFR6): each worker stands in for a pod's reconciler. */
@IntegrationTest
class ConcurrentReconcilersTests {

  private static final int ORDERS = 50;
  private static final int RECONCILERS = 4;
  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  @Autowired private KafkaTestClient kafka;

  @Autowired private ScenarioDatabase database;

  @Autowired private CarrierStub carrier;

  @Autowired private PackingReconciler reconciler;

  @Test
  @DisplayName(
      "I2, D-003: reconcilers racing for 50 orders that complete at once call the carrier once per order")
  void shouldCallCarrierOncePerOrderWhenReconcilersRunConcurrently() throws Exception {
    var orderIds = IntStream.range(0, ORDERS).mapToObj(order -> newOrderId()).toList();
    storeAllRows(orderIds);

    var start = new CountDownLatch(1);
    var running = new AtomicBoolean(true);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<Void>> reconcilers =
          IntStream.range(0, RECONCILERS)
              .mapToObj(
                  worker ->
                      executor.submit(
                          () -> {
                            start.await();
                            while (running.get()) {
                              reconciler.reconcile();
                            }
                            return (Void) null;
                          }))
              .toList();

      start.countDown();
      try {
        // Each "order completed" completes its order the moment it lands: every reconciler wants
        // it.
        orderIds.forEach(orderId -> kafka.publish(orderCompleted(UUID.randomUUID(), orderId)));
        orderIds.forEach(orderId -> database.awaitOrderCompletedStatus(orderId, "PROCESSED"));
      } finally {
        // Stops the workers on failure too; otherwise closing the executor waits forever.
        running.set(false);
      }

      for (var finished : reconcilers) {
        finished.get();
      }
    }

    orderIds.forEach(orderId -> assertThat(carrier.requests(orderId)).hasSize(1));
  }

  // Rows alone are never claimed, so no order is due until its "order completed" arrives.
  private void storeAllRows(List<String> orderIds) {
    orderIds.forEach(
        orderId ->
            IntStream.rangeClosed(1, 4)
                .forEach(row -> kafka.publish(rowCompleted(UUID.randomUUID(), orderId, row))));

    await()
        .atMost(TIMEOUT)
        .until(() -> orderIds.stream().allMatch(orderId -> database.inboxRows(orderId) == 4));
  }
}
