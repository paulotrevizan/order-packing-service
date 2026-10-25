package pt.paulotrevizan.orderpacking.demo;

import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import pt.paulotrevizan.orderpacking.common.config.OrderPackingKafkaProperties;
import pt.paulotrevizan.orderpacking.inbox.domain.MessageType;
import pt.paulotrevizan.orderpacking.inbox.messaging.PickingEvent;

/**
 * Profile {@code demo}: publishes the scenarios the integration tests prove to the local Kafka,
 * then exits. Every run uses new order ids, so it can run any number of times against a running
 * service.
 */
@Slf4j
@Component
@Profile("demo")
@RequiredArgsConstructor
class DemoScenarios implements ApplicationRunner {

  private static final int ROWS = 4;
  private static final int ITEMS_PER_ROW = 16;

  private final KafkaTemplate<Object, Object> kafkaTemplate;
  private final OrderPackingKafkaProperties properties;
  private final Clock clock;

  @Override
  public void run(ApplicationArguments arguments) {
    var outOfOrder = outOfOrder();
    var redelivered = redelivered();
    var missingRow = missingRow();

    log.info("Demo published. Watch each order in inbox_message and the carrier journal:");
    log.info("  out of order   {}  -> PROCESSED, one carrier request", outOfOrder);
    log.info("  redelivered    {}  -> PROCESSED, one carrier request, 5 inbox rows", redelivered);
    log.info("  missing row  {}  -> FAILED MISSING_ROWS, no carrier request", missingRow);
  }

  // the original bug: "order completed" arrives before the rows.
  private String outOfOrder() {
    var orderId = newOrderId();

    publish(orderCompleted(orderId));
    rows(orderId, IntStream.rangeClosed(1, ROWS)).forEach(this::publish);

    return orderId;
  }

  // the original duplicate bug: every message, "order completed" included, are delivered twice.
  private String redelivered() {
    var orderId = newOrderId();
    var events = rows(orderId, IntStream.rangeClosed(1, ROWS));

    events.forEach(this::publishTwice);
    publishTwice(orderCompleted(orderId));

    return orderId;
  }

  // row 3 never arrives: the order must end FAILED, never stuck and never labelled.
  private String missingRow() {
    var orderId = newOrderId();

    rows(orderId, IntStream.of(1, 2, 4)).forEach(this::publish);
    publish(orderCompleted(orderId));

    return orderId;
  }

  private PickingEvent orderCompleted(String orderId) {
    return new PickingEvent(
        UUID.randomUUID(), MessageType.ORDER_COMPLETED, orderId, clock.instant(), null, null, ROWS);
  }

  private List<PickingEvent> rows(String orderId, IntStream rowNumbers) {
    return rowNumbers
        .mapToObj(
            row ->
                new PickingEvent(
                    UUID.randomUUID(),
                    MessageType.ROW_COMPLETED,
                    orderId,
                    clock.instant(),
                    row,
                    items(row),
                    null))
        .toList();
  }

  private static List<PickingEvent.Item> items(int row) {
    return IntStream.rangeClosed(1, ITEMS_PER_ROW)
        .mapToObj(item -> new PickingEvent.Item("SKU-%d-%02d".formatted(row, item), 1))
        .toList();
  }

  private void publishTwice(PickingEvent event) {
    publish(event);
    publish(event);
  }

  // key by order id, so the demo messages arrive exactly in the order the scenario lists them.
  private void publish(PickingEvent event) {
    kafkaTemplate.send(properties.topic(), event.orderId(), event).join();
  }

  private static String newOrderId() {
    return "DEMO-" + UUID.randomUUID();
  }
}
