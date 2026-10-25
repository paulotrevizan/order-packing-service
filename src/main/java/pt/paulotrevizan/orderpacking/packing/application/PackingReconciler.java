package pt.paulotrevizan.orderpacking.packing.application;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Each ORDER_COMPLETED message is processed in its own transaction. A message that ends
 * LABEL_REQUESTED then gets its label, after that transaction is committed.
 */
@Service
@RequiredArgsConstructor
public class PackingReconciler {

  private final MessageProcessor messageProcessor;
  private final LabelRequester labelRequester;
  private final PackingProperties properties;

  @Scheduled(fixedDelayString = "${order-packing.packing.poll-interval}")
  public void reconcile() {
    messageProcessor.failAbandonedLabelRequests();

    for (var processed = 0; processed < properties.batchSize(); processed++) {
      var nextMessage = messageProcessor.processNext();
      if (nextMessage.isEmpty()) {
        return;
      }

      var message = nextMessage.get();
      if (message.isLabelRequested()) {
        labelRequester.requestLabel(message);
      }
    }
  }
}
