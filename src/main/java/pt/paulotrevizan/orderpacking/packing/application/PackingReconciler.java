package pt.paulotrevizan.orderpacking.packing.application;

import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Each ORDER_COMPLETED message is processed in its own transaction. A message that ends
 * LABEL_REQUESTED then gets its label, after that transaction is committed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PackingReconciler {

  private final MessageProcessor messageProcessor;
  private final LabelRequester labelRequester;
  private final PackingProperties properties;

  // each worker claims its own messages with SKIP LOCKED, so the carrier calls run in parallel and
  // no two workers take the same message. closing the executor waits for all of them.
  @Scheduled(fixedDelayString = "${order-packing.packing.poll-interval}")
  public void reconcile() {
    messageProcessor.failAbandonedLabelRequests();

    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      for (var worker = 0; worker < properties.workers(); worker++) {
        executor.execute(this::processBatch);
      }
    }
  }

  private void processBatch() {
    try {
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
    } catch (RuntimeException exception) {
      // the executor would drop it. only this worker stops, the next cycle resumes its work.
      log.error("Reconciler worker stopped", exception);
    }
  }
}
