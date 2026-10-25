package pt.paulotrevizan.orderpacking.inbox.messaging;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import pt.paulotrevizan.orderpacking.inbox.application.InboxWriter;

@Component
@RequiredArgsConstructor
class PickingEventListener {

  private final InboxWriter inboxWriter;

  // acknowledged only after the inbox transaction commits. duplicates are acknowledged too.
  @KafkaListener(topics = "${order-packing.kafka.topic}")
  void onPickingEvent(@Payload @Valid PickingEvent event, Acknowledgment acknowledgment) {
    inboxWriter.store(event);

    acknowledgment.acknowledge();
  }
}
