package pt.paulotrevizan.orderpacking.packing.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import pt.paulotrevizan.orderpacking.inbox.domain.FailureReason;
import pt.paulotrevizan.orderpacking.inbox.domain.InboxMessage;
import pt.paulotrevizan.orderpacking.integration.carrier.CarrierClient;
import pt.paulotrevizan.orderpacking.integration.carrier.LabelRequest;
import pt.paulotrevizan.orderpacking.integration.carrier.LabelResponse;

/**
 * Calls the carrier once for an ORDER_COMPLETED message that was just committed as LABEL_REQUESTED,
 * then records the outcome. Nothing here calls the carrier again for the same order. An unknown
 * outcome is checked by a person, never retried.
 */
@Slf4j
@Service
@RequiredArgsConstructor
class LabelRequester {

  private final CarrierClient carrierClient;
  private final PackedOrders packedOrders;
  private final LabelOutcomes labelOutcomes;
  private final PackingMetrics metrics;

  // an HTTP call inside a transaction could be rolled back after the label exists, so Spring
  // refuses to run this method while one is active.
  @Transactional(propagation = Propagation.NEVER)
  void requestLabel(InboxMessage message) {
    var request = packedOrders.createLabelRequest(message.getOrderId());
    var carrierCall = callCarrier(request);
    metrics.carrierCall(carrierCall.outcome());
    if (carrierCall.outcome() != CarrierCallOutcome.CREATED) {
      log.warn(
          "Carrier call unsuccessful: orderId={}, outcome={}, detail={}",
          message.getOrderId(),
          carrierCall.outcome(),
          carrierCall.detail());
    }

    switch (carrierCall.outcome()) {
      case CREATED -> recordCreated(message, carrierCall.labelId());
      case REFUSED -> labelOutcomes.refused(message.getId());
      case REJECTED -> labelOutcomes.failed(message.getId(), FailureReason.CARRIER_REJECTED);
      case UNKNOWN -> labelOutcomes.failed(message.getId(), FailureReason.LABEL_OUTCOME_UNKNOWN);
    }
  }

  private CarrierCall callCarrier(LabelRequest request) {
    LabelResponse response;
    try {
      response = carrierClient.createLabel(request);
    } catch (RestClientException exception) {
      return new CarrierCall(CarrierCallOutcome.of(exception), null, describe(exception));
    }

    if (response == null || response.labelId() == null || response.labelId().isBlank()) {
      return new CarrierCall(CarrierCallOutcome.UNKNOWN, null, "success response without labelId");
    }
    return new CarrierCall(CarrierCallOutcome.CREATED, response.labelId(), null);
  }

  private void recordCreated(InboxMessage message, String labelId) {
    try {
      labelOutcomes.created(message.getId(), labelId);
    } catch (RuntimeException exception) {
      // the label exists but is not recorded: the order will fail as LABEL_OUTCOME_UNKNOWN after
      // the label timeout, and whoever checks it needs this label id.
      log.error(
          "Label created but not recorded, will fail as LABEL_OUTCOME_UNKNOWN: orderId={}, labelId={}",
          message.getOrderId(),
          labelId,
          exception);
      throw exception;
    }

    log.info("Order labelled: orderId={}, labelId={}", message.getOrderId(), labelId);
    metrics.orderProcessed();
  }

  // logs carry no response bodies.
  private static String describe(RestClientException exception) {
    return exception instanceof RestClientResponseException response
        ? "HTTP " + response.getStatusCode().value()
        : exception.getMostSpecificCause().getClass().getSimpleName();
  }

  private record CarrierCall(CarrierCallOutcome outcome, String labelId, String detail) {}
}
