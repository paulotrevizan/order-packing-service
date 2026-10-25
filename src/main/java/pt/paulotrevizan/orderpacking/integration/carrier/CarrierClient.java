package pt.paulotrevizan.orderpacking.integration.carrier;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.service.annotation.PostExchange;

/**
 * The carrier is not idempotent and without a lookup endpoint, so every call may create a label. It
 * is called at most once per order.
 */
public interface CarrierClient {

  @PostExchange("/labels")
  LabelResponse createLabel(@RequestBody LabelRequest request);
}
