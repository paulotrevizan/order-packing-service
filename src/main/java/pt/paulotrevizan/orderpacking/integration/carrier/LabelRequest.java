package pt.paulotrevizan.orderpacking.integration.carrier;

import java.util.List;

public record LabelRequest(String orderId, List<Item> items) {

  public record Item(String sku, int quantity) {}
}
