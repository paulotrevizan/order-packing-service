package pt.paulotrevizan.orderpacking.packing.application;

import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pt.paulotrevizan.orderpacking.inbox.domain.InboxMessage;
import pt.paulotrevizan.orderpacking.integration.carrier.LabelRequest;
import pt.paulotrevizan.orderpacking.packing.domain.PackedOrder;
import pt.paulotrevizan.orderpacking.packing.domain.PackedOrderItem;
import pt.paulotrevizan.orderpacking.packing.persistence.PackedOrderItemRepository;
import pt.paulotrevizan.orderpacking.packing.persistence.PackedOrderRepository;
import tools.jackson.databind.json.JsonMapper;

/** The packed order and its 64 items: created once, read for the label, marked when labelled. */
@Service
@RequiredArgsConstructor
class PackedOrders {

  private final PackedOrderRepository packedOrderRepository;
  private final PackedOrderItemRepository packedOrderItemRepository;
  private final JsonMapper jsonMapper;

  /** It may already exist when a previous carrier call was refused and the order waited. */
  void createIfAbsent(String orderId, List<InboxMessage> rowMessages, Instant now) {
    if (packedOrderRepository.existsByOrderId(orderId)) {
      return;
    }

    var packedOrder = packedOrderRepository.saveAndFlush(new PackedOrder(orderId, now));
    packedOrderItemRepository.saveAllAndFlush(items(packedOrder, rowMessages));
  }

  LabelRequest createLabelRequest(String orderId) {
    var packedOrder = packedOrderRepository.findByOrderId(orderId).orElseThrow();

    var items =
        packedOrderItemRepository.findByPackedOrderId(packedOrder.getId()).stream()
            .map(item -> new LabelRequest.Item(item.getSku(), item.getQuantity()))
            .toList();

    return new LabelRequest(orderId, items);
  }

  void markLabelled(String orderId, String labelId, Instant now) {
    packedOrderRepository.findByOrderId(orderId).orElseThrow().markLabelled(labelId, now);
  }

  private List<PackedOrderItem> items(PackedOrder packedOrder, List<InboxMessage> rowMessages) {
    return rowMessages.stream()
        .flatMap(rowMessage -> items(packedOrder, rowMessage).stream())
        .toList();
  }

  // the items only exist in the stored message. they are copied once, when the order is packed.
  private List<PackedOrderItem> items(PackedOrder packedOrder, InboxMessage rowMessage) {
    return jsonMapper.readValue(rowMessage.getPayload(), RowPayload.class).items().stream()
        .map(
            item ->
                new PackedOrderItem(
                    packedOrder, rowMessage.getRowNumber(), item.sku(), item.quantity()))
        .toList();
  }

  private record RowPayload(List<Item> items) {

    private record Item(String sku, int quantity) {}
  }
}
