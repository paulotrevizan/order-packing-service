package pt.paulotrevizan.orderpacking.packing.persistence;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import pt.paulotrevizan.orderpacking.packing.domain.PackedOrderItem;

public interface PackedOrderItemRepository extends JpaRepository<PackedOrderItem, UUID> {

  List<PackedOrderItem> findByPackedOrderId(UUID packedOrderId);
}
