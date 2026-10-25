package pt.paulotrevizan.orderpacking.packing.persistence;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import pt.paulotrevizan.orderpacking.packing.domain.PackedOrder;

public interface PackedOrderRepository extends JpaRepository<PackedOrder, UUID> {

  boolean existsByOrderId(String orderId);

  Optional<PackedOrder> findByOrderId(String orderId);
}
