package pt.paulotrevizan.orderpacking.packing.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import pt.paulotrevizan.orderpacking.common.persistence.BaseEntity;

@Entity
@Table(name = "packed_order")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PackedOrder extends BaseEntity {

  private String orderId;

  private String carrierLabelId;

  private Instant packedAt;

  private Instant labelledAt;

  public PackedOrder(String orderId, Instant packedAt) {
    this.orderId = orderId;
    this.packedAt = packedAt;
  }

  public void markLabelled(String carrierLabelId, Instant labelledAt) {
    this.carrierLabelId = carrierLabelId;
    this.labelledAt = labelledAt;
  }
}
