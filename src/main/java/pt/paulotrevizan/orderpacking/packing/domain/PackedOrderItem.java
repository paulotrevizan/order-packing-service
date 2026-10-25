package pt.paulotrevizan.orderpacking.packing.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import pt.paulotrevizan.orderpacking.common.persistence.BaseEntity;

@Entity
@Table(name = "packed_order_item")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PackedOrderItem extends BaseEntity {

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "packed_order_id")
  private PackedOrder packedOrder;

  @JdbcTypeCode(SqlTypes.SMALLINT)
  private Integer rowNumber;

  private String sku;

  private int quantity;

  public PackedOrderItem(PackedOrder packedOrder, int rowNumber, String sku, int quantity) {
    this.packedOrder = packedOrder;
    this.rowNumber = rowNumber;
    this.sku = sku;
    this.quantity = quantity;
  }
}
