package pt.paulotrevizan.orderpacking.support;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Reads the outcome of a scenario straight from the database, as an operator would. */
@RequiredArgsConstructor
public class ScenarioDatabase {

  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  private final JdbcClient jdbcClient;

  public void awaitOrderCompletedStatus(String orderId, String status) {
    await()
        .atMost(TIMEOUT)
        .until(() -> orderCompletedStatus(orderId).filter(status::equals).isPresent());
  }

  /**
   * The state a pod leaves when it dies between committing LABEL_REQUESTED and recording the
   * carrier's outcome, long enough ago to be past the label timeout.
   */
  public void simulateCrashMidCall(String orderId) {
    jdbcClient
        .sql(
            """
                update inbox_message
                set status = 'LABEL_REQUESTED', label_requested_at = now() - interval '1 minute'
                where order_id = :orderId
                  and type = 'ORDER_COMPLETED'
                """)
        .param("orderId", orderId)
        .update();
  }

  public long inboxRows(String orderId) {
    return jdbcClient
        .sql("select count(*) from inbox_message where order_id = :orderId")
        .param("orderId", orderId)
        .query(Long.class)
        .single();
  }

  public Optional<String> orderCompletedStatus(String orderId) {
    return jdbcClient
        .sql(
            """
                select status
                from inbox_message
                where order_id = :orderId
                  and type = 'ORDER_COMPLETED'
                """)
        .param("orderId", orderId)
        .query(String.class)
        .optional();
  }

  public Optional<String> orderCompletedFailureReason(String orderId) {
    return jdbcClient
        .sql(
            """
                select failure_reason
                from inbox_message
                where order_id = :orderId
                  and type = 'ORDER_COMPLETED'
                """)
        .param("orderId", orderId)
        .query(String.class)
        .optional();
  }

  public List<String> rowStatuses(String orderId) {
    return jdbcClient
        .sql(
            """
                select status
                from inbox_message
                where order_id = :orderId
                  and type = 'ROW_COMPLETED'
                """)
        .param("orderId", orderId)
        .query(String.class)
        .list();
  }

  public boolean isPacked(String orderId) {
    return jdbcClient
            .sql("select count(*) from packed_order where order_id = :orderId")
            .param("orderId", orderId)
            .query(Long.class)
            .single()
        > 0;
  }

  public Optional<String> carrierLabelId(String orderId) {
    return jdbcClient
        .sql("select carrier_label_id from packed_order where order_id = :orderId")
        .param("orderId", orderId)
        .query(String.class)
        .optional();
  }

  public long packedItems(String orderId) {
    return jdbcClient
        .sql(
            """
                select count(*)
                from packed_order_item item
                join packed_order packed on packed.id = item.packed_order_id
                where packed.order_id = :orderId
                """)
        .param("orderId", orderId)
        .query(Long.class)
        .single();
  }
}
