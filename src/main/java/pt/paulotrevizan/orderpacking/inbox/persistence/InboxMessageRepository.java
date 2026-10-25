package pt.paulotrevizan.orderpacking.inbox.persistence;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import pt.paulotrevizan.orderpacking.inbox.domain.InboxMessage;
import pt.paulotrevizan.orderpacking.inbox.domain.MessageType;

public interface InboxMessageRepository extends JpaRepository<InboxMessage, UUID> {

  /** Due "order completed" records are locked. Rows that another instance holds are skipped. */
  @Query(
      value =
          """
            select *
            from inbox_message
            where type = 'ORDER_COMPLETED'
              and status in ('RECEIVED', 'WAITING')
              and next_attempt_at <= :now
            order by next_attempt_at
            limit :limit
            for update skip locked
            """,
      nativeQuery = true)
  List<InboxMessage> claimDueOrderCompleted(@Param("now") Instant now, @Param("limit") int limit);

  /** Claims label requests older than the cutoff, abandoned by a pod that died mid-call. */
  @Query(
      value =
          """
            select *
            from inbox_message
            where status = 'LABEL_REQUESTED'
              and label_requested_at < :cutoff
            order by label_requested_at
            limit :limit
            for update skip locked
            """,
      nativeQuery = true)
  List<InboxMessage> claimAbandonedLabelRequests(
      @Param("cutoff") Instant cutoff, @Param("limit") int limit);

  /** The reconciler backlog, the same rows {@link #claimDueOrderCompleted} would claim. */
  @Query(
      value =
          """
            select count(*)
            from inbox_message
            where type = 'ORDER_COMPLETED'
              and status in ('RECEIVED', 'WAITING')
              and next_attempt_at <= :now
            """,
      nativeQuery = true)
  long countDueOrderCompleted(@Param("now") Instant now);

  List<InboxMessage> findByOrderIdAndType(String orderId, MessageType type);
}
