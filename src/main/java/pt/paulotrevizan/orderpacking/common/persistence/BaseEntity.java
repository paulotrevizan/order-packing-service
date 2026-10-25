package pt.paulotrevizan.orderpacking.common.persistence;

import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Transient;
import java.util.UUID;
import org.springframework.data.domain.Persistable;

/**
 * The id is assigned by the application, so {@link #isNew()} tells Spring Data to {@code persist}
 * (a plain INSERT) instead of {@code merge} (a SELECT first).
 */
@MappedSuperclass
public abstract class BaseEntity implements Persistable<UUID> {

  @Id private UUID id;

  @Transient private boolean isNew = true;

  protected BaseEntity() {
    this.id = UUID.randomUUID();
  }

  @Override
  public UUID getId() {
    return id;
  }

  @Override
  public boolean isNew() {
    return isNew;
  }

  @PostLoad
  @PostPersist
  void markNotNew() {
    this.isNew = false;
  }
}
