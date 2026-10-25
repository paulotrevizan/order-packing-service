package pt.paulotrevizan.orderpacking.support;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.utils.ContainerTestUtils;

/** Publishes raw records and checks committed offsets, as an outside client would. */
public class KafkaTestClient implements AutoCloseable {

  private static final Duration TIMEOUT = Duration.ofSeconds(20);
  private static final Duration POLL_INTERVAL = Duration.ofMillis(200);

  private final String topic;
  private final String groupId;
  private final int partitions;
  private final KafkaListenerEndpointRegistry listenerRegistry;
  private final KafkaProducer<String, String> producer;
  private final Admin admin;
  private volatile boolean listenerAssigned;

  public KafkaTestClient(
      String bootstrapServers,
      String topic,
      String groupId,
      int partitions,
      KafkaListenerEndpointRegistry listenerRegistry) {
    this.topic = topic;
    this.groupId = groupId;
    this.partitions = partitions;
    this.listenerRegistry = listenerRegistry;
    this.producer =
        new KafkaProducer<>(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
            new StringSerializer(),
            new StringSerializer());
    this.admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
  }

  public RecordMetadata publish(String value) {
    return send(new ProducerRecord<>(topic, value));
  }

  public RecordMetadata publish(int partition, String value) {
    return send(new ProducerRecord<>(topic, partition, null, value));
  }

  /** Waits until the service has acknowledged the published record (I4). */
  public void awaitCommitted(RecordMetadata metadata) {
    var partition = new TopicPartition(metadata.topic(), metadata.partition());

    await()
        .atMost(TIMEOUT)
        .pollInterval(POLL_INTERVAL)
        .until(() -> committedOffset(partition) > metadata.offset());
  }

  @Override
  public void close() {
    producer.close();
    admin.close();
  }

  private RecordMetadata send(ProducerRecord<String, String> record) {
    awaitListenerAssignment();

    try {
      return producer.send(record).get();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while publishing", exception);
    } catch (ExecutionException exception) {
      throw new IllegalStateException("Could not publish to " + record.topic(), exception);
    }
  }

  // The consumer group takes a few seconds to join; scenarios publish once it owns every partition.
  private void awaitListenerAssignment() {
    if (listenerAssigned) {
      return;
    }

    listenerRegistry
        .getListenerContainers()
        .forEach(container -> ContainerTestUtils.waitForAssignment(container, partitions));
    listenerAssigned = true;
  }

  private long committedOffset(TopicPartition partition) throws Exception {
    var offsets = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

    return Optional.ofNullable(offsets.get(partition)).map(OffsetAndMetadata::offset).orElse(-1L);
  }
}
