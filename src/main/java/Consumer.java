import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

@Log4j2
public class Consumer extends Worker {
  private KafkaConsumer<String, byte[]> consumer;
  private static Map<Integer, Long> startOffsets = new HashMap<>();
  private static Map<Integer, Long> endOffsets = new HashMap<>();

  Consumer(TopicPartition partition) throws IOException {
    setName("consumer-" + partition);
    this.partition = partition;
    consumer = new KafkaConsumer<>(getConsumerConfig());
    List<TopicPartition> partitions = Collections.singletonList(this.partition);
    consumer.assign(partitions);
    consumer.seekToBeginning(partitions);
  }

  private static Properties getConsumerConfig() throws IOException {
    Properties props = new Properties();
    props.load(Consumer.class.getResourceAsStream("consumer.properties"));
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Main.config.brokers);
    return props;
  }

  static int listOffsets(String topicName) throws IOException {
    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(getConsumerConfig());
    List<PartitionInfo> partitionInfos = consumer.listTopics().get(topicName);
    List<TopicPartition> partitions =
        partitionInfos.stream().map(x -> new TopicPartition(x.topic(), x.partition())).collect(Collectors.toList());
    consumer.beginningOffsets(partitions).forEach((k, v) -> startOffsets.put(k.partition(), v));
    consumer.endOffsets(partitions).forEach((k, v) -> endOffsets.put(k.partition(), v));
    consumer.close();
    return partitionInfos.size();
  }

  static void purgeTopic(String topicName) throws ExecutionException, InterruptedException {
    log.warn("Delete all records from topic {}.", topicName);
    Map<TopicPartition, RecordsToDelete> deleteMap = new HashMap<>();

    endOffsets.forEach((k, v) -> {
      TopicPartition tp = new TopicPartition(topicName, k);
      deleteMap.put(tp, RecordsToDelete.beforeOffset(v));
    });

    AdminClient client = KafkaAdminClient.create(KafkaUtil.getAdminConfig());
    DeleteRecordsResult result = client.deleteRecords(deleteMap);
    Map<TopicPartition, KafkaFuture<DeletedRecords>> lowWatermarks = result.lowWatermarks();
    for (Map.Entry<TopicPartition, KafkaFuture<DeletedRecords>> entry : lowWatermarks.entrySet()) {
      TopicPartition k = entry.getKey();
      KafkaFuture<DeletedRecords> v = entry.getValue();
      log.info("Offset [{}]: {}", k, v.get().lowWatermark());
    }
    client.close();
  }

  @Override
  public void loop() {
    while (!stop && counter.get() < Main.config.records) {
      List<ConsumerRecord<String, byte[]>> records =
          consumer.poll(Duration.ofSeconds(5)).records(partition);
      if (records.isEmpty()) {
        continue;
      }
      //records.forEach(r -> keys.append(r.key()).append(','));
      counter.addAndGet(records.size());
      try {
        consumer.commitSync(Duration.ofSeconds(60));
      } catch (Exception e) {
        log.error(e);
      }
    }
    consumer.close();
  }
}
