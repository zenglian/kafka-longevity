import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

@Log4j2
public class Producer extends Worker {
  Producer(TopicPartition partition) {
    this.partition = partition;
    this.setName("producer-" + partition);
  }

  private Properties getProducerConfig() throws IOException {
    Properties props = new Properties();
    props.load(Producer.class.getResourceAsStream("producer.properties"));
    props.put("transactional.id", "producer-" + this.partition);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Main.config.brokers);
    return props;
  }

  boolean pause = false;

  @Override
  public void loop() {
    Random random = new Random(System.currentTimeMillis());
    Config config = Main.config;
    byte[] body = new byte[config.recordSize];
    KafkaProducer<String, byte[]> producer;
    try {
      producer = new KafkaProducer<>(getProducerConfig());
    } catch (IOException e) {
      log.error(e);
      return;
    }
    producer.initTransactions();
    long index = 0;
    long sendTime = 0;
    while (!stop && counter.get() < config.records / config.partitions) {
      if (pause) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        continue;
      }
      long startTime = System.currentTimeMillis();
      try {
        producer.beginTransaction();
        for (int i = 0; i < config.speed / config.partitions; i++) {
          index++;
          random.nextBytes(body);
          ProducerRecord<String, byte[]> record =
              new ProducerRecord<>(partition.topic(), partition.partition(), "" + index, body);
          producer.send(record);
        }
        producer.commitTransaction();
        counter.addAndGet(config.speed / config.partitions);
        //keys.append(sb);
      } catch (Exception ex) {
        log.error("produce error", ex);
        try {
          producer.abortTransaction();
        } catch (ProducerFencedException ex2) {
          log.error("failed to abort", ex2);
          producer.initTransactions();
        }
      }

      long lapSendTime = System.currentTimeMillis() - startTime;
      sendTime += lapSendTime;
      if (lapSendTime < 1000) {
        try {
          Thread.sleep(1000 - lapSendTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    producer.close();
    if (sendTime > 0)
      log.info("Total send time={},  real speed={}.", sendTime, counter.get() * 1000 / sendTime);
  }
}
