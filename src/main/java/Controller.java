import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Controller extends Thread {
  private static Logger log = LogManager.getFormatterLogger(Controller.class.getName());
  private ArrayList<Consumer> consumers = new ArrayList<>();
  private ArrayList<Producer> producers = new ArrayList<>();
  private boolean stop = false;
  private long startTime = System.currentTimeMillis();
  private long lag = 0;

  void startUp() throws ExecutionException, InterruptedException, IOException {
    setName("Controller");

    Config config = Main.config;
    config.partitions = Consumer.listOffsets(config.topic);
    Consumer.purgeTopic(config.topic);
    for (int i = 0; i < config.partitions; i++) {
      producers.add(new Producer(new TopicPartition(config.topic, i)));
    }
    for (int i = 0; i < config.partitions; i++) {
      consumers.add(new Consumer(new TopicPartition(config.topic, i)));
    }
    producers.forEach(Worker::start);
    consumers.forEach(Worker::start);
  }

  @Override
  public void run() {
    long lastSent = 0;
    long lastReceived = 0;
    long startTime = System.currentTimeMillis();
    long lapStartTime = startTime;
    while (!stop) {
      try {
        Thread.sleep(1000 * Main.config.logInterval);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      long sent = producers.stream().map(Worker::getCount).reduce(Long::sum).orElse(0L);
      long received = consumers.stream().map(Worker::getCount).reduce(Long::sum).orElse(0L);
      long totalSpeed = received * 1000 / (System.currentTimeMillis() - startTime);
      long lapSpeed = (sent - lastSent) * 1000 / (System.currentTimeMillis() - lapStartTime);
      lag = sent - received;
      if (lastSent == sent && lastReceived == received) {
        boolean pause = !KafkaUtil.pingTopic();
        producers.forEach(p -> p.pause = pause);
      } else {
        log.info("produced=%,d, consumed=%,d, lag=%,d, rate=%,d/s, rate.all=%,d/s.",
            sent, received, lag, lapSpeed, totalSpeed);
      }
      lastSent = sent;
      lastReceived = received;
      lapStartTime = System.currentTimeMillis();
    }
  }

  //shutdown all threads gracefully
  void shutdown() {
    log.info("Shutting down...");
    try {
      producers.forEach(Worker::setStop);
      for (Worker producer : producers) {
        producer.join();
      }
      Thread.sleep(30000);
      consumers.forEach(Worker::setStop);
      for (Worker consumer : consumers) {
        consumer.join();
      }
      stop = true;
      this.join();
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    }
    long endTime = System.currentTimeMillis();
    log.info(String.format("Duration=%s.",
        Duration.ofMillis(endTime - startTime).toString().substring(2).toLowerCase()));
    log.info("Test %s", lag == 0 ? "SUCCEEDED!" : "FAILED! (lag = " + lag + ")");
  }

  private void saveKeys() throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < producers.size(); i++) {
      String psb = producers.get(i).keys.toString();
      String csb = consumers.get(i).keys.toString();
      if (psb.equals(csb)) continue;
      int j = 0;
      while (j < psb.length() && j < csb.length() && psb.charAt(j) == csb.charAt(j)) {
        j++;
      }
      if (j < csb.length()) {
        sb.append("w[").append(i).append("] ").append(psb.substring(j)).append('\n');
        sb.append("r[").append(i).append("] ").append(csb.substring(j)).append("\n\n");
      }
    }
    Files.write(Paths.get("logs/keys"), sb.toString().getBytes());
  }
}
