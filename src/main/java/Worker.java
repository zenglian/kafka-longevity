import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.TopicPartition;

@Log4j2
public abstract class Worker extends Thread {
  AtomicLong counter = new AtomicLong(0);
  TopicPartition partition;
  boolean stop = false;
  StringBuilder keys = new StringBuilder();

  @Override
  public void run() {
    log.info("Started.");
    loop();
    log.info("Stopped.");
  }

  abstract protected void loop();

  void setStop(){
    this.stop = true;
  }

  long getCount(){
    return counter.get();
  }
}