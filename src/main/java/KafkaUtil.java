import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

@Log4j2
class KafkaUtil {
  static Properties getAdminConfig() {
    Properties pros = new Properties();
    pros.put("bootstrap.servers", Main.config.brokers);
    return pros;
  }

  static boolean pingTopic() {
    log.info("Ping {}...", Main.config.brokers);
    try (AdminClient client = KafkaAdminClient.create(getAdminConfig())) {
      ListTopicsResult topics = client.listTopics();
      Set<String> names = topics.names().get();
      return names.contains(Main.config.topic);
    } catch (Exception e) {
      log.error("ping", e);
      return false;
    }
  }
}
