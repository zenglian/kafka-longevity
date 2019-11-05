import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;

@Log4j2
class Config {
  @JsonFormat(locale = "US")
  long records = 10000;
  int recordSize = 100;
  @JsonIgnore
  int partitions = 1;
  String brokers = "kafka-headless:9092";
  String topic = "test1";
  int speed = 100;
  int logInterval = 10;

  static Config load() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(JsonAutoDetect.Visibility.NON_PRIVATE));
    Config config = mapper.readValue(new File("conf.json"), Config.class);
    log.info("config = " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config));
    if (config.records <= 0)
      config.records = Long.MAX_VALUE;
    return config;
  }
}
