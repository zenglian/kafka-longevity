import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class Main {
  static Config config;

  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    String build = new BufferedReader(new InputStreamReader(Main.class.getResourceAsStream("version"))).readLine();
    log.info("Build date: {}", build);

    config = Config.load();
    Controller controller = new Controller();
    controller.startUp();
    controller.start();
    Runtime.getRuntime().addShutdownHook(new Thread(controller::shutdown));
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> log.error(t.getName() + " terminated.", e));
  }
}