package cart.counter;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** V9's placeholder: replaced by the counter in T029. */
final class Placeholder {
  private Placeholder() {}

  static Object touch() {
    return new Object[] {StreamExecutionEnvironment.class, KafkaSource.class};
  }
}
