package co.cask.wrangler.service.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Class description here.
 */
public final class KafkaProperties {
  private final boolean isZookeeperConnection;
  private final String connection;
  private final String topic;
  private final Properties props;

  public KafkaProperties(Map<String, Object> properties) {
    if(properties == null || properties.size() == 0) {
      throw new IllegalArgumentException("Kafka properties are not defined. Check connection setting.");
    }
    isZookeeperConnection = properties.containsKey("zookeeper_quorum") ? true : false;
    if (isZookeeperConnection) {
      connection = (String) properties.get("zookeeper_quorum");
    } else {
      if(properties.containsKey("brokers")) {
        connection = (String) properties.get("brokers");
      } else {
        connection = null;
      }
    }
    if(properties.containsKey("topic")) {
      topic = (String) properties.get("topic");
    } else {
      topic = null;
    }
    props = new Properties();
    if (!isZookeeperConnection) {
      props.setProperty("bootstrap.servers", connection);
      props.setProperty("group.id", UUID.randomUUID().toString());
      props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }
  }

  public boolean isZookeeper() {
    return isZookeeperConnection;
  }

  public String getConnection() {
    return connection;
  }

  public String topic() {
    return topic;
  }

  public Properties get() {
    return props;
  }
}
